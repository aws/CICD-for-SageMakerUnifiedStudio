"""Workflow validation checker (Phase 10).

Validates workflow YAML files from the bundle or local filesystem:
- Checks for valid YAML syntax
- Verifies required top-level Airflow DAG keys (``dag_id``, ``tasks``)
- Verifies environment variable references (``${VAR}`` / ``$VAR``) against
  ``target_config.environment_variables``

Workflow files are identified by:
1. Files in the bundle matching ``.yaml`` / ``.yml`` extensions that parse as
   Airflow DAG definitions (top-level key containing ``dag_id`` and ``tasks``).
2. Bootstrap actions of type ``workflow.create`` whose parameters include a
   ``workflow_file`` key pointing to a specific file.

If no workflow bootstrap actions are configured, returns OK with
"no workflows configured".

Requirements: 9.1, 9.2, 9.3, 9.4
"""

from __future__ import annotations

import logging
import os
import re
import zipfile
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

from smus_cicd.commands.dry_run.models import DryRunContext, Finding, Severity
from smus_cicd.helpers.operator_code import code_ref_error, is_code_artifact

logger = logging.getLogger(__name__)

# Required keys inside the top-level DAG value dict
_REQUIRED_DAG_KEYS = {"dag_id", "tasks"}


class WorkflowChecker:
    """Validates workflow YAML files for syntax, structure, and env var references."""

    def check(self, context: DryRunContext) -> List[Finding]:
        findings: List[Finding] = []

        # Determine if workflows are configured
        if not self._has_workflow_actions(context):
            findings.append(
                Finding(
                    severity=Severity.OK,
                    message="No workflows configured; skipping.",
                    service="airflow",
                )
            )
            return findings

        # Collect workflow file contents: list of (filename, raw_content)
        workflow_files = self._collect_workflow_files(context, findings)

        for filename, content in workflow_files:
            self._validate_workflow_file(filename, content, context, findings)

        # Validate operator code packages declared on workflow entries. This is
        # independent of the DAG YAML checks above: a workflow using
        # PythonOperator/BashOperator points at a .py/.sh/.zip via its `code`
        # field, which must be present in the bundle and have a supported
        # extension for deploy to upload it and pass the MWAA Serverless Code
        # parameter.
        self._validate_operator_code(context, findings)

        return findings

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _has_workflow_actions(context: DryRunContext) -> bool:
        """Return True if any bootstrap action type starts with 'workflow.'."""
        target = context.target_config
        if not target:
            return False

        bootstrap = getattr(target, "bootstrap", None)
        if not bootstrap:
            return False

        actions = getattr(bootstrap, "actions", None) or []
        return any(
            getattr(action, "type", "").startswith("workflow.") for action in actions
        )

    def _collect_workflow_files(
        self,
        context: DryRunContext,
        findings: List[Finding],
    ) -> List[Tuple[str, str]]:
        """Gather workflow file contents from the bundle or local filesystem.

        Returns a list of ``(filename, raw_content)`` tuples.
        """
        workflow_files: List[Tuple[str, str]] = []
        seen: Set[str] = set()

        # 1. Look for YAML files in the bundle under common workflow paths
        if context.bundle_path and os.path.isfile(context.bundle_path):
            try:
                with zipfile.ZipFile(context.bundle_path, "r") as zf:
                    for name in zf.namelist():
                        if name.endswith(("/", "\\")) or name in seen:
                            continue
                        if not (name.endswith(".yaml") or name.endswith(".yml")):
                            continue
                        try:
                            raw = zf.read(name).decode("utf-8")
                            # Quick check: does it look like a workflow YAML?
                            parsed = yaml.safe_load(raw)
                            if self._is_workflow_yaml(parsed):
                                workflow_files.append((name, raw))
                                seen.add(name)
                        except yaml.YAMLError:
                            # We'll still validate it below for the error report
                            workflow_files.append(
                                (name, zf.read(name).decode("utf-8", errors="replace"))
                            )
                            seen.add(name)
                        except Exception:
                            pass
            except zipfile.BadZipFile:
                pass  # BundleChecker already reports this

        # 2. Check bootstrap action parameters for explicit workflow_file refs
        target = context.target_config
        if target:
            bootstrap = getattr(target, "bootstrap", None)
            actions = getattr(bootstrap, "actions", None) or [] if bootstrap else []
            for action in actions:
                if not getattr(action, "type", "").startswith("workflow."):
                    continue
                params = getattr(action, "parameters", {}) or {}
                wf_file = params.get("workflow_file")
                if wf_file and wf_file not in seen:
                    content = self._read_workflow_file(wf_file, context, findings)
                    if content is not None:
                        workflow_files.append((wf_file, content))
                        seen.add(wf_file)

        if not workflow_files:
            findings.append(
                Finding(
                    severity=Severity.OK,
                    message="No workflow YAML files found in bundle.",
                    service="airflow",
                )
            )

        return workflow_files

    def _validate_operator_code(
        self,
        context: DryRunContext,
        findings: List[Finding],
    ) -> None:
        """Validate per-workflow ``code`` files (PythonOperator/BashOperator).

        For each ``content.workflows`` entry that declares ``code``:

        - The value must name a single ``.py``/``.sh``/``.zip`` file (deploy
          uploads it as-is). A directory or any other extension is a hard error
          - multiple modules or dependencies must be pre-packaged into a ``.zip``.
        - When a bundle is enumerated, a matching file (by basename) must be
          present in it.

        Workflows without ``code`` are skipped (they use only AWS service
        operators). When no bundle is enumerated (manifest-only dry run) the
        presence check is skipped and the declaration is reported as OK.
        """
        manifest = getattr(context, "manifest", None)
        content = getattr(manifest, "content", None) if manifest else None
        workflows = getattr(content, "workflows", None) or []

        bundle_files = context.bundle_files or set()

        for workflow in workflows:
            code_path = workflow.get("code") if isinstance(workflow, dict) else None
            workflow_name = (
                workflow.get("workflowName") if isinstance(workflow, dict) else None
            )
            if not code_path:
                continue

            code_basename = os.path.basename(str(code_path).rstrip("/"))

            # Must be a single supported file. A directory (no extension) or an
            # unsupported extension is a hard error - pre-package a .zip instead.
            # Shared rule with the deploy path so the two cannot drift.
            ref_error = code_ref_error(code_path)
            if ref_error:
                findings.append(
                    Finding(
                        severity=Severity.ERROR,
                        message=f"Workflow '{workflow_name}': {ref_error}",
                        resource=code_path,
                        service="airflow",
                    )
                )
                continue

            # Presence check requires an enumerated bundle.
            if not bundle_files:
                findings.append(
                    Finding(
                        severity=Severity.OK,
                        message=(
                            f"Workflow '{workflow_name}': operator code '{code_path}' "
                            f"declared (bundle not available to verify presence)"
                        ),
                        resource=code_path,
                        service="airflow",
                    )
                )
                continue

            matches = [
                f
                for f in bundle_files
                if os.path.basename(f) == code_basename and not is_code_artifact(f)
            ]

            if matches:
                findings.append(
                    Finding(
                        severity=Severity.OK,
                        message=(
                            f"Workflow '{workflow_name}': operator code '{matches[0]}' "
                            f"found in bundle"
                        ),
                        resource=code_path,
                        service="airflow",
                    )
                )
            else:
                findings.append(
                    Finding(
                        severity=Severity.ERROR,
                        message=(
                            f"Workflow '{workflow_name}': operator code '{code_path}' "
                            f"not found in bundle. Include it via content.storage so "
                            f"deploy can upload it as the Code parameter."
                        ),
                        resource=code_path,
                        service="airflow",
                    )
                )

    @staticmethod
    def _read_workflow_file(
        filename: str,
        context: DryRunContext,
        findings: List[Finding],
    ) -> Optional[str]:
        """Try to read a workflow file from the bundle or local filesystem."""
        # Try bundle first
        if context.bundle_path and os.path.isfile(context.bundle_path):
            try:
                with zipfile.ZipFile(context.bundle_path, "r") as zf:
                    if filename in zf.namelist():
                        return zf.read(filename).decode("utf-8")
            except Exception:
                pass

        # Try local filesystem
        if os.path.isfile(filename):
            try:
                with open(filename, "r") as fh:
                    return fh.read()
            except Exception as exc:
                findings.append(
                    Finding(
                        severity=Severity.ERROR,
                        message=(f"Cannot read workflow file '{filename}': {exc}"),
                        resource=filename,
                        service="airflow",
                    )
                )
                return None

        findings.append(
            Finding(
                severity=Severity.ERROR,
                message=f"Workflow file '{filename}' not found in bundle or filesystem",
                resource=filename,
                service="airflow",
            )
        )
        return None

    @staticmethod
    def _is_workflow_yaml(parsed: Any) -> bool:
        """Return True if *parsed* looks like an Airflow DAG YAML."""
        if not isinstance(parsed, dict):
            return False
        for _key, value in parsed.items():
            if isinstance(value, dict) and "dag_id" in value and "tasks" in value:
                return True
        return False

    def _validate_workflow_file(
        self,
        filename: str,
        content: str,
        context: DryRunContext,
        findings: List[Finding],
    ) -> None:
        """Validate a single workflow file for syntax, structure, and env vars."""
        # 1. YAML syntax (Req 9.1)
        try:
            parsed = yaml.safe_load(content)
        except yaml.YAMLError as exc:
            findings.append(
                Finding(
                    severity=Severity.ERROR,
                    message=(
                        f"Workflow file '{filename}' contains invalid YAML: {exc}"
                    ),
                    resource=filename,
                    service="airflow",
                    details={"error": str(exc)},
                )
            )
            return  # Can't check structure if YAML is invalid

        # 2. Required top-level Airflow DAG keys (Req 9.2)
        self._validate_dag_structure(filename, parsed, findings)

        # 3. Environment variable references (Req 9.3)
        self._validate_env_vars(filename, content, context, findings)

    def _validate_dag_structure(
        self,
        filename: str,
        parsed: Any,
        findings: List[Finding],
    ) -> None:
        """Verify the parsed YAML has the expected Airflow DAG structure."""
        if not isinstance(parsed, dict):
            findings.append(
                Finding(
                    severity=Severity.ERROR,
                    message=(
                        f"Workflow file '{filename}' does not contain "
                        f"a YAML mapping at the top level"
                    ),
                    resource=filename,
                    service="airflow",
                )
            )
            return

        # Look for at least one top-level key whose value is a dict with
        # the required DAG keys (dag_id, tasks).
        found_dag = False
        for key, value in parsed.items():
            if isinstance(value, dict):
                missing = _REQUIRED_DAG_KEYS - set(value.keys())
                if not missing:
                    found_dag = True
                    findings.append(
                        Finding(
                            severity=Severity.OK,
                            message=(
                                f"Workflow file '{filename}': "
                                f"DAG '{key}' has required keys"
                            ),
                            resource=filename,
                            service="airflow",
                        )
                    )
                    break

        if not found_dag:
            findings.append(
                Finding(
                    severity=Severity.ERROR,
                    message=(
                        f"Workflow file '{filename}' is missing required "
                        f"Airflow DAG keys: {', '.join(sorted(_REQUIRED_DAG_KEYS))}"
                    ),
                    resource=filename,
                    service="airflow",
                    details={"required_keys": sorted(_REQUIRED_DAG_KEYS)},
                )
            )

    @staticmethod
    def _validate_env_vars(
        filename: str,
        content: str,
        context: DryRunContext,
        findings: List[Finding],
    ) -> None:
        """Check that env var references in the workflow file are resolvable."""
        # Collect all ${VAR_NAME} and $VAR_NAME references
        braced = set(re.findall(r"\$\{([^}]+)\}", content))
        bare = set(re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", content))
        all_refs = braced | bare

        if not all_refs:
            return

        # Build the set of known variables
        env_vars: Dict[str, Any] = {}
        if context.target_config and getattr(
            context.target_config, "environment_variables", None
        ):
            env_vars = context.target_config.environment_variables

        unresolved = sorted(
            name for name in all_refs if name not in env_vars and name not in os.environ
        )

        if unresolved:
            for var_name in unresolved:
                findings.append(
                    Finding(
                        severity=Severity.WARNING,
                        message=(
                            f"Workflow file '{filename}': unresolved "
                            f"environment variable reference: ${var_name}"
                        ),
                        resource=filename,
                        service="airflow",
                        details={"variable": var_name},
                    )
                )
