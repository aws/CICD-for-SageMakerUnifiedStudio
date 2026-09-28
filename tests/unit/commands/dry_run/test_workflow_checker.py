"""Unit tests for WorkflowChecker (Phase 10 — Workflow Validation).

Tests cover:
- No workflow bootstrap actions → OK skip
- No workflow YAML files found in bundle → OK
- Valid workflow YAML with required DAG keys (dag_id, tasks)
- Invalid YAML syntax → ERROR
- Missing required DAG keys → ERROR
- Non-dict top-level YAML → ERROR
- Environment variable references: resolved → no WARNING
- Environment variable references: unresolved → WARNING per var
- Workflow file referenced by bootstrap action parameter
- Workflow file not found → ERROR
- Multiple workflow files in bundle
- Finding metadata (resource, service, details)
"""

from __future__ import annotations

import os
import zipfile
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set
from unittest.mock import patch

import pytest

from smus_cicd.commands.dry_run.checkers.workflow_checker import WorkflowChecker
from smus_cicd.commands.dry_run.models import DryRunContext, Severity

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass
class _FakeAction:
    type: str
    parameters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class _FakeBootstrap:
    actions: List[_FakeAction] = field(default_factory=list)


@dataclass
class _FakeTarget:
    bootstrap: Optional[_FakeBootstrap] = None
    environment_variables: Optional[Dict[str, str]] = None


def _make_context(
    target: Optional[_FakeTarget] = None,
    bundle_path: Optional[str] = None,
    bundle_files: Optional[Set[str]] = None,
) -> DryRunContext:
    return DryRunContext(
        manifest_file="manifest.yaml",
        target_config=target,
        bundle_path=bundle_path,
        bundle_files=bundle_files or set(),
    )


def _make_bundle_zip(tmp_path, files: Dict[str, str]) -> str:
    """Create a ZIP bundle with the given filename→content mapping."""
    zip_path = str(tmp_path / "bundle.zip")
    with zipfile.ZipFile(zip_path, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return zip_path


_VALID_WORKFLOW_YAML = """\
my_dag:
  dag_id: my_dag
  tasks:
    task1:
      operator: BashOperator
      bash_command: echo hello
"""

_MISSING_TASKS_YAML = """\
my_dag:
  dag_id: my_dag
  schedule: daily
"""

_MISSING_DAG_ID_YAML = """\
my_dag:
  tasks:
    task1:
      operator: BashOperator
"""

_INVALID_YAML = """\
my_dag:
  dag_id: my_dag
  tasks:
    - [invalid
"""

_NON_DICT_YAML = "just a string"

_YAML_WITH_ENV_VARS = """\
my_dag:
  dag_id: my_dag
  tasks:
    task1:
      operator: BashOperator
      bash_command: echo ${MY_VAR} $OTHER_VAR
"""


@pytest.fixture
def checker() -> WorkflowChecker:
    return WorkflowChecker()


# ---------------------------------------------------------------------------
# No workflow actions configured
# ---------------------------------------------------------------------------


class TestNoWorkflowActions:
    """When no workflow bootstrap actions exist, return OK skip."""

    def test_no_target_config(self, checker):
        ctx = _make_context(target=None)
        findings = checker.check(ctx)

        assert len(findings) == 1
        assert findings[0].severity == Severity.OK
        assert "no workflows configured" in findings[0].message.lower()

    def test_no_bootstrap(self, checker):
        target = _FakeTarget(bootstrap=None)
        ctx = _make_context(target=target)
        findings = checker.check(ctx)

        assert len(findings) == 1
        assert findings[0].severity == Severity.OK

    def test_empty_actions(self, checker):
        target = _FakeTarget(bootstrap=_FakeBootstrap(actions=[]))
        ctx = _make_context(target=target)
        findings = checker.check(ctx)

        assert len(findings) == 1
        assert findings[0].severity == Severity.OK

    def test_non_workflow_actions_only(self, checker):
        target = _FakeTarget(
            bootstrap=_FakeBootstrap(
                actions=[_FakeAction(type="quicksight.refresh_dataset")]
            )
        )
        ctx = _make_context(target=target)
        findings = checker.check(ctx)

        assert len(findings) == 1
        assert findings[0].severity == Severity.OK


# ---------------------------------------------------------------------------
# Valid workflow YAML
# ---------------------------------------------------------------------------


class TestValidWorkflowYaml:
    """Valid workflow YAML with required DAG keys produces OK."""

    def test_valid_workflow_in_bundle(self, checker, tmp_path):
        target = _FakeTarget(
            bootstrap=_FakeBootstrap(actions=[_FakeAction(type="workflow.create")])
        )
        zip_path = _make_bundle_zip(
            tmp_path, {"dags/my_dag.yaml": _VALID_WORKFLOW_YAML}
        )
        ctx = _make_context(target=target, bundle_path=zip_path)
        findings = checker.check(ctx)

        ok_findings = [f for f in findings if f.severity == Severity.OK]
        assert any("required keys" in f.message for f in ok_findings)

        errors = [f for f in findings if f.severity == Severity.ERROR]
        assert len(errors) == 0


# ---------------------------------------------------------------------------
# Invalid YAML syntax
# ---------------------------------------------------------------------------


class TestInvalidYamlSyntax:
    """Invalid YAML produces ERROR with file name and error details."""

    def test_invalid_yaml_produces_error(self, checker, tmp_path):
        target = _FakeTarget(
            bootstrap=_FakeBootstrap(
                actions=[
                    _FakeAction(
                        type="workflow.create",
                        parameters={"workflow_file": "dags/bad.yaml"},
                    )
                ]
            )
        )
        zip_path = _make_bundle_zip(tmp_path, {"dags/bad.yaml": _INVALID_YAML})
        ctx = _make_context(target=target, bundle_path=zip_path)
        findings = checker.check(ctx)

        errors = [f for f in findings if f.severity == Severity.ERROR]
        assert len(errors) >= 1
        assert any(
            "invalid YAML" in e.message.lower() or "invalid yaml" in e.message.lower()
            for e in errors
        )
        assert any("bad.yaml" in e.message for e in errors)


# ---------------------------------------------------------------------------
# Missing required DAG keys
# ---------------------------------------------------------------------------


class TestMissingDagKeys:
    """Workflow YAML missing dag_id or tasks produces ERROR."""

    def test_missing_tasks_key(self, checker, tmp_path):
        target = _FakeTarget(
            bootstrap=_FakeBootstrap(
                actions=[
                    _FakeAction(
                        type="workflow.create",
                        parameters={"workflow_file": "dags/no_tasks.yaml"},
                    )
                ]
            )
        )
        zip_path = _make_bundle_zip(
            tmp_path, {"dags/no_tasks.yaml": _MISSING_TASKS_YAML}
        )
        ctx = _make_context(target=target, bundle_path=zip_path)
        findings = checker.check(ctx)

        errors = [f for f in findings if f.severity == Severity.ERROR]
        assert len(errors) >= 1
        assert any("missing required" in e.message.lower() for e in errors)

    def test_missing_dag_id_key(self, checker, tmp_path):
        target = _FakeTarget(
            bootstrap=_FakeBootstrap(
                actions=[
                    _FakeAction(
                        type="workflow.create",
                        parameters={"workflow_file": "dags/no_dagid.yaml"},
                    )
                ]
            )
        )
        zip_path = _make_bundle_zip(
            tmp_path, {"dags/no_dagid.yaml": _MISSING_DAG_ID_YAML}
        )
        ctx = _make_context(target=target, bundle_path=zip_path)
        findings = checker.check(ctx)

        errors = [f for f in findings if f.severity == Severity.ERROR]
        assert len(errors) >= 1
        assert any("missing required" in e.message.lower() for e in errors)


# ---------------------------------------------------------------------------
# Non-dict top-level YAML
# ---------------------------------------------------------------------------


class TestNonDictYaml:
    """Workflow YAML that is not a mapping at top level produces ERROR."""

    def test_non_dict_yaml(self, checker, tmp_path):
        target = _FakeTarget(
            bootstrap=_FakeBootstrap(
                actions=[
                    _FakeAction(
                        type="workflow.create",
                        parameters={"workflow_file": "dags/str.yaml"},
                    )
                ]
            )
        )
        zip_path = _make_bundle_zip(tmp_path, {"dags/str.yaml": _NON_DICT_YAML})
        ctx = _make_context(target=target, bundle_path=zip_path)
        findings = checker.check(ctx)

        errors = [f for f in findings if f.severity == Severity.ERROR]
        assert len(errors) >= 1
        assert any(
            "mapping" in e.message.lower() or "missing required" in e.message.lower()
            for e in errors
        )


# ---------------------------------------------------------------------------
# Environment variable references
# ---------------------------------------------------------------------------


class TestEnvVarReferences:
    """Env var references are checked against target_config.environment_variables."""

    def test_resolved_vars_no_warning(self, checker, tmp_path):
        target = _FakeTarget(
            bootstrap=_FakeBootstrap(actions=[_FakeAction(type="workflow.create")]),
            environment_variables={"MY_VAR": "val1", "OTHER_VAR": "val2"},
        )
        zip_path = _make_bundle_zip(tmp_path, {"dags/dag.yaml": _YAML_WITH_ENV_VARS})
        ctx = _make_context(target=target, bundle_path=zip_path)

        with patch.dict(os.environ, {}, clear=False):
            findings = checker.check(ctx)

        warnings = [f for f in findings if f.severity == Severity.WARNING]
        assert len(warnings) == 0

    def test_unresolved_vars_produce_warnings(self, checker, tmp_path):
        target = _FakeTarget(
            bootstrap=_FakeBootstrap(actions=[_FakeAction(type="workflow.create")]),
            environment_variables={},
        )
        zip_path = _make_bundle_zip(tmp_path, {"dags/dag.yaml": _YAML_WITH_ENV_VARS})
        ctx = _make_context(target=target, bundle_path=zip_path)

        # Ensure the vars are not in os.environ either
        env_copy = {
            k: v for k, v in os.environ.items() if k not in ("MY_VAR", "OTHER_VAR")
        }
        with patch.dict(os.environ, env_copy, clear=True):
            findings = checker.check(ctx)

        warnings = [f for f in findings if f.severity == Severity.WARNING]
        var_names = {f.details["variable"] for f in warnings if f.details}
        assert "MY_VAR" in var_names
        assert "OTHER_VAR" in var_names

    def test_partially_resolved_vars(self, checker, tmp_path):
        target = _FakeTarget(
            bootstrap=_FakeBootstrap(actions=[_FakeAction(type="workflow.create")]),
            environment_variables={"MY_VAR": "val1"},
        )
        zip_path = _make_bundle_zip(tmp_path, {"dags/dag.yaml": _YAML_WITH_ENV_VARS})
        ctx = _make_context(target=target, bundle_path=zip_path)

        env_copy = {k: v for k, v in os.environ.items() if k != "OTHER_VAR"}
        with patch.dict(os.environ, env_copy, clear=True):
            findings = checker.check(ctx)

        warnings = [f for f in findings if f.severity == Severity.WARNING]
        var_names = {f.details["variable"] for f in warnings if f.details}
        assert "OTHER_VAR" in var_names
        assert "MY_VAR" not in var_names

    def test_env_var_resolved_via_os_environ(self, checker, tmp_path):
        target = _FakeTarget(
            bootstrap=_FakeBootstrap(actions=[_FakeAction(type="workflow.create")]),
            environment_variables={},
        )
        zip_path = _make_bundle_zip(tmp_path, {"dags/dag.yaml": _YAML_WITH_ENV_VARS})
        ctx = _make_context(target=target, bundle_path=zip_path)

        with patch.dict(os.environ, {"MY_VAR": "x", "OTHER_VAR": "y"}, clear=False):
            findings = checker.check(ctx)

        warnings = [f for f in findings if f.severity == Severity.WARNING]
        # Both vars resolved via os.environ
        env_var_warnings = [
            w
            for w in warnings
            if w.details and w.details.get("variable") in ("MY_VAR", "OTHER_VAR")
        ]
        assert len(env_var_warnings) == 0


# ---------------------------------------------------------------------------
# Workflow file referenced by bootstrap action parameter
# ---------------------------------------------------------------------------


class TestBootstrapActionWorkflowFile:
    """Bootstrap action parameters can reference specific workflow files."""

    def test_workflow_file_from_action_params(self, checker, tmp_path):
        target = _FakeTarget(
            bootstrap=_FakeBootstrap(
                actions=[
                    _FakeAction(
                        type="workflow.create",
                        parameters={"workflow_file": "dags/my_dag.yaml"},
                    )
                ]
            )
        )
        zip_path = _make_bundle_zip(
            tmp_path, {"dags/my_dag.yaml": _VALID_WORKFLOW_YAML}
        )
        ctx = _make_context(target=target, bundle_path=zip_path)
        findings = checker.check(ctx)

        errors = [f for f in findings if f.severity == Severity.ERROR]
        assert len(errors) == 0

    def test_workflow_file_not_found(self, checker, tmp_path):
        target = _FakeTarget(
            bootstrap=_FakeBootstrap(
                actions=[
                    _FakeAction(
                        type="workflow.create",
                        parameters={"workflow_file": "dags/missing.yaml"},
                    )
                ]
            )
        )
        zip_path = _make_bundle_zip(tmp_path, {})
        ctx = _make_context(target=target, bundle_path=zip_path)
        findings = checker.check(ctx)

        errors = [f for f in findings if f.severity == Severity.ERROR]
        assert len(errors) >= 1
        assert any("not found" in e.message.lower() for e in errors)

    def test_workflow_file_from_local_filesystem(self, checker, tmp_path):
        target = _FakeTarget(
            bootstrap=_FakeBootstrap(
                actions=[
                    _FakeAction(
                        type="workflow.create",
                        parameters={"workflow_file": str(tmp_path / "local_dag.yaml")},
                    )
                ]
            )
        )
        # Write the file to the local filesystem
        local_file = tmp_path / "local_dag.yaml"
        local_file.write_text(_VALID_WORKFLOW_YAML)

        ctx = _make_context(target=target)
        findings = checker.check(ctx)

        ok_findings = [f for f in findings if f.severity == Severity.OK]
        assert any("required keys" in f.message for f in ok_findings)

        errors = [f for f in findings if f.severity == Severity.ERROR]
        assert len(errors) == 0


# ---------------------------------------------------------------------------
# Multiple workflow files
# ---------------------------------------------------------------------------


class TestMultipleWorkflowFiles:
    """Multiple workflow files in the bundle are all validated."""

    def test_multiple_valid_workflows(self, checker, tmp_path):
        dag1 = """\
dag_one:
  dag_id: dag_one
  tasks:
    t1:
      operator: BashOperator
"""
        dag2 = """\
dag_two:
  dag_id: dag_two
  tasks:
    t2:
      operator: PythonOperator
"""
        target = _FakeTarget(
            bootstrap=_FakeBootstrap(actions=[_FakeAction(type="workflow.create")])
        )
        zip_path = _make_bundle_zip(
            tmp_path,
            {"dags/dag1.yaml": dag1, "dags/dag2.yaml": dag2},
        )
        ctx = _make_context(target=target, bundle_path=zip_path)
        findings = checker.check(ctx)

        ok_findings = [
            f
            for f in findings
            if f.severity == Severity.OK and "required keys" in f.message
        ]
        assert len(ok_findings) == 2

    def test_mix_valid_and_invalid(self, checker, tmp_path):
        target = _FakeTarget(
            bootstrap=_FakeBootstrap(actions=[_FakeAction(type="workflow.create")])
        )
        zip_path = _make_bundle_zip(
            tmp_path,
            {
                "dags/good.yaml": _VALID_WORKFLOW_YAML,
                "dags/bad.yaml": _MISSING_TASKS_YAML,
            },
        )
        ctx = _make_context(target=target, bundle_path=zip_path)

        # bad.yaml won't be auto-detected as a workflow (no dag_id+tasks),
        # but if referenced explicitly it would error. The auto-detection
        # only picks up files that look like workflows.
        findings = checker.check(ctx)

        ok_findings = [
            f
            for f in findings
            if f.severity == Severity.OK and "required keys" in f.message
        ]
        # Only the valid one is auto-detected
        assert len(ok_findings) >= 1


# ---------------------------------------------------------------------------
# No workflow files found in bundle
# ---------------------------------------------------------------------------


class TestNoWorkflowFilesInBundle:
    """When workflow actions exist but no YAML files found, produce OK."""

    def test_empty_bundle(self, checker, tmp_path):
        target = _FakeTarget(
            bootstrap=_FakeBootstrap(actions=[_FakeAction(type="workflow.create")])
        )
        zip_path = _make_bundle_zip(tmp_path, {"readme.txt": "hello"})
        ctx = _make_context(target=target, bundle_path=zip_path)
        findings = checker.check(ctx)

        ok_findings = [f for f in findings if f.severity == Severity.OK]
        assert any("no workflow" in f.message.lower() for f in ok_findings)


# ---------------------------------------------------------------------------
# Finding metadata
# ---------------------------------------------------------------------------


class TestFindingMetadata:
    """Findings carry appropriate service, resource, and details."""

    def test_skip_finding_has_service(self, checker):
        ctx = _make_context(target=None)
        findings = checker.check(ctx)
        assert findings[0].service == "airflow"

    def test_error_finding_has_resource(self, checker, tmp_path):
        target = _FakeTarget(
            bootstrap=_FakeBootstrap(
                actions=[
                    _FakeAction(
                        type="workflow.create",
                        parameters={"workflow_file": "dags/bad.yaml"},
                    )
                ]
            )
        )
        zip_path = _make_bundle_zip(tmp_path, {"dags/bad.yaml": _INVALID_YAML})
        ctx = _make_context(target=target, bundle_path=zip_path)
        findings = checker.check(ctx)

        errors = [f for f in findings if f.severity == Severity.ERROR]
        assert any(e.resource is not None for e in errors)

    def test_error_finding_has_details(self, checker, tmp_path):
        target = _FakeTarget(
            bootstrap=_FakeBootstrap(
                actions=[
                    _FakeAction(
                        type="workflow.create",
                        parameters={"workflow_file": "dags/bad.yaml"},
                    )
                ]
            )
        )
        zip_path = _make_bundle_zip(tmp_path, {"dags/bad.yaml": _INVALID_YAML})
        ctx = _make_context(target=target, bundle_path=zip_path)
        findings = checker.check(ctx)

        errors = [f for f in findings if f.severity == Severity.ERROR]
        yaml_errors = [e for e in errors if e.details and "error" in e.details]
        assert len(yaml_errors) >= 1

    def test_warning_finding_has_variable_detail(self, checker, tmp_path):
        target = _FakeTarget(
            bootstrap=_FakeBootstrap(actions=[_FakeAction(type="workflow.create")]),
            environment_variables={},
        )
        yaml_content = """\
my_dag:
  dag_id: my_dag
  tasks:
    t1:
      bash_command: echo ${SOME_VAR}
"""
        zip_path = _make_bundle_zip(tmp_path, {"dags/dag.yaml": yaml_content})
        ctx = _make_context(target=target, bundle_path=zip_path)

        env_copy = {k: v for k, v in os.environ.items() if k != "SOME_VAR"}
        with patch.dict(os.environ, env_copy, clear=True):
            findings = checker.check(ctx)

        warnings = [f for f in findings if f.severity == Severity.WARNING]
        assert any(
            w.details and w.details.get("variable") == "SOME_VAR" for w in warnings
        )


# ---------------------------------------------------------------------------
# Operator code (PythonOperator/BashOperator `code` field) validation
# ---------------------------------------------------------------------------


@dataclass
class _FakeContent:
    workflows: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class _FakeManifest:
    content: _FakeContent = field(default_factory=_FakeContent)


def _make_code_context(workflows, bundle_files):
    """Context with a workflow.create action, a manifest, and a bundle listing."""
    target = _FakeTarget(
        bootstrap=_FakeBootstrap(actions=[_FakeAction(type="workflow.create")]),
        environment_variables={},
    )
    ctx = _make_context(target=target, bundle_files=set(bundle_files))
    ctx.manifest = _FakeManifest(content=_FakeContent(workflows=workflows))
    return ctx


def test_operator_code_present_in_bundle_is_ok():
    ctx = _make_code_context(
        [
            {"workflowName": "py", "code": "code/python/greeting.py"},
            {"workflowName": "sh", "code": "code/bash/greet.sh"},
        ],
        [
            "python-bash-operators/bundle/greeting.py",
            "python-bash-operators/bundle/greet.sh",
        ],
    )
    findings = WorkflowChecker().check(ctx)
    code_findings = [f for f in findings if "operator code" in f.message]
    assert len(code_findings) == 2
    assert all(f.severity == Severity.OK for f in code_findings)


def test_operator_code_missing_from_bundle_is_error():
    ctx = _make_code_context(
        [{"workflowName": "py", "code": "code/python/greeting.py"}],
        ["python-bash-operators/bundle/workflows/py.yaml"],
    )
    findings = WorkflowChecker().check(ctx)
    errors = [f for f in findings if f.severity == Severity.ERROR]
    assert any("not found in bundle" in f.message for f in errors)


def test_operator_code_bad_extension_is_error():
    ctx = _make_code_context(
        [{"workflowName": "py", "code": "code/python/module.txt"}],
        ["python-bash-operators/bundle/module.txt"],
    )
    findings = WorkflowChecker().check(ctx)
    errors = [f for f in findings if f.severity == Severity.ERROR]
    assert any("must be a single .py/.sh file" in f.message for f in errors)


def test_operator_code_directory_ref_is_error():
    """A `code` naming a directory (no extension) is a hard error - no auto-zip."""
    ctx = _make_code_context(
        [{"workflowName": "py", "code": "code/mypkg"}],
        [
            "python-bash-operators/bundle/mypkg/mod.py",
            "python-bash-operators/bundle/mypkg/helper.py",
        ],
    )
    findings = WorkflowChecker().check(ctx)
    errors = [f for f in findings if f.severity == Severity.ERROR]
    assert any("pre-built .zip" in f.message for f in errors)


def test_workflow_without_code_field_produces_no_code_findings():
    ctx = _make_code_context(
        [{"workflowName": "py"}],
        ["python-bash-operators/bundle/workflows/py.yaml"],
    )
    findings = WorkflowChecker().check(ctx)
    assert not [f for f in findings if "operator code" in f.message]
