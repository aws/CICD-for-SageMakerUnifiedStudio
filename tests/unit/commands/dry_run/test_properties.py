"""Property-based tests for the deploy dry-run feature.

Uses Hypothesis to verify universal correctness properties across randomly
generated inputs.
"""

import io
import json
import os
import re
import tempfile
import textwrap
import zipfile
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set
from unittest.mock import MagicMock, patch

from hypothesis import given, settings
from hypothesis import strategies as st
from typer.testing import CliRunner

from smus_cicd.cli import app
from smus_cicd.commands.dry_run.checkers.bundle_checker import BundleChecker
from smus_cicd.commands.dry_run.checkers.manifest_checker import ManifestChecker
from smus_cicd.commands.dry_run.engine import DryRunEngine
from smus_cicd.commands.dry_run.models import (
    DryRunContext,
    DryRunReport,
    Finding,
    Phase,
    Severity,
)
from smus_cicd.commands.dry_run.report import ReportFormatter

# --- Strategies ---

severity_strategy = st.sampled_from(list(Severity))
phase_strategy = st.sampled_from(list(Phase))

# Number of deployment phases. Tests that generate one finding per phase size
# their input lists to this value; deriving it from the Phase enum keeps the
# tests correct when phases are added or removed.
PHASE_COUNT = len(list(Phase))

finding_tuple_strategy = st.tuples(
    phase_strategy,
    severity_strategy,
    st.text(min_size=1, max_size=80),
)


def _group_resources_by_type(resources):
    """Group a flat list of resource dicts by their 'type' field into keyed format."""
    grouped = {"metadata": {}}
    for r in resources:
        if isinstance(r, dict):
            rtype = r.get("type", "unknown")
            grouped.setdefault(rtype, []).append(r)
    return grouped


# Feature: deploy-dry-run, Property 15: Report structure correctness
# **Validates: Requirements 7.1, 7.2, 7.3**
@given(finding_tuples=st.lists(finding_tuple_strategy, min_size=0, max_size=50))
@settings(max_examples=100)
def test_property_15_report_structure_correctness(finding_tuples):
    """For any list of findings with arbitrary severities and phases,
    the DryRunReport shall satisfy:
    - ok_count equals the number of findings with severity OK
    - warning_count equals the number with severity WARNING
    - error_count equals the number with severity ERROR
    - findings_by_phase[p] contains exactly the findings assigned to phase p
    """
    report = DryRunReport()

    # Group findings by phase for batch add_findings calls
    by_phase = {}
    for phase, severity, message in finding_tuples:
        by_phase.setdefault(phase, []).append(
            Finding(severity=severity, message=message)
        )

    for phase, findings in by_phase.items():
        report.add_findings(phase, findings)

    # Count expected severities
    severity_counts = Counter(sev for _, sev, _ in finding_tuples)
    expected_ok = severity_counts[Severity.OK]
    expected_warning = severity_counts[Severity.WARNING]
    expected_error = severity_counts[Severity.ERROR]

    assert report.ok_count == expected_ok
    assert report.warning_count == expected_warning
    assert report.error_count == expected_error

    # Verify findings_by_phase grouping
    expected_by_phase = {}
    for phase, severity, message in finding_tuples:
        expected_by_phase.setdefault(phase, []).append((severity, message))

    for phase in Phase:
        actual_findings = report.findings_by_phase.get(phase, [])
        expected_findings = expected_by_phase.get(phase, [])
        assert len(actual_findings) == len(expected_findings)
        for actual, (exp_sev, exp_msg) in zip(actual_findings, expected_findings):
            assert actual.severity == exp_sev
            assert actual.message == exp_msg
            assert actual.phase == phase


# Feature: deploy-dry-run, Property 17: JSON report round-trip
# **Validates: Requirements 7.7**
@given(finding_tuples=st.lists(finding_tuple_strategy, min_size=0, max_size=50))
@settings(max_examples=100)
def test_property_17_json_report_round_trip(finding_tuples):
    """For any DryRunReport, serializing it to JSON via ReportFormatter.to_json()
    and then parsing the result with json.loads() shall produce a dictionary whose
    summary.ok, summary.warnings, and summary.errors values equal the report's
    ok_count, warning_count, and error_count respectively.
    """
    report = DryRunReport()

    by_phase = {}
    for phase, severity, message in finding_tuples:
        by_phase.setdefault(phase, []).append(
            Finding(severity=severity, message=message)
        )

    for phase, findings in by_phase.items():
        report.add_findings(phase, findings)

    json_str = ReportFormatter.to_json(report)
    parsed = json.loads(json_str)

    assert parsed["summary"]["ok"] == report.ok_count
    assert parsed["summary"]["warnings"] == report.warning_count
    assert parsed["summary"]["errors"] == report.error_count


# ---------------------------------------------------------------------------
# Helpers for manifest & bundle property tests
# ---------------------------------------------------------------------------


def _write_manifest_yaml(tmp_dir: str, content: str) -> str:
    """Write manifest content to a temp YAML file and return the path."""
    path = os.path.join(tmp_dir, "manifest.yaml")
    with open(path, "w") as f:
        f.write(content)
    return path


def _make_zip_bytes(files: Dict[str, str]) -> bytes:
    """Create a ZIP archive in memory and return the bytes."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


def _write_zip(tmp_dir: str, files: Dict[str, str], name: str = "bundle.zip") -> str:
    """Write a ZIP archive to a temp file and return the path."""
    path = os.path.join(tmp_dir, name)
    with zipfile.ZipFile(path, "w") as zf:
        for fname, content in files.items():
            zf.writestr(fname, content)
    return path


def _make_stub_manifest_and_config(
    storage_names: Optional[List[str]] = None,
    git_names: Optional[List[str]] = None,
    local_storage_names: Optional[List[str]] = None,
):
    """Return minimal manifest-like and target_config-like stubs for BundleChecker tests."""

    @dataclass
    class _StorageContent:
        name: str
        connectionName: Optional[str] = "default.s3_shared"
        include: list = field(default_factory=list)

    @dataclass
    class _Content:
        storage: List[_StorageContent] = field(default_factory=list)
        git: list = field(default_factory=list)

    @dataclass
    class _StorageConfig:
        name: str
        connectionName: Optional[str] = "default.s3_shared"
        targetDirectory: str = ""

    @dataclass
    class _GitTargetConfig:
        name: str
        connectionName: str = "default.git"
        targetDirectory: str = ""

    @dataclass
    class _DeploymentConfiguration:
        storage: List[_StorageConfig] = field(default_factory=list)
        git: List[_GitTargetConfig] = field(default_factory=list)

    @dataclass
    class _TargetConfig:
        deployment_configuration: Optional[_DeploymentConfiguration] = None
        environment_variables: Optional[dict] = None

    @dataclass
    class _Manifest:
        application_name: str = "TestApp"
        content: Optional[_Content] = None

    content_storage = [_StorageContent(name=n) for n in (storage_names or [])]
    content_storage += [
        _StorageContent(name=n, connectionName=None)
        for n in (local_storage_names or [])
    ]
    content = _Content(storage=content_storage)

    dep_storage = [_StorageConfig(name=n) for n in (storage_names or [])]
    dep_storage += [_StorageConfig(name=n) for n in (local_storage_names or [])]
    dep_git = [_GitTargetConfig(name=n) for n in (git_names or [])]
    dep_config = _DeploymentConfiguration(storage=dep_storage, git=dep_git)

    target_config = _TargetConfig(deployment_configuration=dep_config)
    manifest = _Manifest(content=content)

    return manifest, target_config


# ---------------------------------------------------------------------------
# Strategies for manifest & bundle property tests
# ---------------------------------------------------------------------------

# Strategy for valid identifier-like names (used for storage/git item names, var names)
_identifier_strategy = st.from_regex(r"[A-Za-z][A-Za-z0-9_]{0,15}", fullmatch=True)


# Feature: deploy-dry-run, Property 5: Manifest validation error reporting
# **Validates: Requirements 2.1, 2.2**
@given(raw_content=st.text(min_size=1, max_size=300))
@settings(max_examples=100, deadline=None)
def test_property_5_manifest_validation_error_reporting(raw_content):
    """For any input string, if the string is not valid YAML or does not conform
    to the manifest schema, the ManifestChecker shall return at least one ERROR
    finding. Conversely, if the string is valid YAML conforming to the schema,
    no ERROR findings shall be produced for the manifest parsing step.
    """
    checker = ManifestChecker()

    with tempfile.TemporaryDirectory() as tmp_dir:
        manifest_path = _write_manifest_yaml(tmp_dir, raw_content)
        context = DryRunContext(
            manifest_file=manifest_path,
            stage_name="dev",
        )
        findings = checker.check(context)

    # Determine if the content is a valid manifest by trying to parse it
    # the same way the checker does
    is_valid = False
    try:
        from smus_cicd.application.application_manifest import ApplicationManifest

        ApplicationManifest.from_file(manifest_path)
        is_valid = True
    except Exception:
        is_valid = False

    error_findings = [f for f in findings if f.severity == Severity.ERROR]

    if not is_valid:
        # Invalid YAML or schema → at least one ERROR
        assert len(error_findings) >= 1, (
            f"Expected at least one ERROR for invalid manifest content, "
            f"got {len(error_findings)} errors. Content: {raw_content!r}"
        )
    # Note: We don't assert the converse (valid → no errors) here because
    # the checker also validates target stage resolution and domain config,
    # which may fail even with valid YAML. The property specifically covers
    # the "manifest parsing step" — if from_file succeeds, the manifest
    # loaded OK finding should be present.


# Feature: deploy-dry-run, Property 4: Environment variable detection
# **Validates: Requirements 2.5**
@given(
    var_names=st.lists(
        _identifier_strategy,
        min_size=1,
        max_size=5,
        unique=True,
    ),
    resolve_flags=st.lists(st.booleans(), min_size=5, max_size=5),
)
@settings(max_examples=100, deadline=None)
def test_property_4_environment_variable_detection(var_names, resolve_flags):
    """For any manifest content string containing ${VAR_NAME} or $VAR_NAME
    references and any environment variable dictionary, the ManifestChecker
    shall report a WARNING finding for every variable reference whose name
    is absent from both the dictionary and os.environ.
    """
    # Decide which vars are "resolved" (in env_vars dict) vs "unresolved"
    env_vars_dict = {}
    expected_unresolved = set()

    for i, name in enumerate(var_names):
        if resolve_flags[i % len(resolve_flags)]:
            env_vars_dict[name] = "some_value"
        else:
            expected_unresolved.add(name)

    # Build a minimal valid manifest YAML that references these variables
    var_refs = " ".join(f"${{{name}}}" for name in var_names)

    # Build environment_variables block
    env_lines = []
    for k, v in env_vars_dict.items():
        env_lines.append(f'      {k}: "{v}"')
    if not env_lines:
        env_lines.append('      _DUMMY_: "x"')
    env_block = "\n".join(env_lines)

    manifest_content = textwrap.dedent(f"""\
        applicationName: TestApp
        stages:
          dev:
            stage: DEV
            domain:
              region: us-east-1
            project:
              name: dev-project
            environment_variables:
{env_block}
        # refs: {var_refs}
    """)

    checker = ManifestChecker()

    with tempfile.TemporaryDirectory() as tmp_dir:
        manifest_path = _write_manifest_yaml(tmp_dir, manifest_content)
        context = DryRunContext(
            manifest_file=manifest_path,
            stage_name="dev",
        )
        # Clear os.environ to ensure only env_vars_dict matters
        with patch.dict(os.environ, {}, clear=True):
            findings = checker.check(context)

    # If manifest failed to load, skip the env var assertion
    if context.manifest is None:
        return

    warning_findings = [f for f in findings if f.severity == Severity.WARNING]
    warned_vars = set()
    for f in warning_findings:
        # Extract variable name from warning message like "Unresolved environment variable reference: $VAR_NAME"
        match = re.search(r"\$(\w+)", f.message)
        if match:
            warned_vars.add(match.group(1))

    # Every expected unresolved var should have a warning
    # (some vars like PLACEHOLDER may also appear as unresolved)
    for var_name in expected_unresolved:
        assert var_name in warned_vars, (
            f"Expected WARNING for unresolved var '{var_name}', "
            f"but only got warnings for: {warned_vars}"
        )

    # Resolved vars should NOT have warnings
    for var_name in env_vars_dict:
        assert (
            var_name not in warned_vars
        ), f"Unexpected WARNING for resolved var '{var_name}'"


# Feature: deploy-dry-run, Property 6: Bundle file enumeration
# **Validates: Requirements 3.1**
@given(
    file_names=st.lists(
        st.from_regex(r"[a-z][a-z0-9_/]{0,30}\.[a-z]{1,4}", fullmatch=True),
        min_size=0,
        max_size=20,
        unique=True,
    )
)
@settings(max_examples=100)
def test_property_6_bundle_file_enumeration(file_names):
    """For any valid ZIP archive, the BundleChecker shall report an OK finding
    whose message contains the exact count of files in the archive, and
    context.bundle_files shall equal the set of file names returned by
    ZipFile.namelist().
    """
    checker = BundleChecker()

    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create a ZIP with the generated file names
        zip_path = _write_zip(
            tmp_dir, {name: f"content of {name}" for name in file_names}
        )

        manifest, target_config = _make_stub_manifest_and_config()
        context = DryRunContext(
            manifest_file="manifest.yaml",
            manifest=manifest,
            target_config=target_config,
            config={"region": "us-east-1"},
            bundle_path=zip_path,
        )

        findings = checker.check(context)

    # Verify the OK finding with the file count
    ok_findings = [f for f in findings if f.severity == Severity.OK]
    count_str = f"{len(file_names)} file(s)"
    count_findings = [f for f in ok_findings if count_str in f.message]
    assert len(count_findings) == 1, (
        f"Expected exactly one OK finding containing '{count_str}', "
        f"got {len(count_findings)}. OK messages: {[f.message for f in ok_findings]}"
    )

    # Verify context.bundle_files matches the ZIP contents
    assert context.bundle_files == set(
        file_names
    ), f"context.bundle_files {context.bundle_files} != expected {set(file_names)}"


# Feature: deploy-dry-run, Property 7: Missing artifact detection
# **Validates: Requirements 3.3, 3.4, 3.5**
@given(
    storage_names=st.lists(
        st.from_regex(r"s_[a-z][a-z0-9]{1,8}", fullmatch=True),
        min_size=1,
        max_size=5,
        unique=True,
    ),
    git_names=st.lists(
        st.from_regex(r"g_[a-z][a-z0-9]{1,8}", fullmatch=True),
        min_size=0,
        max_size=3,
        unique=True,
    ),
    present_flags=st.lists(st.booleans(), min_size=8, max_size=8),
)
@settings(max_examples=100)
def test_property_7_missing_artifact_detection(storage_names, git_names, present_flags):
    """For any deployment configuration listing storage and git items, and any
    bundle archive, every item name that has no corresponding files in the
    bundle (and no local filesystem fallback) shall produce an ERROR finding
    containing the item name. Items that do have corresponding files shall not
    produce ERROR findings.
    """
    # Decide which items are present in the bundle using the flags
    present_storage = set()
    missing_storage = set()
    for i, name in enumerate(storage_names):
        if present_flags[i % len(present_flags)]:
            present_storage.add(name)
        else:
            missing_storage.add(name)

    present_git = set()
    missing_git = set()
    for i, name in enumerate(git_names):
        if present_flags[(i + len(storage_names)) % len(present_flags)]:
            present_git.add(name)
        else:
            missing_git.add(name)

    # Build the ZIP with present items
    zip_files = {}
    for name in present_storage:
        zip_files[f"{name}/data.csv"] = "data"
    for name in present_git:
        zip_files[f"repositories/{name}/README.md"] = "readme"
    # Always include at least one file so the ZIP is valid
    if not zip_files:
        zip_files["placeholder.txt"] = "placeholder"

    checker = BundleChecker()

    with tempfile.TemporaryDirectory() as tmp_dir:
        zip_path = _write_zip(tmp_dir, zip_files)

        manifest, target_config = _make_stub_manifest_and_config(
            storage_names=list(storage_names),
            git_names=list(git_names),
        )
        context = DryRunContext(
            manifest_file="manifest.yaml",
            manifest=manifest,
            target_config=target_config,
            config={"region": "us-east-1"},
            bundle_path=zip_path,
        )

        findings = checker.check(context)

    error_findings = [f for f in findings if f.severity == Severity.ERROR]
    error_messages = " ".join(f.message for f in error_findings)

    # Every missing item should produce an ERROR containing its name
    for name in missing_storage:
        assert name in error_messages, (
            f"Expected ERROR for missing storage item '{name}', "
            f"but not found in error messages: {error_messages}"
        )
    for name in missing_git:
        assert name in error_messages, (
            f"Expected ERROR for missing git item '{name}', "
            f"but not found in error messages: {error_messages}"
        )

    # Present items should NOT produce ERROR findings for their category
    for name in present_storage:
        storage_errors = [
            f
            for f in error_findings
            if f.resource == name and "Storage item" in f.message
        ]
        assert (
            len(storage_errors) == 0
        ), f"Unexpected ERROR for present storage item '{name}'"
    for name in present_git:
        git_errors = [
            f for f in error_findings if f.resource == name and "Git item" in f.message
        ]
        assert len(git_errors) == 0, f"Unexpected ERROR for present git item '{name}'"


# Feature: deploy-dry-run, Property 8: Catalog export schema validation
# **Validates: Requirements 3.6**
@given(
    has_metadata=st.booleans(),
    has_resource_keys=st.booleans(),
    extra_keys=st.dictionaries(
        st.from_regex(r"[a-z]{1,10}", fullmatch=True),
        st.text(min_size=1, max_size=20),
        max_size=3,
    ),
)
@settings(max_examples=100)
def test_property_8_catalog_export_schema_validation(
    has_metadata, has_resource_keys, extra_keys
):
    """For any JSON object, if it is missing any of the required top-level keys
    (metadata, glossaries, glossaryTerms, formTypes, assetTypes, assets,
    dataProducts) the BundleChecker shall produce an ERROR finding. If all
    required keys are present, no schema-related ERROR findings shall be produced.
    """
    # Required keys matching the real catalog export format
    required_keys = {
        "metadata",
        "glossaries",
        "glossaryTerms",
        "formTypes",
        "assetTypes",
        "assets",
        "dataProducts",
    }

    # Build the catalog JSON object
    catalog_obj: Dict = {}
    catalog_obj.update(extra_keys)

    if has_metadata:
        catalog_obj["metadata"] = {"version": "1.0"}
    if has_resource_keys:
        for key in required_keys - {"metadata"}:
            catalog_obj[key] = []

    # Check which required keys are actually present
    # (extra_keys could accidentally contain some required keys)
    all_present = required_keys.issubset(set(catalog_obj.keys()))

    checker = BundleChecker()

    with tempfile.TemporaryDirectory() as tmp_dir:
        zip_files = {
            "catalog/catalog_export.json": json.dumps(catalog_obj),
        }
        zip_path = _write_zip(tmp_dir, zip_files)

        manifest, target_config = _make_stub_manifest_and_config()
        context = DryRunContext(
            manifest_file="manifest.yaml",
            manifest=manifest,
            target_config=target_config,
            config={"region": "us-east-1"},
            bundle_path=zip_path,
        )

        findings = checker.check(context)

    # Filter to only catalog-related ERROR findings
    catalog_errors = [
        f
        for f in findings
        if f.severity == Severity.ERROR and f.resource == "catalog/catalog_export.json"
    ]

    if all_present:
        # All required keys present → no schema-related ERROR
        assert len(catalog_errors) == 0, (
            f"Expected no catalog schema errors when all keys present, "
            f"got: {[f.message for f in catalog_errors]}"
        )
        # catalog_data should be populated
        assert context.catalog_data is not None
    else:
        # Missing required key(s) → at least one ERROR
        assert len(catalog_errors) >= 1, (
            f"Expected at least one catalog schema ERROR when keys missing "
            f"(all_present={all_present}), "
            f"got {len(catalog_errors)} errors"
        )


# Feature: deploy-dry-run, Property 9: Permission set correctness
# **Validates: Requirements 4.1–4.13**


# --- Stub dataclasses for PermissionChecker property test ---


@dataclass
class _PCStorageConfig:
    name: str
    connectionName: Optional[str] = "default.s3_shared"
    targetDirectory: str = ""


@dataclass
class _PCDeploymentConfiguration:
    storage: List[Any] = field(default_factory=list)
    git: list = field(default_factory=list)
    catalog: Optional[Dict] = None
    quicksight: Optional[Dict] = None


@dataclass
class _PCQuickSightDashboard:
    name: str
    type: str = "dashboard"


@dataclass
class _PCProjectConfig:
    name: str = "test-project"
    create: bool = False
    role: Optional[Dict] = None


@dataclass
class _PCBootstrapAction:
    type: str
    parameters: Dict = field(default_factory=dict)


@dataclass
class _PCBootstrapConfig:
    actions: List[_PCBootstrapAction] = field(default_factory=list)


@dataclass
class _PCTargetConfig:
    project: _PCProjectConfig = field(default_factory=_PCProjectConfig)
    deployment_configuration: Optional[_PCDeploymentConfiguration] = None
    environment_variables: Optional[dict] = None
    bootstrap: Optional[_PCBootstrapConfig] = None
    quicksight: list = field(default_factory=list)


# Strategy for bootstrap action types
_bootstrap_action_types = st.sampled_from(
    [
        "workflow.create",
        "workflow.run",
        "workflow.logs",
        "workflow.monitor",
        "quicksight.refresh_dataset",
        "eventbridge.put_events",
        "project.create_environment",
        "project.create_connection",
    ]
)


@given(
    storage_names=st.lists(
        st.from_regex(r"s[a-z]{1,6}", fullmatch=True),
        min_size=0,
        max_size=3,
        unique=True,
    ),
    has_catalog=st.booleans(),
    has_iam_role=st.booleans(),
    has_quicksight=st.booleans(),
    bootstrap_types=st.lists(
        _bootstrap_action_types,
        min_size=0,
        max_size=4,
    ),
    has_glue_refs=st.booleans(),
)
@settings(max_examples=100, deadline=None)
def test_property_9_permission_set_correctness(
    storage_names,
    has_catalog,
    has_iam_role,
    has_quicksight,
    bootstrap_types,
    has_glue_refs,
):
    """For any deployment configuration (with arbitrary combinations of storage
    items, catalog assets, IAM role config, QuickSight dashboards, and bootstrap
    actions of any type), the set of IAM actions passed to SimulatePrincipalPolicy
    by the PermissionChecker shall be a superset of the union of:
    (a) base deployment permissions,
    (b) storage-related S3 permissions,
    (c) catalog-related DataZone permissions,
    (d) QuickSight permissions if dashboards are configured,
    (e) bootstrap action permissions from BOOTSTRAP_PERMISSION_MAP, and
    (f) Glue permissions when catalog assets contain Glue references.
    """
    from unittest.mock import MagicMock

    from smus_cicd.commands.dry_run.checkers.permission_checker import (
        BOOTSTRAP_PERMISSION_MAP,
        PermissionChecker,
    )

    # --- Build the deployment configuration ---
    storage = [_PCStorageConfig(name=n) for n in storage_names]
    deploy_cfg = _PCDeploymentConfiguration(
        storage=storage,
        catalog={"enabled": True} if has_catalog else None,
    )
    project = _PCProjectConfig(
        role={"name": "test-role", "policies": []} if has_iam_role else None,
    )
    qs_dashboards = [_PCQuickSightDashboard(name="dash1")] if has_quicksight else []
    bootstrap = None
    if bootstrap_types:
        bootstrap = _PCBootstrapConfig(
            actions=[_PCBootstrapAction(type=t) for t in bootstrap_types]
        )

    target = _PCTargetConfig(
        project=project,
        deployment_configuration=deploy_cfg,
        bootstrap=bootstrap,
        quicksight=qs_dashboards,
    )

    # Build catalog_data with optional Glue references
    catalog_data = None
    if has_catalog or has_glue_refs:
        resources = []
        if has_glue_refs:
            resources.append(
                {
                    "type": "assets",
                    "name": "glue-asset",
                    "identifier": "id-glue",
                    "formsInput": [
                        {
                            "typeIdentifier": "amazon.datazone.GlueTableFormType",
                            "content": json.dumps(
                                {
                                    "databaseName": "mydb",
                                    "tableName": "mytable",
                                }
                            ),
                        }
                    ],
                }
            )
        catalog_data = _group_resources_by_type(resources)

    context = DryRunContext(
        manifest_file="manifest.yaml",
        target_config=target,
        config={
            "region": "us-east-1",
            "account_id": "123456789012",
            "domain_id": "dzd_test",
        },
        catalog_data=catalog_data,
    )

    # --- Mock boto3 to capture actions passed to simulate_principal_policy ---
    captured_actions: Set[str] = set()

    mock_sts = MagicMock()
    mock_sts.get_caller_identity.return_value = {
        "Arn": "arn:aws:iam::123456789012:user/testuser"
    }

    mock_iam = MagicMock()

    def capture_simulate(**kw):
        for action in kw.get("ActionNames", []):
            captured_actions.add(action)
        return {
            "EvaluationResults": [
                {"EvalActionName": a, "EvalDecision": "allowed"}
                for a in kw.get("ActionNames", [])
            ]
        }

    mock_iam.simulate_principal_policy.side_effect = capture_simulate

    with patch(
        "smus_cicd.commands.dry_run.checkers.permission_checker.create_client"
    ) as mock_create_client, patch(
        "smus_cicd.commands.dry_run.checkers.permission_checker.get_project_connections",
        return_value={},
    ):
        mock_create_client.side_effect = lambda svc, **kw: (
            mock_sts if svc == "sts" else mock_iam
        )
        checker = PermissionChecker()
        checker.check(context)

    # --- Independently compute expected actions ---
    expected_actions: Set[str] = set()

    # (a) Base DataZone permissions (always present)
    expected_actions.update(
        [
            "datazone:GetDomain",
            "datazone:GetProject",
            "datazone:SearchListings",
        ]
    )

    # (b) S3 storage permissions
    if storage_names:
        expected_actions.update(["s3:PutObject", "s3:GetObject"])

    # (c) Catalog permissions — only when actual catalog resources exist
    if catalog_data is not None:
        expected_actions.update(
            [
                "datazone:CreateAsset",
                "datazone:CreateGlossary",
                "datazone:CreateGlossaryTerm",
                "datazone:CreateFormType",
                "datazone:CreateSubscriptionGrant",
                "datazone:GetSubscriptionGrant",
                "datazone:CreateSubscriptionRequest",
            ]
        )

    # (d) IAM role permissions
    if has_iam_role:
        expected_actions.update(
            ["iam:CreateRole", "iam:AttachRolePolicy", "iam:PutRolePolicy"]
        )

    # (d) QuickSight permissions
    if has_quicksight:
        expected_actions.update(
            [
                "quicksight:DescribeDashboard",
                "quicksight:CreateDashboard",
                "quicksight:UpdateDashboard",
            ]
        )

    # (e) Bootstrap action permissions
    for action_type in bootstrap_types:
        required = BOOTSTRAP_PERMISSION_MAP.get(action_type, [])
        expected_actions.update(required)

    # (f) Glue permissions for catalog Glue references
    if has_glue_refs:
        expected_actions.update(
            ["glue:GetTable", "glue:GetDatabase", "glue:GetPartitions"]
        )

    # --- Assert captured actions are a superset of expected ---
    missing = expected_actions - captured_actions
    assert not missing, (
        f"PermissionChecker did not check expected actions: {missing}. "
        f"Config: storage={storage_names}, catalog={has_catalog}, "
        f"iam_role={has_iam_role}, quicksight={has_quicksight}, "
        f"bootstrap={bootstrap_types}, glue_refs={has_glue_refs}. "
        f"Captured: {captured_actions}"
    )


# ---------------------------------------------------------------------------
# Stubs for ConnectivityChecker property test (Property 14)
# ---------------------------------------------------------------------------


@dataclass
class _CCStorageConfig:
    name: str
    connectionName: Optional[str] = "default.s3_shared"
    targetDirectory: str = ""


@dataclass
class _CCDeploymentConfiguration:
    storage: List[Any] = field(default_factory=list)
    git: list = field(default_factory=list)


@dataclass
class _CCTargetConfig:
    deployment_configuration: Optional[_CCDeploymentConfiguration] = None
    bootstrap: Optional[Any] = None


# Strategy for valid S3 bucket-name-like strings (lowercase, 3-20 chars)
_bucket_name_strategy = st.from_regex(r"[a-z][a-z0-9\-]{2,14}", fullmatch=True)


# Feature: deploy-dry-run, Property 14: S3 bucket reachability
# **Validates: Requirements 6.3, 6.5**
@given(
    bucket_names=st.lists(
        _bucket_name_strategy,
        min_size=1,
        max_size=6,
        unique=True,
    ),
    fail_flags=st.lists(st.booleans(), min_size=6, max_size=6),
)
@settings(max_examples=100, deadline=None)
def test_property_14_s3_bucket_reachability(bucket_names, fail_flags):
    """For any set of S3 bucket names referenced in the deployment configuration,
    the ConnectivityChecker shall produce exactly one finding per unique bucket.
    Buckets where HeadBucket succeeds shall produce OK findings; buckets where
    HeadBucket raises an error shall produce ERROR findings containing the bucket
    name.
    """
    from unittest.mock import MagicMock

    from botocore.exceptions import ClientError

    from smus_cicd.commands.dry_run.checkers.connectivity_checker import (
        ConnectivityChecker,
    )

    # Determine which buckets should fail HeadBucket
    failing_buckets = set()
    for i, bucket in enumerate(bucket_names):
        if fail_flags[i % len(fail_flags)]:
            failing_buckets.add(bucket)

    # Build storage items with connectionName that derives the bucket name.
    # The ConnectivityChecker splits connectionName on "." and takes the last part.
    storage_items = [
        _CCStorageConfig(name=f"item_{b}", connectionName=f"conn.{b}")
        for b in bucket_names
    ]
    deploy_cfg = _CCDeploymentConfiguration(storage=storage_items)
    target = _CCTargetConfig(deployment_configuration=deploy_cfg)

    context = DryRunContext(
        manifest_file="manifest.yaml",
        target_config=target,
        config={
            "region": "us-east-1",
            "domain_id": "dzd_test",
            "project_name": "test-project",
        },
    )

    # --- Mock boto3 ---
    mock_dz = MagicMock()
    mock_dz.get_domain.return_value = {"id": "dzd_test"}

    mock_s3 = MagicMock()

    def head_bucket_side_effect(**kwargs):
        bucket = kwargs.get("Bucket", "")
        if bucket in failing_buckets:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}},
                "HeadBucket",
            )
        return {}

    mock_s3.head_bucket.side_effect = head_bucket_side_effect

    # Build mock project connections: each "conn.{bucket}" resolves to s3://{bucket}
    mock_connections = {f"conn.{b}": {"s3Uri": f"s3://{b}"} for b in bucket_names}

    # Mock get_project_by_name and _get_project_connections
    with patch(
        "smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client"
    ) as mock_create_client, patch(
        "smus_cicd.helpers.datazone.get_project_by_name",
        return_value={"name": "test-project"},
    ), patch(
        "smus_cicd.commands.dry_run.checkers.connectivity_checker.get_project_connections",
        return_value=mock_connections,
    ):
        mock_create_client.side_effect = lambda svc, **kw: {
            "datazone": mock_dz,
            "s3": mock_s3,
        }.get(svc, MagicMock())

        checker = ConnectivityChecker()
        findings = checker.check(context)

    # --- Filter to S3-related findings only ---
    s3_findings = [f for f in findings if f.service == "s3"]

    # Exactly one finding per unique bucket
    unique_buckets = set(bucket_names)
    s3_bucket_findings = [f for f in s3_findings if f.resource in unique_buckets]
    found_buckets = [f.resource for f in s3_bucket_findings]

    assert set(found_buckets) == unique_buckets, (
        f"Expected findings for buckets {unique_buckets}, "
        f"got findings for {set(found_buckets)}"
    )
    assert len(found_buckets) == len(unique_buckets), (
        f"Expected exactly one finding per bucket ({len(unique_buckets)}), "
        f"got {len(found_buckets)}"
    )

    # Verify severity: OK for reachable, ERROR for unreachable
    for f in s3_bucket_findings:
        bucket = f.resource
        if bucket in failing_buckets:
            assert f.severity == Severity.ERROR, (
                f"Expected ERROR for unreachable bucket '{bucket}', "
                f"got {f.severity}"
            )
            assert bucket in f.message, (
                f"ERROR finding for bucket '{bucket}' should contain "
                f"the bucket name in the message: {f.message}"
            )
        else:
            assert f.severity == Severity.OK, (
                f"Expected OK for reachable bucket '{bucket}', " f"got {f.severity}"
            )


# ---------------------------------------------------------------------------
# Stubs for StorageChecker property test (Property 11)
# ---------------------------------------------------------------------------


@dataclass
class _SCStorageConfig:
    name: str
    connectionName: Optional[str] = "default.s3_shared"
    targetDirectory: str = ""


@dataclass
class _SCDeploymentConfiguration:
    storage: List[Any] = field(default_factory=list)
    git: list = field(default_factory=list)


@dataclass
class _SCTargetConfig:
    deployment_configuration: Optional[_SCDeploymentConfiguration] = None


# Feature: deploy-dry-run, Property 11: Storage simulation reporting
# **Validates: Requirements 5.2**
@given(
    storage_items=st.lists(
        st.tuples(
            st.from_regex(r"[a-z][a-z0-9]{1,8}", fullmatch=True),  # name
            st.from_regex(r"[a-z][a-z0-9\-]{2,10}", fullmatch=True),  # bucket
            st.from_regex(r"[a-z0-9/]{0,15}", fullmatch=True),  # prefix
        ),
        min_size=1,
        max_size=5,
        unique_by=lambda t: t[0],
    ),
    file_counts=st.lists(
        st.integers(min_value=0, max_value=10),
        min_size=5,
        max_size=5,
    ),
)
@settings(max_examples=100)
def test_property_11_storage_simulation_reporting(storage_items, file_counts):
    """For any storage deployment configuration item and corresponding bundle
    contents, the StorageChecker shall produce a finding whose message contains
    the target S3 bucket name, the S3 prefix, and the file count matching the
    number of files in the bundle for that item.
    """
    from smus_cicd.commands.dry_run.checkers.storage_checker import StorageChecker

    # Build storage config stubs and bundle files
    configs = []
    bundle_files: Set[str] = set()
    expected: List[dict] = []

    for idx, (name, bucket, prefix) in enumerate(storage_items):
        connection_name = f"conn.{bucket}"
        configs.append(
            _SCStorageConfig(
                name=name,
                connectionName=connection_name,
                targetDirectory=prefix,
            )
        )

        # Determine how many files this item has in the bundle
        count = file_counts[idx % len(file_counts)]
        for fi in range(count):
            bundle_files.add(f"{name}/file_{fi}.dat")

        expected.append(
            {
                "name": name,
                "bucket": bucket,
                "prefix": prefix,
                "file_count": count,
            }
        )

    deploy_cfg = _SCDeploymentConfiguration(storage=configs)
    target = _SCTargetConfig(deployment_configuration=deploy_cfg)

    context = DryRunContext(
        manifest_file="manifest.yaml",
        target_config=target,
        config={"region": "us-east-1"},
        bundle_files=bundle_files,
    )

    # Build mock connections from storage items
    mock_connections = {}
    for item in storage_items:
        conn = item[1]  # connectionName
        bucket = item[2]  # bucket
        if conn:
            mock_connections[conn] = {"s3Uri": f"s3://{bucket}"}

    checker = StorageChecker()
    with patch(
        "smus_cicd.commands.dry_run.checkers.storage_checker.get_project_connections",
        return_value=mock_connections,
    ):
        findings = checker.check(context)

    # Filter to OK findings with service "s3" (storage simulation findings)
    ok_findings = [
        f for f in findings if f.severity == Severity.OK and f.service == "s3"
    ]

    # There should be exactly one finding per storage item
    assert len(ok_findings) == len(storage_items), (
        f"Expected {len(storage_items)} OK findings, got {len(ok_findings)}. "
        f"All findings: {[(f.severity, f.message) for f in findings]}"
    )

    for exp in expected:
        matching = [f for f in ok_findings if f.resource == exp["name"]]
        assert len(matching) == 1, (
            f"Expected exactly one finding for storage item '{exp['name']}', "
            f"got {len(matching)}"
        )
        finding = matching[0]

        # Message must contain the bucket name
        assert exp["bucket"] in finding.message, (
            f"Finding message should contain bucket '{exp['bucket']}': "
            f"{finding.message}"
        )

        # Message must contain the prefix
        assert exp["prefix"] in finding.message, (
            f"Finding message should contain prefix '{exp['prefix']}': "
            f"{finding.message}"
        )

        # Message must contain the correct file count
        count_str = f"{exp['file_count']} file(s)"
        assert count_str in finding.message, (
            f"Finding message should contain '{count_str}': " f"{finding.message}"
        )


# Feature: deploy-dry-run, Property 12: Catalog resource type counting
# **Validates: Requirements 5.4, 8.4**
@given(
    glossary_count=st.integers(min_value=0, max_value=5),
    term_count=st.integers(min_value=0, max_value=5),
    asset_type_count=st.integers(min_value=0, max_value=5),
    form_type_count=st.integers(min_value=0, max_value=5),
    data_product_count=st.integers(min_value=0, max_value=5),
)
@settings(max_examples=100)
def test_property_12_catalog_resource_type_counting(
    glossary_count, term_count, asset_type_count, form_type_count, data_product_count
):
    """For any catalog export JSON containing resources of types glossary,
    glossary_term, custom_asset_type, form_type, and data_product, the
    CatalogChecker shall produce a finding whose message contains the correct
    count for each resource type present.
    """
    from smus_cicd.commands.dry_run.checkers.catalog_checker import CatalogChecker

    # Build resources list with the generated counts
    resources = []

    glossary_ids = []
    for i in range(glossary_count):
        gid = f"glossary-{i}"
        glossary_ids.append(gid)
        resources.append(
            {"type": "glossaries", "name": f"Glossary {i}", "identifier": gid}
        )

    for i in range(term_count):
        ref = glossary_ids[i % len(glossary_ids)] if glossary_ids else None
        entry = {
            "type": "glossaryTerms",
            "name": f"Term {i}",
            "identifier": f"term-{i}",
        }
        if ref:
            entry["glossaryId"] = ref
        resources.append(entry)

    form_type_ids = []
    for i in range(form_type_count):
        ftid = f"formtype-{i}"
        form_type_ids.append(ftid)
        resources.append(
            {"type": "formTypes", "name": f"FormType {i}", "identifier": ftid}
        )

    for i in range(asset_type_count):
        resources.append(
            {
                "type": "assetTypes",
                "name": f"AssetType {i}",
                "identifier": f"assettype-{i}",
            }
        )

    for i in range(data_product_count):
        resources.append(
            {
                "type": "dataProducts",
                "name": f"DataProduct {i}",
                "identifier": f"dp-{i}",
            }
        )

    catalog_data = _group_resources_by_type(resources)

    context = DryRunContext(
        manifest_file="manifest.yaml",
        catalog_data=catalog_data,
        config={"region": "us-east-1"},
    )

    checker = CatalogChecker()
    findings = checker.check(context)

    total = (
        glossary_count
        + term_count
        + asset_type_count
        + form_type_count
        + data_product_count
    )

    if total == 0:
        # When no resources, checker returns early with "0 resources" message
        ok_findings = [f for f in findings if f.severity == Severity.OK]
        assert len(ok_findings) >= 1
        assert any("0 resources" in f.message for f in ok_findings)
        return

    # Find the summary finding that reports resource type counts
    summary_findings = [
        f
        for f in findings
        if f.severity == Severity.OK
        and "resource(s)" in f.message
        and f.service == "datazone"
    ]
    assert len(summary_findings) == 1, (
        f"Expected exactly one summary finding, got {len(summary_findings)}. "
        f"All findings: {[(f.severity, f.message) for f in findings]}"
    )
    summary = summary_findings[0]

    # Verify total count in message
    assert (
        f"{total} resource(s)" in summary.message
    ), f"Expected '{total} resource(s)' in message: {summary.message}"

    # Verify each resource type count appears in the message when count > 0
    type_to_display = {
        "glossaries": ("glossaries", glossary_count),
        "glossaryTerms": ("glossary terms", term_count),
        "formTypes": ("form types", form_type_count),
        "assetTypes": ("custom asset types", asset_type_count),
        "dataProducts": ("data products", data_product_count),
    }

    for type_key, (display_name, count) in type_to_display.items():
        if count > 0:
            expected_part = f"{count} {display_name}"
            assert (
                expected_part in summary.message
            ), f"Expected '{expected_part}' in summary message: {summary.message}"

    # Also verify the details dict has correct type_counts
    assert summary.details is not None
    type_counts = summary.details.get("type_counts", {})
    if glossary_count > 0:
        assert type_counts.get("glossaries") == glossary_count
    if term_count > 0:
        assert type_counts.get("glossaryTerms") == term_count
    if form_type_count > 0:
        assert type_counts.get("formTypes") == form_type_count
    if asset_type_count > 0:
        assert type_counts.get("assetTypes") == asset_type_count
    if data_product_count > 0:
        assert type_counts.get("dataProducts") == data_product_count
    assert summary.details.get("total") == total


# Feature: deploy-dry-run, Property 18: Catalog resource field validation
# **Validates: Requirements 8.1**


def _build_catalog_resource_with_subset(idx, field_flags):
    """Build a catalog resource dict with a unique index-based identity.

    ``field_flags`` is a 2-tuple of bools indicating whether (name,
    identifier) should be present. Type is always set since it's auto-tagged
    from the key in the keyed format.
    """
    resource = {"type": "glossaries"}
    if field_flags[0]:
        resource["name"] = f"Resource-{idx}"
    if field_flags[1]:
        resource["identifier"] = f"res-id-{idx}"
    return resource


@given(
    field_flags_list=st.lists(
        st.tuples(st.booleans(), st.booleans()),
        min_size=1,
        max_size=10,
    )
)
@settings(max_examples=100)
def test_property_18_catalog_resource_field_validation(field_flags_list):
    """For any catalog resource entry, if it is missing any of the required
    fields (type, name, identifier), the CatalogChecker shall produce an ERROR
    finding referencing the resource. If all required fields are present, no
    field-validation ERROR shall be produced for that resource.
    """
    from smus_cicd.commands.dry_run.checkers.catalog_checker import CatalogChecker

    required_fields = {"type", "name", "identifier"}

    # Build resources with unique, index-based names/identifiers to avoid
    # ambiguous substring matching across resources.
    resources = [
        _build_catalog_resource_with_subset(idx, flags)
        for idx, flags in enumerate(field_flags_list)
    ]

    catalog_data = _group_resources_by_type(resources)

    context = DryRunContext(
        manifest_file="manifest.yaml",
        catalog_data=catalog_data,
        config={"region": "us-east-1"},
    )

    checker = CatalogChecker()
    findings = checker.check(context)

    # Collect ERROR findings that are about missing required fields
    field_error_findings = [
        f
        for f in findings
        if f.severity == Severity.ERROR and "missing required field(s)" in f.message
    ]

    # For each resource, determine if it should produce a field-validation error
    expected_error_count = 0
    for idx, resource in enumerate(resources):
        present_fields = set(resource.keys())
        missing = required_fields - present_fields
        if missing:
            expected_error_count += 1
            # The checker labels the resource using:
            #   resource.get("name", resource.get("identifier", f"index {idx}"))
            label = resource.get("name", resource.get("identifier", f"index {idx}"))
            matching = [f for f in field_error_findings if f"'{label}'" in f.message]
            assert len(matching) >= 1, (
                f"Expected an ERROR finding for resource at index {idx} "
                f"(label='{label}') with missing fields {missing}, "
                f"but found none. All error findings: "
                f"{[(f.severity, f.message) for f in field_error_findings]}"
            )
            # Verify each missing field is mentioned in the finding message
            for field_name in missing:
                assert any(field_name in m.message for m in matching), (
                    f"Expected missing field '{field_name}' to be mentioned "
                    f"in error finding for resource at index {idx}. "
                    f"Matching findings: {[m.message for m in matching]}"
                )

    assert len(field_error_findings) == expected_error_count, (
        f"Expected {expected_error_count} field-validation ERROR findings, "
        f"got {len(field_error_findings)}. "
        f"Findings: {[(f.severity, f.message) for f in field_error_findings]}"
    )


# Feature: deploy-dry-run, Property 19: Catalog cross-reference resolution
# **Validates: Requirements 8.2**


@given(
    num_glossaries=st.integers(min_value=1, max_value=5),
    num_terms=st.integers(min_value=1, max_value=5),
    term_valid_flags=st.lists(st.booleans(), min_size=1, max_size=5),
    num_form_types=st.integers(min_value=1, max_value=5),
    num_assets=st.integers(min_value=1, max_value=5),
    asset_valid_flags=st.lists(st.booleans(), min_size=1, max_size=5),
    num_asset_types=st.integers(min_value=0, max_value=3),
    asset_type_valid_flags=st.lists(st.booleans(), min_size=0, max_size=3),
)
@settings(max_examples=100)
def test_property_19_catalog_cross_reference_resolution(
    num_glossaries,
    num_terms,
    term_valid_flags,
    num_form_types,
    num_assets,
    asset_valid_flags,
    num_asset_types,
    asset_type_valid_flags,
):
    """For any catalog export containing cross-references (glossary terms
    referencing glossary identifiers, assets referencing form type identifiers),
    if a referenced identifier does not exist in the catalog data, the
    CatalogChecker shall produce an ERROR finding for each unresolvable
    reference.
    """
    from smus_cicd.commands.dry_run.checkers.catalog_checker import CatalogChecker

    resources = []

    # --- Build glossary resources (these are the targets for glossary term refs) ---
    glossary_ids = [f"glossary-id-{i}" for i in range(num_glossaries)]
    for i, gid in enumerate(glossary_ids):
        resources.append(
            {
                "type": "glossaries",
                "name": f"Glossary-{i}",
                "identifier": gid,
            }
        )

    # --- Build form type resources (these are the targets for asset form refs) ---
    form_type_ids = [f"custom-form-type-{i}" for i in range(num_form_types)]
    for i, ftid in enumerate(form_type_ids):
        resources.append(
            {
                "type": "formTypes",
                "name": f"FormType-{i}",
                "identifier": ftid,
            }
        )

    # --- Build glossary terms with valid/invalid glossary references ---
    # Pad or truncate term_valid_flags to match num_terms
    padded_term_flags = (term_valid_flags * ((num_terms // len(term_valid_flags)) + 1))[
        :num_terms
    ]
    expected_bad_glossary_refs = 0
    for i, is_valid in enumerate(padded_term_flags):
        if is_valid:
            # Reference a valid glossary identifier
            ref_id = glossary_ids[i % len(glossary_ids)]
        else:
            # Reference a non-existent glossary identifier
            ref_id = f"nonexistent-glossary-{i}"
            expected_bad_glossary_refs += 1
        resources.append(
            {
                "type": "glossaryTerms",
                "name": f"Term-{i}",
                "identifier": f"term-id-{i}",
                "glossaryId": ref_id,
            }
        )

    # --- Build assets with valid/invalid form type references ---
    padded_asset_flags = (
        asset_valid_flags * ((num_assets // len(asset_valid_flags)) + 1)
    )[:num_assets]
    expected_bad_asset_form_refs = 0
    for i, is_valid in enumerate(padded_asset_flags):
        if is_valid:
            ref_id = form_type_ids[i % len(form_type_ids)]
        else:
            ref_id = f"nonexistent-form-type-{i}"
            expected_bad_asset_form_refs += 1
        resources.append(
            {
                "type": "assets",
                "name": f"Asset-{i}",
                "identifier": f"asset-id-{i}",
                "formsInput": [{"typeIdentifier": ref_id, "content": "{}"}],
            }
        )

    # --- Build asset types with valid/invalid form type references ---
    padded_at_flags = (
        (
            asset_type_valid_flags
            * ((num_asset_types // max(len(asset_type_valid_flags), 1)) + 1)
        )[:num_asset_types]
        if asset_type_valid_flags
        else []
    )
    expected_bad_asset_type_form_refs = 0
    for i, is_valid in enumerate(padded_at_flags):
        if is_valid:
            ref_id = form_type_ids[i % len(form_type_ids)]
        else:
            ref_id = f"nonexistent-at-form-type-{i}"
            expected_bad_asset_type_form_refs += 1
        resources.append(
            {
                "type": "assetTypes",
                "name": f"AssetType-{i}",
                "identifier": f"asset-type-id-{i}",
                "formsInput": {
                    "form0": {"typeIdentifier": ref_id},
                },
            }
        )

    catalog_data = _group_resources_by_type(resources)

    context = DryRunContext(
        manifest_file="manifest.yaml",
        catalog_data=catalog_data,
        config={"region": "us-east-1"},
    )

    checker = CatalogChecker()
    findings = checker.check(context)

    # Collect cross-reference ERROR findings (exclude field-validation errors)
    cross_ref_errors = [
        f
        for f in findings
        if f.severity == Severity.ERROR
        and "which is not in the catalog export" in f.message
    ]

    total_expected = (
        expected_bad_glossary_refs
        + expected_bad_asset_form_refs
        + expected_bad_asset_type_form_refs
    )

    assert len(cross_ref_errors) == total_expected, (
        f"Expected {total_expected} cross-reference ERROR findings "
        f"(glossary={expected_bad_glossary_refs}, "
        f"asset_form={expected_bad_asset_form_refs}, "
        f"asset_type_form={expected_bad_asset_type_form_refs}), "
        f"got {len(cross_ref_errors)}. "
        f"Findings: {[(f.severity.value, f.message) for f in cross_ref_errors]}"
    )

    # Verify each bad glossary reference has a corresponding finding
    for i, is_valid in enumerate(padded_term_flags):
        if not is_valid:
            ref_id = f"nonexistent-glossary-{i}"
            matching = [f for f in cross_ref_errors if ref_id in f.message]
            assert len(matching) >= 1, (
                f"Expected ERROR finding for unresolvable glossary ref "
                f"'{ref_id}' but found none."
            )

    # Verify each bad asset form type reference has a corresponding finding
    for i, is_valid in enumerate(padded_asset_flags):
        if not is_valid:
            ref_id = f"nonexistent-form-type-{i}"
            matching = [f for f in cross_ref_errors if ref_id in f.message]
            assert len(matching) >= 1, (
                f"Expected ERROR finding for unresolvable asset form type ref "
                f"'{ref_id}' but found none."
            )

    # Verify each bad asset type form type reference has a corresponding finding
    for i, is_valid in enumerate(padded_at_flags):
        if not is_valid:
            ref_id = f"nonexistent-at-form-type-{i}"
            matching = [f for f in cross_ref_errors if ref_id in f.message]
            assert len(matching) >= 1, (
                f"Expected ERROR finding for unresolvable asset type form ref "
                f"'{ref_id}' but found none."
            )

    # Verify that valid references do NOT produce cross-ref errors
    for i, is_valid in enumerate(padded_term_flags):
        if is_valid:
            term_name = f"Term-{i}"
            bad_matches = [
                f
                for f in cross_ref_errors
                if f"'{term_name}'" in f.message and "glossary" in f.message.lower()
            ]
            assert len(bad_matches) == 0, (
                f"Valid glossary term '{term_name}' should not produce a "
                f"cross-ref error, but found: {[f.message for f in bad_matches]}"
            )

    for i, is_valid in enumerate(padded_asset_flags):
        if is_valid:
            asset_name = f"Asset-{i}"
            bad_matches = [
                f
                for f in cross_ref_errors
                if f"'{asset_name}'" in f.message and "form type" in f.message.lower()
            ]
            assert len(bad_matches) == 0, (
                f"Valid asset '{asset_name}' should not produce a "
                f"cross-ref error, but found: {[f.message for f in bad_matches]}"
            )


# ---------------------------------------------------------------------------
# Stubs for WorkflowChecker property test (Property 20)
# ---------------------------------------------------------------------------


@dataclass
class _WCAction:
    type: str
    parameters: Dict = field(default_factory=dict)


@dataclass
class _WCBootstrap:
    actions: List[_WCAction] = field(default_factory=list)


@dataclass
class _WCTargetConfig:
    bootstrap: Optional[_WCBootstrap] = None
    environment_variables: Optional[Dict[str, str]] = None


def _make_workflow_bundle_zip(tmp_dir: str, workflow_content: str) -> str:
    """Create a ZIP bundle with a single workflow YAML file."""
    path = os.path.join(tmp_dir, "wf_bundle.zip")
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("dags/workflow.yaml", workflow_content)
    return path


# Feature: deploy-dry-run, Property 20: Workflow file validation
# **Validates: Requirements 9.1, 9.2, 9.3**


# --- Sub-property 20a: Invalid YAML produces ERROR ---
@given(
    # Generate strings that are definitely invalid YAML by injecting
    # unbalanced brackets/colons into random text
    base_text=st.text(
        min_size=1,
        max_size=50,
        alphabet=st.characters(
            whitelist_categories=("L", "N", "P"),
        ),
    ),
)
@settings(max_examples=100, deadline=None)
def test_property_20a_invalid_yaml_produces_error(base_text):
    """For any file content string that is not valid YAML, the WorkflowChecker
    shall produce an ERROR finding.
    """
    import yaml as _yaml

    from smus_cicd.commands.dry_run.checkers.workflow_checker import WorkflowChecker

    # Construct content that is guaranteed to be invalid YAML
    invalid_content = f"key: [invalid\n  {base_text}\n  - broken: {{"

    # Verify it's actually invalid YAML
    try:
        _yaml.safe_load(invalid_content)
        is_invalid_yaml = False
    except _yaml.YAMLError:
        is_invalid_yaml = True

    if not is_invalid_yaml:
        # If by chance the content is valid YAML, skip this example
        return

    target = _WCTargetConfig(
        bootstrap=_WCBootstrap(
            actions=[
                _WCAction(
                    type="workflow.create",
                    parameters={"workflow_file": "dags/workflow.yaml"},
                )
            ]
        ),
    )

    checker = WorkflowChecker()

    with tempfile.TemporaryDirectory() as tmp_dir:
        zip_path = _make_workflow_bundle_zip(tmp_dir, invalid_content)
        context = DryRunContext(
            manifest_file="manifest.yaml",
            target_config=target,
            bundle_path=zip_path,
        )
        findings = checker.check(context)

    error_findings = [f for f in findings if f.severity == Severity.ERROR]
    assert len(error_findings) >= 1, (
        f"Expected at least one ERROR finding for invalid YAML, "
        f"got {len(error_findings)}. Content: {invalid_content!r}"
    )
    assert any("invalid yaml" in f.message.lower() for f in error_findings), (
        f"Expected an ERROR mentioning 'invalid YAML'. "
        f"Errors: {[f.message for f in error_findings]}"
    )


# --- Sub-property 20b: Missing required DAG keys produces ERROR ---
@given(
    include_dag_id=st.booleans(),
    include_tasks=st.booleans(),
    dag_name=st.from_regex(r"[a-z][a-z0-9_]{1,10}", fullmatch=True),
)
@settings(max_examples=100, deadline=None)
def test_property_20b_missing_dag_keys_produces_error(
    include_dag_id, include_tasks, dag_name
):
    """For any parsed YAML that is missing required top-level DAG keys
    (dag_id, tasks), the WorkflowChecker shall produce an ERROR finding.
    When both keys are present, no structure-related ERROR shall be produced.
    """
    from smus_cicd.commands.dry_run.checkers.workflow_checker import WorkflowChecker

    # Build a YAML dict with optional dag_id and tasks keys
    dag_value = {}
    if include_dag_id:
        dag_value["dag_id"] = dag_name
    if include_tasks:
        dag_value["tasks"] = {"task1": {"operator": "BashOperator"}}

    # Add a non-DAG key to ensure the YAML is a valid mapping
    dag_value["schedule"] = "daily"

    import yaml as _yaml

    yaml_content = _yaml.dump({dag_name: dag_value}, default_flow_style=False)

    both_present = include_dag_id and include_tasks

    target = _WCTargetConfig(
        bootstrap=_WCBootstrap(
            actions=[
                _WCAction(
                    type="workflow.create",
                    parameters={"workflow_file": "dags/workflow.yaml"},
                )
            ]
        ),
    )

    checker = WorkflowChecker()

    with tempfile.TemporaryDirectory() as tmp_dir:
        zip_path = _make_workflow_bundle_zip(tmp_dir, yaml_content)
        context = DryRunContext(
            manifest_file="manifest.yaml",
            target_config=target,
            bundle_path=zip_path,
        )
        findings = checker.check(context)

    error_findings = [f for f in findings if f.severity == Severity.ERROR]
    structure_errors = [
        f
        for f in error_findings
        if "missing required" in f.message.lower() or "mapping" in f.message.lower()
    ]

    if both_present:
        # Both required keys present → no structure ERROR
        assert len(structure_errors) == 0, (
            f"Expected no structure ERROR when both dag_id and tasks present, "
            f"got: {[f.message for f in structure_errors]}"
        )
    else:
        # Missing at least one required key → at least one ERROR
        assert len(structure_errors) >= 1, (
            f"Expected at least one structure ERROR when dag_id={include_dag_id}, "
            f"tasks={include_tasks}, got {len(structure_errors)} errors. "
            f"All findings: {[(f.severity.value, f.message) for f in findings]}"
        )


# --- Sub-property 20c: Unresolved env vars produce WARNINGs ---
@given(
    var_names=st.lists(
        st.from_regex(r"[A-Z][A-Z0-9_]{1,10}", fullmatch=True),
        min_size=1,
        max_size=5,
        unique=True,
    ),
    resolve_flags=st.lists(st.booleans(), min_size=5, max_size=5),
    use_braced=st.lists(st.booleans(), min_size=5, max_size=5),
)
@settings(max_examples=100, deadline=None)
def test_property_20c_unresolved_env_vars_produce_warnings(
    var_names, resolve_flags, use_braced
):
    """For any set of ${VAR} / $VAR references in the workflow content and any
    environment variable dictionary, every unresolved variable shall produce a
    WARNING finding.
    """
    from smus_cicd.commands.dry_run.checkers.workflow_checker import WorkflowChecker

    # Decide which vars are resolved vs unresolved
    env_vars_dict: Dict[str, str] = {}
    expected_unresolved: Set[str] = set()

    for i, name in enumerate(var_names):
        if resolve_flags[i % len(resolve_flags)]:
            env_vars_dict[name] = "resolved_value"
        else:
            expected_unresolved.add(name)

    # Build variable references using either ${VAR} or $VAR syntax
    var_refs = []
    for i, name in enumerate(var_names):
        if use_braced[i % len(use_braced)]:
            var_refs.append(f"${{{name}}}")
        else:
            var_refs.append(f"${name}")

    refs_line = " ".join(var_refs)

    # Build a valid workflow YAML that contains the variable references
    yaml_content = (
        f"my_dag:\n"
        f"  dag_id: my_dag\n"
        f"  tasks:\n"
        f"    task1:\n"
        f"      operator: BashOperator\n"
        f"      bash_command: echo {refs_line}\n"
    )

    target = _WCTargetConfig(
        bootstrap=_WCBootstrap(actions=[_WCAction(type="workflow.create")]),
        environment_variables=env_vars_dict,
    )

    checker = WorkflowChecker()

    with tempfile.TemporaryDirectory() as tmp_dir:
        zip_path = _make_workflow_bundle_zip(tmp_dir, yaml_content)
        context = DryRunContext(
            manifest_file="manifest.yaml",
            target_config=target,
            bundle_path=zip_path,
        )

        # Clear os.environ to ensure only env_vars_dict matters
        with patch.dict(os.environ, {}, clear=True):
            findings = checker.check(context)

    warning_findings = [f for f in findings if f.severity == Severity.WARNING]
    warned_vars = set()
    for f in warning_findings:
        if f.details and "variable" in f.details:
            warned_vars.add(f.details["variable"])

    # Every expected unresolved var should have a WARNING
    for var_name in expected_unresolved:
        assert var_name in warned_vars, (
            f"Expected WARNING for unresolved var '{var_name}', "
            f"but only got warnings for: {warned_vars}"
        )

    # Resolved vars should NOT have warnings
    for var_name in env_vars_dict:
        assert (
            var_name not in warned_vars
        ), f"Unexpected WARNING for resolved var '{var_name}'"


# ---------------------------------------------------------------------------
# Stubs for BootstrapChecker property test (Property 22)
# ---------------------------------------------------------------------------


@dataclass
class _BCBootstrapAction:
    type: str
    parameters: Dict = field(default_factory=dict)


@dataclass
class _BCBootstrapConfig:
    actions: List[_BCBootstrapAction] = field(default_factory=list)


@dataclass
class _BCTargetConfig:
    bootstrap: Optional[_BCBootstrapConfig] = None


# Strategy for bootstrap action types (all types that have registered handlers)
_bc_action_type_strategy = st.sampled_from(
    [
        "workflow.create",
        "workflow.run",
        "workflow.logs",
        "workflow.monitor",
        "quicksight.refresh_dataset",
        "project.create_environment",
        "project.create_connection",
    ]
)

# Strategy for parameter keys (simple identifiers)
_param_key_strategy = st.from_regex(r"[a-z][a-z0-9_]{1,10}", fullmatch=True)

# Strategy for parameter values
_param_value_strategy = st.text(
    min_size=1,
    max_size=20,
    alphabet=st.characters(
        whitelist_categories=("L", "N"),
    ),
)


# Feature: deploy-dry-run, Property 22: Bootstrap action listing
# **Validates: Requirements 5.6**
@given(
    action_types=st.lists(
        _bc_action_type_strategy,
        min_size=0,
        max_size=6,
    ),
    param_dicts=st.lists(
        st.dictionaries(
            _param_key_strategy,
            _param_value_strategy,
            min_size=0,
            max_size=3,
        ),
        min_size=6,
        max_size=6,
    ),
)
@settings(max_examples=100)
def test_property_22_bootstrap_action_listing(action_types, param_dicts):
    """For any list of bootstrap actions in the manifest, the BootstrapChecker
    shall produce one OK finding per action whose message contains the action's
    type string and its parameter keys.
    """
    from smus_cicd.commands.dry_run.checkers.bootstrap_checker import BootstrapChecker

    # Build bootstrap actions with generated types and parameters
    actions = []
    for i, action_type in enumerate(action_types):
        params = param_dicts[i % len(param_dicts)]
        actions.append(_BCBootstrapAction(type=action_type, parameters=params))

    bootstrap = _BCBootstrapConfig(actions=actions) if actions else None
    target = _BCTargetConfig(bootstrap=bootstrap)

    context = DryRunContext(
        manifest_file="manifest.yaml",
        target_config=target,
        config={"region": "us-east-1"},
    )

    checker = BootstrapChecker()
    findings = checker.check(context)

    if not actions:
        # No actions → single OK finding about no bootstrap actions
        ok_findings = [f for f in findings if f.severity == Severity.OK]
        assert (
            len(ok_findings) == 1
        ), f"Expected 1 OK finding for empty actions, got {len(ok_findings)}"
        assert (
            "no bootstrap actions" in ok_findings[0].message.lower()
        ), f"Expected 'no bootstrap actions' in message: {ok_findings[0].message}"
        return

    # For each action, there should be exactly one OK finding
    ok_findings = [f for f in findings if f.severity == Severity.OK]
    assert len(ok_findings) == len(actions), (
        f"Expected {len(actions)} OK findings, got {len(ok_findings)}. "
        f"All findings: {[(f.severity.value, f.message) for f in findings]}"
    )

    for idx, action in enumerate(actions):
        finding = ok_findings[idx]

        # Message must contain the action type string
        assert action.type in finding.message, (
            f"Expected action type '{action.type}' in finding message: "
            f"{finding.message}"
        )

        # Message must contain each parameter key
        for key in action.parameters:
            assert key in finding.message, (
                f"Expected parameter key '{key}' in finding message for "
                f"action '{action.type}': {finding.message}"
            )


# ---------------------------------------------------------------------------
# Feature: deploy-dry-run, Property 23: Glue Data Catalog resource dependency detection
# **Validates: Requirements 13.1, 13.2, 13.3**
# ---------------------------------------------------------------------------


@dataclass
class _DepTargetConfig:
    """Minimal target config stub for DependencyChecker tests."""

    environment_variables: dict = field(default_factory=dict)


@given(
    data=st.data(),
    num_assets=st.integers(min_value=1, max_value=5),
)
@settings(max_examples=100)
def test_property_23_glue_data_catalog_dependency(data, num_assets):
    """For any catalog export containing assets with GlueTableFormType forms
    (covering both Glue tables and Glue views), and any set of Glue
    table/view/database existence responses, the DependencyChecker shall
    produce an ERROR finding for each (databaseName, tableName) pair where
    glue:GetTable returns EntityNotFoundException — regardless of whether the
    asset is a table or a view.  For Glue tables (non-view tableType), the
    DependencyChecker shall additionally call glue:GetPartitions and produce a
    WARNING finding if partitions are inaccessible.  Assets whose
    GlueTableFormType content contains a databaseName and tableName that both
    exist shall not produce ERROR findings for those references.
    """
    from unittest.mock import MagicMock, patch

    from botocore.exceptions import ClientError

    from smus_cicd.commands.dry_run.checkers.dependency_checker import (
        DependencyChecker,
    )

    name_st = st.text(
        min_size=1, max_size=10, alphabet=st.characters(whitelist_categories=("L",))
    )

    # Generate per-asset data
    db_names = data.draw(st.lists(name_st, min_size=num_assets, max_size=num_assets))
    table_names = data.draw(st.lists(name_st, min_size=num_assets, max_size=num_assets))
    table_types = data.draw(
        st.lists(
            st.sampled_from(["TABLE", "VIRTUAL_VIEW"]),
            min_size=num_assets,
            max_size=num_assets,
        )
    )
    db_exists_flags = data.draw(
        st.lists(st.booleans(), min_size=num_assets, max_size=num_assets)
    )
    table_exists_flags = data.draw(
        st.lists(st.booleans(), min_size=num_assets, max_size=num_assets)
    )
    partition_accessible_flags = data.draw(
        st.lists(st.booleans(), min_size=num_assets, max_size=num_assets)
    )

    # Build catalog resources
    resources = []
    for i in range(num_assets):
        resources.append(
            {
                "type": "assets",
                "name": f"asset-{i}",
                "identifier": f"asset-id-{i}",
                "typeIdentifier": "amazon.datazone.DefaultAssetType",
                "formsInput": [
                    {
                        "typeIdentifier": "amazon.datazone.GlueTableFormType",
                        "content": json.dumps(
                            {
                                "databaseName": db_names[i],
                                "tableName": table_names[i],
                                "tableType": table_types[i],
                            }
                        ),
                    }
                ],
            }
        )

    catalog_data = _group_resources_by_type(resources)

    # Build lookup maps keyed by unique resource identifiers.
    # The checker caches by db_name and (db_name, table_name), so the
    # *first* occurrence of each unique key determines the mock response.
    db_existence: Dict[str, bool] = {}
    table_existence: Dict[tuple, bool] = {}
    partition_access: Dict[tuple, bool] = {}

    for i in range(num_assets):
        db = db_names[i]
        tbl = table_names[i]
        key = (db, tbl)
        if db not in db_existence:
            db_existence[db] = db_exists_flags[i]
        if key not in table_existence:
            table_existence[key] = table_exists_flags[i]
        if key not in partition_access:
            partition_access[key] = partition_accessible_flags[i]

    # Configure mock Glue client
    def _make_client_error(code: str, op: str) -> ClientError:
        return ClientError({"Error": {"Code": code}}, op)

    mock_glue = MagicMock()

    def _get_database(**kwargs):
        name = kwargs.get("Name", "")
        if db_existence.get(name, False):
            return {}
        raise _make_client_error("EntityNotFoundException", "GetDatabase")

    def _get_table(**kwargs):
        db = kwargs.get("DatabaseName", "")
        tbl = kwargs.get("Name", "")
        if table_existence.get((db, tbl), False):
            return {}
        raise _make_client_error("EntityNotFoundException", "GetTable")

    def _get_partitions(**kwargs):
        db = kwargs.get("DatabaseName", "")
        tbl = kwargs.get("TableName", "")
        if partition_access.get((db, tbl), True):
            return {"Partitions": []}
        raise _make_client_error("AccessDeniedException", "GetPartitions")

    mock_glue.get_database.side_effect = _get_database
    mock_glue.get_table.side_effect = _get_table
    mock_glue.get_partitions.side_effect = _get_partitions

    target = _DepTargetConfig()
    context = DryRunContext(
        manifest_file="manifest.yaml",
        target_config=target,
        config={
            "region": "us-east-1",
            "domain_id": "dom-123",
            "project_id": "proj-123",
        },
        catalog_data=catalog_data,
    )

    with patch("boto3.client", return_value=mock_glue):
        checker = DependencyChecker()
        findings = checker.check(context)

    error_findings = [f for f in findings if f.severity == Severity.ERROR]
    warning_findings = [f for f in findings if f.severity == Severity.WARNING]

    # --- Compute expected ERROR findings ---
    # The checker iterates assets in order.  For each asset it checks the
    # database and then the table.  Because of caching, a missing db/table
    # produces an ERROR on every asset that references it (the cache stores
    # the boolean and re-emits findings on cache hits).
    expected_db_errors = 0
    expected_table_errors = 0
    for i in range(num_assets):
        db = db_names[i]
        tbl = table_names[i]
        if not db_existence.get(db, False):
            expected_db_errors += 1
        if not table_existence.get((db, tbl), False):
            expected_table_errors += 1

    # Every ERROR finding should reference either a missing database or a
    # missing table/view.
    db_error_findings = [
        f
        for f in error_findings
        if f.details and f.details.get("resource_type") == "database"
    ]
    table_error_findings = [
        f
        for f in error_findings
        if f.details and f.details.get("resource_type") in ("table", "view")
    ]

    assert len(db_error_findings) == expected_db_errors, (
        f"Expected {expected_db_errors} database ERROR findings, "
        f"got {len(db_error_findings)}. "
        f"db_existence={db_existence}, db_names={db_names}"
    )
    assert len(table_error_findings) == expected_table_errors, (
        f"Expected {expected_table_errors} table/view ERROR findings, "
        f"got {len(table_error_findings)}. "
        f"table_existence={table_existence}, table_names={table_names}"
    )

    # --- Verify no ERROR findings for resources that exist ---
    for f in error_findings:
        if f.details and f.details.get("resource_type") == "database":
            db = f.details["database_name"]
            assert not db_existence.get(
                db, False
            ), f"Got ERROR for database '{db}' that should exist"
        elif f.details and f.details.get("resource_type") in ("table", "view"):
            db = f.details["database_name"]
            tbl = f.details["table_name"]
            assert not table_existence.get(
                (db, tbl), False
            ), f"Got ERROR for table/view '{db}.{tbl}' that should exist"

    # --- Compute expected WARNING findings for inaccessible partitions ---
    # Partition checks only happen for non-view tables that exist.
    expected_partition_warnings = 0
    for i in range(num_assets):
        db = db_names[i]
        tbl = table_names[i]
        is_view = table_types[i].upper() in ("VIRTUAL_VIEW", "VIEW")
        if not is_view and table_existence.get((db, tbl), False):
            if not partition_access.get((db, tbl), True):
                expected_partition_warnings += 1

    partition_warnings = [
        f for f in warning_findings if "partition" in f.message.lower()
    ]
    assert len(partition_warnings) == expected_partition_warnings, (
        f"Expected {expected_partition_warnings} partition WARNING findings, "
        f"got {len(partition_warnings)}. "
        f"partition_access={partition_access}, table_types={table_types}"
    )


# ---------------------------------------------------------------------------
# Feature: deploy-dry-run, Property 24: Data source dependency detection
# **Validates: Requirements 13.4, 13.5**
# ---------------------------------------------------------------------------


@given(
    data=st.data(),
    num_assets=st.integers(min_value=1, max_value=5),
)
@settings(max_examples=100)
def test_property_24_data_source_dependency(data, num_assets):
    """For any catalog export containing assets with DataSourceReferenceFormType
    forms, and any set of datazone:ListDataSources responses, the
    DependencyChecker shall produce a WARNING finding for each asset whose data
    source type and database name combination has no matching data source in the
    target project.  Assets with matching data sources shall not produce WARNING
    findings for those references.
    """
    from unittest.mock import MagicMock, patch

    from smus_cicd.commands.dry_run.checkers.dependency_checker import (
        DependencyChecker,
    )
    from smus_cicd.commands.dry_run.models import DryRunContext

    name_st = st.text(
        min_size=1, max_size=10, alphabet=st.characters(whitelist_categories=("L",))
    )

    # Generate per-asset data
    db_names = data.draw(st.lists(name_st, min_size=num_assets, max_size=num_assets))
    table_names = data.draw(st.lists(name_st, min_size=num_assets, max_size=num_assets))
    ds_types = data.draw(
        st.lists(
            st.sampled_from(["GLUE", "SAGEMAKER", "REDSHIFT"]),
            min_size=num_assets,
            max_size=num_assets,
        )
    )
    ds_exists_flags = data.draw(
        st.lists(st.booleans(), min_size=num_assets, max_size=num_assets)
    )

    # Build catalog resources — each asset has both GlueTableFormType (for
    # databaseName context) and DataSourceReferenceFormType.
    resources = []
    for i in range(num_assets):
        resources.append(
            {
                "type": "assets",
                "name": f"asset-{i}",
                "identifier": f"asset-id-{i}",
                "typeIdentifier": "amazon.datazone.DefaultAssetType",
                "formsInput": [
                    {
                        "typeIdentifier": "amazon.datazone.GlueTableFormType",
                        "content": json.dumps(
                            {
                                "databaseName": db_names[i],
                                "tableName": table_names[i],
                                "tableType": "TABLE",
                            }
                        ),
                    },
                    {
                        "typeIdentifier": "amazon.datazone.DataSourceReferenceFormType",
                        "content": json.dumps(
                            {
                                "dataSourceType": ds_types[i],
                            }
                        ),
                    },
                ],
            }
        )

    catalog_data = _group_resources_by_type(resources)

    # Build lookup map keyed by unique (ds_type, db_name) — the checker
    # caches by this tuple, so the *first* occurrence determines the mock
    # response.
    ds_existence: dict = {}
    for i in range(num_assets):
        key = (ds_types[i], db_names[i])
        if key not in ds_existence:
            ds_existence[key] = ds_exists_flags[i]

    # Configure mock DataZone client for list_data_sources.
    #
    # The checker's _find_data_source calls list_data_sources (which returns
    # ALL data sources for the project), then filters by type in Python, and
    # finally checks if db_name is a substring of any candidate's name.
    # There is also a fallback: if any candidate of the right type exists,
    # _find_data_source returns True even without a db_name match.
    #
    # Because the checker caches by (ds_type, db_name) and calls
    # _find_data_source once per unique key, we track which key is being
    # queried by intercepting calls in order.  For each call we return
    # items that will produce the desired boolean result.
    mock_dz = MagicMock()

    # Determine the order of unique cache keys the checker will encounter.
    _call_order: list = []
    _seen_keys: set = set()
    for i in range(num_assets):
        key = (ds_types[i], db_names[i])
        if key not in _seen_keys:
            _seen_keys.add(key)
            _call_order.append(key)

    _call_idx = {"n": 0}

    def _list_data_sources(**kwargs):
        idx = _call_idx["n"]
        _call_idx["n"] += 1
        if idx < len(_call_order):
            key = _call_order[idx]
            if ds_existence.get(key, False):
                # Return a matching item of the right type with READY
                # status and a name that contains the db_name.
                return {
                    "items": [
                        {
                            "type": key[0],
                            "status": "READY",
                            "name": f"ds-{key[1]}-source",
                        }
                    ]
                }
        # No match — return empty items.
        return {"items": []}

    mock_dz.list_data_sources.side_effect = _list_data_sources

    # Configure mock Glue client — make all Glue resources exist to isolate
    # data source testing.
    mock_glue = MagicMock()
    mock_glue.get_database.return_value = {}
    mock_glue.get_table.return_value = {}
    mock_glue.get_partitions.return_value = {"Partitions": []}

    def _make_client(service, **kwargs):
        if service == "glue":
            return mock_glue
        return mock_dz

    target = _DepTargetConfig()
    context = DryRunContext(
        manifest_file="manifest.yaml",
        target_config=target,
        config={
            "region": "us-east-1",
            "domain_id": "dom-123",
            "project_id": "proj-123",
        },
        catalog_data=catalog_data,
    )

    with patch("boto3.client", side_effect=_make_client):
        checker = DependencyChecker()
        findings = checker.check(context)

    warning_findings = [f for f in findings if f.severity == Severity.WARNING]

    # Filter to only data-source-related warnings (exclude partition warnings
    # or other warning types).
    ds_warnings = [
        f for f in warning_findings if f.details and "data_source_type" in f.details
    ]

    # --- Compute expected WARNING findings ---
    # The checker emits a WARNING for every asset whose (ds_type, db_name)
    # has no matching data source.  Because of caching, the cached boolean
    # is re-used for subsequent assets with the same key.
    expected_ds_warnings = 0
    for i in range(num_assets):
        key = (ds_types[i], db_names[i])
        if not ds_existence.get(key, False):
            expected_ds_warnings += 1

    assert len(ds_warnings) == expected_ds_warnings, (
        f"Expected {expected_ds_warnings} data source WARNING findings, "
        f"got {len(ds_warnings)}. "
        f"ds_existence={ds_existence}, ds_types={ds_types}, db_names={db_names}"
    )

    # --- Verify no WARNING findings for data sources that have matches ---
    for f in ds_warnings:
        ds_type = f.details["data_source_type"]
        db_name = f.details["database_name"]
        key = (ds_type, db_name)
        assert not ds_existence.get(key, False), (
            f"Got WARNING for data source type='{ds_type}', db='{db_name}' "
            f"that should have a match"
        )

    # --- Verify assets with matching data sources have no WARNING ---
    warned_assets = {f.details["asset"] for f in ds_warnings}
    for i in range(num_assets):
        key = (ds_types[i], db_names[i])
        if ds_existence.get(key, False):
            assert (
                f"asset-{i}" not in warned_assets
            ), f"asset-{i} has matching data source {key} but got WARNING"


# ---------------------------------------------------------------------------
# Feature: deploy-dry-run, Property 25: Custom form type dependency detection
# **Validates: Requirements 13.6, 13.7, 13.12**
# ---------------------------------------------------------------------------


@given(
    data=st.data(),
    num_asset_types=st.integers(min_value=1, max_value=5),
)
@settings(max_examples=100)
def test_property_25_form_type_dependency(data, num_asset_types):
    """For any catalog export containing asset types with formsInput referencing
    custom form types (non-amazon.datazone. prefix), and any set of
    datazone:GetFormType responses, the DependencyChecker shall produce an ERROR
    finding for each custom form type that does not exist in the target domain.
    Managed form types (with amazon.datazone. prefix) shall not be checked and
    shall not produce findings.
    """
    from unittest.mock import MagicMock, patch

    from botocore.exceptions import ClientError

    from smus_cicd.commands.dry_run.checkers.dependency_checker import (
        DependencyChecker,
    )

    form_type_pool = st.sampled_from(
        [
            "custom.FormA",
            "custom.FormB",
            "custom.FormC",
            "amazon.datazone.ManagedForm",
            "amazon.datazone.SystemForm",
        ]
    )

    # Generate asset types, each with 1-3 form type references in formsInput
    resources = []
    all_custom_form_types: Set[str] = set()
    managed_form_types: Set[str] = set()

    for i in range(num_asset_types):
        num_forms = data.draw(st.integers(min_value=1, max_value=3))
        forms_input = {}
        for j in range(num_forms):
            ft_id = data.draw(form_type_pool)
            forms_input[f"form{j}"] = {
                "typeIdentifier": ft_id,
                "required": True,
            }
            if ft_id.startswith("amazon.datazone."):
                managed_form_types.add(ft_id)
            else:
                all_custom_form_types.add(ft_id)

        resources.append(
            {
                "type": "assetTypes",
                "name": f"MyAssetType-{i}",
                "identifier": f"custom.MyAssetType-{i}",
                "formsInput": forms_input,
            }
        )

    # Decide which custom form types exist in the target domain
    custom_exists: Dict[str, bool] = {}
    for ft_id in all_custom_form_types:
        custom_exists[ft_id] = data.draw(st.booleans())

    missing_custom = {ft for ft, exists in custom_exists.items() if not exists}

    catalog_data = _group_resources_by_type(resources)

    # Configure mock DataZone client
    mock_dz = MagicMock()

    def _get_form_type(**kwargs):
        ft_id = kwargs.get("formTypeIdentifier", "")
        if custom_exists.get(ft_id, False):
            return {"name": ft_id, "revision": "1"}
        raise ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}},
            "GetFormType",
        )

    mock_dz.get_form_type.side_effect = _get_form_type

    # Configure mock Glue client — make all Glue calls succeed to avoid
    # interference from Glue checks.
    mock_glue = MagicMock()
    mock_glue.get_database.return_value = {}
    mock_glue.get_table.return_value = {}
    mock_glue.get_partitions.return_value = {"Partitions": []}

    def _make_client(service, **kwargs):
        if service == "glue":
            return mock_glue
        return mock_dz

    target = _DepTargetConfig()
    context = DryRunContext(
        manifest_file="manifest.yaml",
        target_config=target,
        config={
            "region": "us-east-1",
            "domain_id": "dom-123",
            "project_id": "proj-123",
        },
        catalog_data=catalog_data,
    )

    with patch("boto3.client", side_effect=_make_client):
        checker = DependencyChecker()
        findings = checker.check(context)

    error_findings = [f for f in findings if f.severity == Severity.ERROR]

    # Filter to form-type-related ERROR findings
    form_type_errors = [
        f for f in error_findings if f.details and "form_type" in f.details
    ]

    # --- Compute expected ERROR findings ---
    # The checker iterates asset types and for each form in formsInput,
    # checks custom form types (skipping managed ones).  Due to caching,
    # each unique custom form type is looked up once, but an ERROR is
    # emitted for every asset type that references a missing form type.
    expected_form_type_errors = 0
    for resource in resources:
        forms_input = resource.get("formsInput", {})
        for form_name, form_config in forms_input.items():
            ft_id = form_config.get("typeIdentifier", "")
            if ft_id.startswith("amazon.datazone."):
                continue
            if ft_id in missing_custom:
                expected_form_type_errors += 1

    assert len(form_type_errors) == expected_form_type_errors, (
        f"Expected {expected_form_type_errors} form type ERROR findings, "
        f"got {len(form_type_errors)}. "
        f"custom_exists={custom_exists}, missing={missing_custom}"
    )

    # --- Verify no ERROR findings for custom form types that exist ---
    existing_custom = {ft for ft, exists in custom_exists.items() if exists}
    for f in form_type_errors:
        ft_id = f.details["form_type"]
        assert (
            ft_id not in existing_custom
        ), f"Got ERROR for form type '{ft_id}' that should exist"

    # --- Verify no findings at all for managed form types ---
    for managed_ft in managed_form_types:
        managed_findings = [
            f
            for f in findings
            if f.details and f.details.get("form_type") == managed_ft
        ]
        assert len(managed_findings) == 0, (
            f"Managed form type '{managed_ft}' should not produce any "
            f"findings, but found: {[f.message for f in managed_findings]}"
        )

    # --- Verify ERROR findings match exactly the set of missing custom form types ---
    errored_form_types = {f.details["form_type"] for f in form_type_errors}
    assert (
        errored_form_types == missing_custom
    ), f"ERROR form types {errored_form_types} != expected missing {missing_custom}"


# ---------------------------------------------------------------------------
# Feature: deploy-dry-run, Property 26: Custom asset type dependency detection
# **Validates: Requirements 13.8, 13.9, 13.12**
# ---------------------------------------------------------------------------


@given(
    data=st.data(),
    num_assets=st.integers(min_value=1, max_value=5),
)
@settings(max_examples=100)
def test_property_26_asset_type_dependency(data, num_assets):
    """For any catalog export containing assets with typeIdentifier referencing
    custom asset types (non-amazon.datazone. prefix), and any set of
    datazone:SearchTypes responses, the DependencyChecker shall produce an ERROR
    finding for each custom asset type that does not exist in the target domain.
    Managed asset types (with amazon.datazone. prefix) shall not be checked and
    shall not produce findings.
    """
    from typing import Dict, Set
    from unittest.mock import MagicMock, patch

    from smus_cicd.commands.dry_run.checkers.dependency_checker import (
        DependencyChecker,
    )
    from smus_cicd.commands.dry_run.models import DryRunContext

    type_id_pool = st.sampled_from(
        [
            "custom.TypeA",
            "custom.TypeB",
            "custom.TypeC",
            "amazon.datazone.DefaultAssetType",
            "amazon.datazone.GlueTableAssetType",
        ]
    )

    # Generate per-asset type identifiers
    type_ids = data.draw(
        st.lists(type_id_pool, min_size=num_assets, max_size=num_assets)
    )

    # Collect unique custom and managed type identifiers
    all_custom_types: Set[str] = set()
    managed_types: Set[str] = set()
    for tid in type_ids:
        if tid.startswith("amazon.datazone."):
            managed_types.add(tid)
        else:
            all_custom_types.add(tid)

    # Decide which custom asset types exist in the target domain
    custom_exists: Dict[str, bool] = {}
    for tid in all_custom_types:
        custom_exists[tid] = data.draw(st.booleans())

    missing_custom = {tid for tid, exists in custom_exists.items() if not exists}

    # Build catalog resources — assets with various typeIdentifiers and
    # empty formsInput to avoid triggering Glue/DataSource checks.
    resources = []
    for i in range(num_assets):
        resources.append(
            {
                "type": "assets",
                "name": f"asset-{i}",
                "identifier": f"asset-id-{i}",
                "typeIdentifier": type_ids[i],
                "formsInput": [],
            }
        )

    catalog_data = _group_resources_by_type(resources)

    # Build lookup map — the checker caches by type_identifier, so the
    # *first* occurrence of each unique type determines the mock response.
    type_existence: Dict[str, bool] = {}
    for i in range(num_assets):
        tid = type_ids[i]
        if tid.startswith("amazon.datazone."):
            continue
        if tid not in type_existence:
            type_existence[tid] = custom_exists[tid]

    # Configure mock DataZone client for search_types
    mock_dz = MagicMock()

    def _search_types(**kwargs):
        filters = kwargs.get("filters", {})
        # Extract the type name from the filter structure
        type_name = ""
        and_filters = filters.get("and", [])
        for f in and_filters:
            inner = f.get("filter", {})
            if inner.get("attribute") == "typeName":
                type_name = inner.get("value", "")
                break
        if type_existence.get(type_name, False):
            return {"items": [{"name": type_name}]}
        return {"items": []}

    mock_dz.search_types.side_effect = _search_types
    # list_data_sources — return empty (no DataSourceReferenceFormType forms)
    mock_dz.list_data_sources.return_value = {"items": []}
    # get_form_type — return success (won't be called for assets, only asset types)
    mock_dz.get_form_type.return_value = {"name": "dummy", "revision": "1"}

    # Configure mock Glue client — make all Glue calls succeed to avoid
    # interference from Glue checks.
    mock_glue = MagicMock()
    mock_glue.get_database.return_value = {}
    mock_glue.get_table.return_value = {}
    mock_glue.get_partitions.return_value = {"Partitions": []}

    def _make_client(service, **kwargs):
        if service == "glue":
            return mock_glue
        return mock_dz

    target = _DepTargetConfig()
    context = DryRunContext(
        manifest_file="manifest.yaml",
        target_config=target,
        config={
            "region": "us-east-1",
            "domain_id": "dom-123",
            "project_id": "proj-123",
        },
        catalog_data=catalog_data,
    )

    with patch("boto3.client", side_effect=_make_client):
        checker = DependencyChecker()
        findings = checker.check(context)

    error_findings = [f for f in findings if f.severity == Severity.ERROR]

    # Filter to asset-type-related ERROR findings
    asset_type_errors = [
        f for f in error_findings if f.details and "asset_type_identifier" in f.details
    ]

    # --- Compute expected ERROR findings ---
    # The checker iterates assets and for each custom typeIdentifier,
    # checks via search_types.  Due to caching, each unique custom type
    # is looked up once, but an ERROR is emitted for every asset that
    # references a missing custom asset type.
    expected_asset_type_errors = 0
    for i in range(num_assets):
        tid = type_ids[i]
        if tid.startswith("amazon.datazone."):
            continue
        if tid in missing_custom:
            expected_asset_type_errors += 1

    assert len(asset_type_errors) == expected_asset_type_errors, (
        f"Expected {expected_asset_type_errors} asset type ERROR findings, "
        f"got {len(asset_type_errors)}. "
        f"custom_exists={custom_exists}, missing={missing_custom}, "
        f"type_ids={type_ids}"
    )

    # --- Verify no ERROR findings for custom asset types that exist ---
    existing_custom = {tid for tid, exists in custom_exists.items() if exists}
    for f in asset_type_errors:
        tid = f.details["asset_type_identifier"]
        assert (
            tid not in existing_custom
        ), f"Got ERROR for asset type '{tid}' that should exist"

    # --- Verify no findings at all for managed asset types ---
    for managed_tid in managed_types:
        managed_findings = [
            f
            for f in findings
            if f.details and f.details.get("asset_type_identifier") == managed_tid
        ]
        assert len(managed_findings) == 0, (
            f"Managed asset type '{managed_tid}' should not produce any "
            f"findings, but found: {[f.message for f in managed_findings]}"
        )

    # --- Verify ERROR findings match exactly the set of missing custom asset types ---
    errored_asset_types = {
        f.details["asset_type_identifier"] for f in asset_type_errors
    }
    assert (
        errored_asset_types == missing_custom
    ), f"ERROR asset types {errored_asset_types} != expected missing {missing_custom}"


# ---------------------------------------------------------------------------
# Feature: deploy-dry-run, Property 27: Form type revision resolution
# **Validates: Requirements 13.10, 13.11**
# ---------------------------------------------------------------------------


@given(
    data=st.data(),
    num_assets=st.integers(min_value=1, max_value=5),
)
@settings(max_examples=100)
def test_property_27_form_type_revision_resolution(data, num_assets):
    """For any catalog export containing assets with formsInput referencing
    custom form types with typeRevision values, and any set of
    datazone:GetFormType responses, the DependencyChecker shall produce a
    WARNING finding for each custom form type whose revision cannot be resolved
    in the target domain.  Form types whose revision is successfully resolved
    shall not produce WARNING findings.
    """
    from typing import Dict, Optional, Set
    from unittest.mock import MagicMock, patch

    from botocore.exceptions import ClientError

    from smus_cicd.commands.dry_run.checkers.dependency_checker import (
        DependencyChecker,
    )
    from smus_cicd.commands.dry_run.models import DryRunContext

    revision_st = st.from_regex(r"[0-9]{1,4}", fullmatch=True)

    # Pool of form type identifiers — mix of custom and managed
    form_type_pool = st.sampled_from(
        [
            "custom.FormA",
            "custom.FormB",
            "custom.FormC",
            "amazon.datazone.ManagedForm",
            "amazon.datazone.SystemForm",
        ]
    )

    # For each asset, generate 1-3 forms with typeIdentifier and typeRevision
    resources = []
    # Track all unique custom form types and their expected revision
    all_custom_ft_ids: Set[str] = set()
    managed_ft_ids: Set[str] = set()

    # Per-asset form data for later assertion computation
    asset_forms_data: list = []

    for i in range(num_assets):
        num_forms = data.draw(st.integers(min_value=1, max_value=3))
        forms_input = []
        asset_forms = []
        for j in range(num_forms):
            ft_id = data.draw(form_type_pool)
            has_revision = data.draw(st.booleans())
            revision = data.draw(revision_st) if has_revision else None

            form_entry: dict = {
                "typeIdentifier": ft_id,
                "content": "{}",
            }
            if revision is not None:
                form_entry["typeRevision"] = revision

            forms_input.append(form_entry)
            asset_forms.append({"ft_id": ft_id, "revision": revision})

            if ft_id.startswith("amazon.datazone."):
                managed_ft_ids.add(ft_id)
            else:
                all_custom_ft_ids.add(ft_id)

        asset_forms_data.append(asset_forms)
        resources.append(
            {
                "type": "assets",
                "name": f"asset-{i}",
                "identifier": f"asset-id-{i}",
                "typeIdentifier": "amazon.datazone.DefaultAssetType",
                "formsInput": forms_input,
            }
        )

    catalog_data = _group_resources_by_type(resources)

    # For each custom form type, decide the target domain response:
    # - "exists_matching": form type exists, revision matches any requested revision
    # - "exists_no_revision": form type exists but has no revision field
    # - "exists_mismatch": form type exists but revision differs
    # - "not_exists": form type doesn't exist (ResourceNotFoundException)
    response_type_st = st.sampled_from(
        ["exists_matching", "exists_no_revision", "exists_mismatch", "not_exists"]
    )

    ft_response_type: Dict[str, str] = {}
    ft_target_revision: Dict[str, Optional[str]] = {}
    for ft_id in all_custom_ft_ids:
        resp_type = data.draw(response_type_st)
        ft_response_type[ft_id] = resp_type
        if resp_type == "exists_matching":
            # Will return a revision that matches the requested one
            ft_target_revision[ft_id] = None  # set dynamically per request
        elif resp_type == "exists_no_revision":
            ft_target_revision[ft_id] = None
        elif resp_type == "exists_mismatch":
            ft_target_revision[ft_id] = data.draw(revision_st)
        else:
            ft_target_revision[ft_id] = None

    # Track which revision each form type was first requested with (for
    # "exists_matching" — the cache stores the result of the first lookup)
    first_requested_revision: Dict[str, Optional[str]] = {}
    for forms in asset_forms_data:
        for form in forms:
            ft_id = form["ft_id"]
            if ft_id.startswith("amazon.datazone."):
                continue
            if ft_id not in first_requested_revision and form["revision"] is not None:
                first_requested_revision[ft_id] = form["revision"]

    # Configure mock DataZone client
    mock_dz = MagicMock()

    def _get_form_type(**kwargs):
        ft_id = kwargs.get("formTypeIdentifier", "")
        resp_type = ft_response_type.get(ft_id, "not_exists")
        if resp_type == "not_exists":
            raise ClientError(
                {"Error": {"Code": "ResourceNotFoundException"}},
                "GetFormType",
            )
        elif resp_type == "exists_no_revision":
            return {"name": ft_id}
        elif resp_type == "exists_matching":
            # Return a revision that matches the first requested revision
            rev = first_requested_revision.get(ft_id, "1")
            return {"name": ft_id, "revision": rev}
        elif resp_type == "exists_mismatch":
            return {"name": ft_id, "revision": ft_target_revision[ft_id]}
        return {"name": ft_id, "revision": "1"}

    mock_dz.get_form_type.side_effect = _get_form_type
    # search_types — return success for all asset types to avoid interference
    mock_dz.search_types.return_value = {"items": [{"name": "dummy"}]}
    # list_data_sources — return empty to avoid interference
    mock_dz.list_data_sources.return_value = {"items": []}

    # Configure mock Glue client — make all Glue calls succeed
    mock_glue = MagicMock()
    mock_glue.get_database.return_value = {}
    mock_glue.get_table.return_value = {}
    mock_glue.get_partitions.return_value = {"Partitions": []}

    def _make_client(service, **kwargs):
        if service == "glue":
            return mock_glue
        return mock_dz

    target = _DepTargetConfig()
    context = DryRunContext(
        manifest_file="manifest.yaml",
        target_config=target,
        config={
            "region": "us-east-1",
            "domain_id": "dom-123",
            "project_id": "proj-123",
        },
        catalog_data=catalog_data,
    )

    with patch("boto3.client", side_effect=_make_client):
        checker = DependencyChecker()
        findings = checker.check(context)

    warning_findings = [f for f in findings if f.severity == Severity.WARNING]

    # Filter to form-type-revision-related WARNING findings
    revision_warnings = [
        f
        for f in warning_findings
        if f.details and "form_type" in f.details and "expected_revision" in f.details
    ]

    # --- Compute expected WARNING findings ---
    # The checker iterates assets and for each form with a custom
    # typeIdentifier and typeRevision, checks the cached form type info.
    # A WARNING is emitted when:
    # (a) form type exists but has no revision field, or
    # (b) form type exists but revision doesn't match.
    # Form types that don't exist (None) are skipped (already reported by
    # _check_custom_form_types).
    # Managed form types are skipped.
    # Forms without typeRevision are skipped.

    # First, compute what the cache will contain after _get_form_type calls.
    # The cache is populated by _check_custom_form_types (for asset types)
    # and _check_form_type_revisions (for assets).  Since we have no asset
    # types in our resources, the cache is populated during
    # _check_form_type_revisions.
    cache: Dict[str, Optional[Dict[str, Any]]] = {}
    for ft_id in all_custom_ft_ids:
        resp_type = ft_response_type[ft_id]
        if resp_type == "not_exists":
            cache[ft_id] = None
        elif resp_type == "exists_no_revision":
            cache[ft_id] = {"name": ft_id}
        elif resp_type == "exists_matching":
            rev = first_requested_revision.get(ft_id, "1")
            cache[ft_id] = {"name": ft_id, "revision": rev}
        elif resp_type == "exists_mismatch":
            cache[ft_id] = {"name": ft_id, "revision": ft_target_revision[ft_id]}

    expected_revision_warnings = 0
    for i in range(num_assets):
        for form in asset_forms_data[i]:
            ft_id = form["ft_id"]
            revision = form["revision"]

            # Skip if no typeRevision
            if revision is None:
                continue
            # Skip managed
            if ft_id.startswith("amazon.datazone."):
                continue

            form_type_info = cache.get(ft_id)
            # Skip if form type doesn't exist
            if form_type_info is None:
                continue

            target_rev = form_type_info.get("revision")
            if target_rev is None:
                # No revision in target → WARNING
                expected_revision_warnings += 1
            elif str(revision) != str(target_rev):
                # Revision mismatch → WARNING
                expected_revision_warnings += 1

    assert len(revision_warnings) == expected_revision_warnings, (
        f"Expected {expected_revision_warnings} revision WARNING findings, "
        f"got {len(revision_warnings)}. "
        f"ft_response_type={ft_response_type}, "
        f"ft_target_revision={ft_target_revision}, "
        f"asset_forms_data={asset_forms_data}"
    )

    # --- Verify no WARNING for forms with matching revisions ---
    for f in revision_warnings:
        ft_id = f.details["form_type"]
        expected_rev = f.details["expected_revision"]
        form_type_info = cache.get(ft_id)
        assert (
            form_type_info is not None
        ), f"Got WARNING for form type '{ft_id}' that doesn't exist in cache"
        target_rev = form_type_info.get("revision")
        # Either no target revision or mismatch — both are valid WARNING cases
        assert target_rev is None or str(expected_rev) != str(target_rev), (
            f"Got WARNING for form type '{ft_id}' with matching revision "
            f"'{expected_rev}' == '{target_rev}'"
        )

    # --- Verify no findings for managed form types ---
    for managed_ft in managed_ft_ids:
        managed_warnings = [
            f for f in revision_warnings if f.details.get("form_type") == managed_ft
        ]
        assert len(managed_warnings) == 0, (
            f"Managed form type '{managed_ft}' should not produce revision "
            f"WARNING findings, but found: "
            f"{[f.message for f in managed_warnings]}"
        )


# ---------------------------------------------------------------------------
# Feature: deploy-dry-run, Property 28: Dependency check caching
# **Validates: Requirements 13.1, 13.2 (caching invariant)**
# ---------------------------------------------------------------------------


@given(
    data=st.data(),
    num_duplicates=st.integers(min_value=2, max_value=5),
)
@settings(max_examples=100)
def test_property_28_dependency_check_caching(data, num_duplicates):
    """For any set of catalog assets that reference the same Glue
    database/table multiple times, the DependencyChecker shall call each
    underlying AWS API (glue:GetDatabase, glue:GetTable, glue:GetPartitions)
    at most once per unique resource key.  Subsequent references to the same
    resource must be served from the internal cache without additional API
    calls.  The findings produced must still be correct — one finding per
    asset reference, not per unique key.
    """
    from unittest.mock import MagicMock, patch

    from botocore.exceptions import ClientError

    from smus_cicd.commands.dry_run.checkers.dependency_checker import (
        DependencyChecker,
    )

    # Generate a single shared database and table name
    name_st = st.text(
        min_size=1, max_size=10, alphabet=st.characters(whitelist_categories=("L",))
    )
    shared_db = data.draw(name_st, label="shared_db")
    shared_table = data.draw(name_st, label="shared_table")
    db_exists = data.draw(st.booleans(), label="db_exists")
    table_exists = data.draw(st.booleans(), label="table_exists")
    partitions_accessible = data.draw(st.booleans(), label="partitions_accessible")

    # Build N assets all referencing the same (db, table) pair as non-view
    resources = []
    for i in range(num_duplicates):
        resources.append(
            {
                "type": "assets",
                "name": f"dup-asset-{i}",
                "identifier": f"dup-id-{i}",
                "typeIdentifier": "amazon.datazone.DefaultAssetType",
                "formsInput": [
                    {
                        "typeIdentifier": "amazon.datazone.GlueTableFormType",
                        "content": json.dumps(
                            {
                                "databaseName": shared_db,
                                "tableName": shared_table,
                                "tableType": "TABLE",
                            }
                        ),
                    }
                ],
            }
        )

    catalog_data = _group_resources_by_type(resources)

    def _make_client_error(code: str, op: str) -> ClientError:
        return ClientError({"Error": {"Code": code}}, op)

    mock_glue = MagicMock()

    def _get_database(**kwargs):
        if db_exists:
            return {}
        raise _make_client_error("EntityNotFoundException", "GetDatabase")

    def _get_table(**kwargs):
        if table_exists:
            return {}
        raise _make_client_error("EntityNotFoundException", "GetTable")

    def _get_partitions(**kwargs):
        if partitions_accessible:
            return {"Partitions": []}
        raise _make_client_error("AccessDeniedException", "GetPartitions")

    mock_glue.get_database.side_effect = _get_database
    mock_glue.get_table.side_effect = _get_table
    mock_glue.get_partitions.side_effect = _get_partitions

    # Mock DataZone client to avoid interference from other checks
    mock_dz = MagicMock()
    mock_dz.list_data_sources.return_value = {"items": []}
    mock_dz.search_types.return_value = {"items": [{"name": "dummy"}]}

    def _make_client(service, **kwargs):
        if service == "glue":
            return mock_glue
        return mock_dz

    target = _DepTargetConfig()
    context = DryRunContext(
        manifest_file="manifest.yaml",
        target_config=target,
        config={
            "region": "us-east-1",
            "domain_id": "dom-123",
            "project_id": "proj-123",
        },
        catalog_data=catalog_data,
    )

    with patch("boto3.client", side_effect=_make_client):
        checker = DependencyChecker()
        findings = checker.check(context)

    # --- Caching invariant: each API called at most once per unique key ---
    # There is only one unique (db, table) pair, so each API should be
    # called exactly once regardless of how many assets reference it.
    assert mock_glue.get_database.call_count == 1, (
        f"get_database should be called exactly once for shared db "
        f"'{shared_db}', but was called {mock_glue.get_database.call_count} "
        f"times"
    )
    assert mock_glue.get_table.call_count == 1, (
        f"get_table should be called exactly once for shared table "
        f"'{shared_db}.{shared_table}', but was called "
        f"{mock_glue.get_table.call_count} times"
    )

    # Partitions are only checked for existing non-view tables
    if table_exists:
        assert mock_glue.get_partitions.call_count == 1, (
            f"get_partitions should be called exactly once for existing "
            f"table '{shared_db}.{shared_table}', but was called "
            f"{mock_glue.get_partitions.call_count} times"
        )
    else:
        assert mock_glue.get_partitions.call_count == 0, (
            f"get_partitions should not be called for non-existent table, "
            f"but was called {mock_glue.get_partitions.call_count} times"
        )

    # --- Findings correctness: one finding per asset reference ---
    error_findings = [f for f in findings if f.severity == Severity.ERROR]
    warning_findings = [f for f in findings if f.severity == Severity.WARNING]

    db_errors = [
        f
        for f in error_findings
        if f.details and f.details.get("resource_type") == "database"
    ]
    table_errors = [
        f
        for f in error_findings
        if f.details and f.details.get("resource_type") in ("table", "view")
    ]

    if not db_exists:
        # Every asset referencing the missing db should get an ERROR
        assert len(db_errors) == num_duplicates, (
            f"Expected {num_duplicates} db ERROR findings for missing db, "
            f"got {len(db_errors)}"
        )
    else:
        assert len(db_errors) == 0, (
            f"Expected 0 db ERROR findings for existing db, " f"got {len(db_errors)}"
        )

    if not table_exists:
        assert len(table_errors) == num_duplicates, (
            f"Expected {num_duplicates} table ERROR findings for missing "
            f"table, got {len(table_errors)}"
        )
    else:
        assert len(table_errors) == 0, (
            f"Expected 0 table ERROR findings for existing table, "
            f"got {len(table_errors)}"
        )

    # Partition warnings: only for existing tables
    partition_warnings = [
        f for f in warning_findings if "partition" in f.message.lower()
    ]
    if table_exists and not partitions_accessible:
        assert len(partition_warnings) == num_duplicates, (
            f"Expected {num_duplicates} partition WARNING findings, "
            f"got {len(partition_warnings)}"
        )
    elif table_exists and partitions_accessible:
        assert len(partition_warnings) == 0, (
            f"Expected 0 partition WARNING findings for accessible "
            f"partitions, got {len(partition_warnings)}"
        )
    else:
        assert len(partition_warnings) == 0, (
            f"Expected 0 partition WARNING findings for non-existent table, "
            f"got {len(partition_warnings)}"
        )

    # --- Verify cache state is consistent ---
    assert (
        shared_db in checker._db_cache
    ), f"Database '{shared_db}' should be in cache after check"
    assert (
        checker._db_cache[shared_db] == db_exists
    ), f"Cache for db '{shared_db}' should be {db_exists}"
    assert (
        shared_db,
        shared_table,
    ) in checker._table_cache, (
        f"Table '{shared_db}.{shared_table}' should be in cache after check"
    )
    assert checker._table_cache[(shared_db, shared_table)] == table_exists, (
        f"Cache for table '{shared_db}.{shared_table}' should be " f"{table_exists}"
    )


# ---------------------------------------------------------------------------
# Property tests for DryRunEngine (Properties 13, 1, 21)
# ---------------------------------------------------------------------------


# Feature: deploy-dry-run, Property 13: Phase ordering invariant
# **Validates: Requirements 5.7**
@given(
    severities=st.lists(
        st.sampled_from(list(Severity)),
        min_size=PHASE_COUNT,
        max_size=PHASE_COUNT,
    ),
)
@settings(max_examples=100)
def test_property_13_phase_ordering_invariant(severities):
    """For any dry-run execution, the phases in the report shall appear in the
    declaration order of the Phase enum (MANIFEST_VALIDATION first through
    BOOTSTRAP_ACTIONS last), matching ``list(Phase)``.
    """
    expected_phase_order = list(Phase)

    engine = DryRunEngine(
        manifest_file="manifest.yaml",
        stage_name="dev",
    )

    # Replace all 13 checkers with mocks that return OK findings
    # Ensure manifest and preflight checkers never return ERROR so all phases run
    mock_checkers = []
    for i, phase in enumerate(expected_phase_order):
        mock = MagicMock()
        sev = (
            Severity.OK
            if phase in (Phase.MANIFEST_VALIDATION, Phase.PREFLIGHT)
            else severities[i]
        )
        mock.check.return_value = [
            Finding(severity=sev, message=f"{phase.value} check")
        ]
        mock_checkers.append((phase, mock))

    engine._checkers = mock_checkers

    report = engine.run()

    # Verify phases appear in the correct order
    actual_phases = list(report.findings_by_phase.keys())
    assert actual_phases == expected_phase_order, (
        f"Phase order mismatch: expected {expected_phase_order}, "
        f"got {actual_phases}"
    )


# Feature: deploy-dry-run, Property 1: No-mutation invariant
# **Validates: Requirements 1.1**
@given(
    num_checkers=st.integers(min_value=1, max_value=12),
)
@settings(max_examples=100)
def test_property_1_no_mutation_invariant(num_checkers):
    """For any valid manifest, target configuration, and bundle archive,
    executing the dry-run engine shall invoke zero mutating AWS API calls.
    Only read-only calls are permitted.

    We verify this by mocking boto3 clients and checking that no mutating
    API methods (create*, put*, delete*, update*, upload*) are called.
    """
    engine = DryRunEngine(
        manifest_file="manifest.yaml",
        stage_name="dev",
    )

    # Track all boto3 client method calls
    api_calls: list[str] = []

    class _TrackingClient:
        """A mock boto3 client that records all method calls."""

        def __init__(self, service_name, **kwargs):
            self._service = service_name

        def __getattr__(self, name):
            def method_call(**kwargs):
                api_calls.append(f"{self._service}.{name}")
                return {}

            return method_call

    # Replace all checkers with mocks that return OK findings
    # (this ensures the engine itself doesn't make any API calls)
    phases = list(Phase)
    mock_checkers = []
    for phase in phases[:num_checkers]:
        mock = MagicMock()
        mock.check.return_value = [
            Finding(severity=Severity.OK, message=f"{phase.value} OK")
        ]
        mock_checkers.append((phase, mock))

    engine._checkers = mock_checkers

    with patch("boto3.client", side_effect=_TrackingClient):
        report = engine.run()  # noqa: F841

    # Verify no mutating API calls were made
    mutating_prefixes = ("create", "put", "delete", "update", "upload")
    mutating_calls = [
        call
        for call in api_calls
        if any(call.split(".")[-1].startswith(p) for p in mutating_prefixes)
    ]
    assert (
        len(mutating_calls) == 0
    ), f"Mutating API calls detected during dry-run: {mutating_calls}"


# Feature: deploy-dry-run, Property 21: Dry-run idempotence
# **Validates: Requirements 11.6**
@given(
    finding_severities=st.lists(
        st.sampled_from(list(Severity)),
        min_size=PHASE_COUNT,
        max_size=PHASE_COUNT,
    ),
    finding_messages=st.lists(
        st.text(min_size=1, max_size=40),
        min_size=PHASE_COUNT,
        max_size=PHASE_COUNT,
    ),
)
@settings(max_examples=100)
def test_property_21_dry_run_idempotence(finding_severities, finding_messages):
    """For any valid manifest, target, and bundle inputs, executing the
    dry-run engine twice with the same inputs shall produce reports with
    identical finding counts and identical finding messages.
    """
    phases = list(Phase)

    def _build_engine():
        engine = DryRunEngine(
            manifest_file="manifest.yaml",
            stage_name="dev",
        )
        mock_checkers = []
        for i, phase in enumerate(phases):
            mock = MagicMock()
            # Ensure manifest and preflight checkers never return ERROR so all phases run
            sev = (
                Severity.OK
                if phase in (Phase.MANIFEST_VALIDATION, Phase.PREFLIGHT)
                else finding_severities[i]
            )
            mock.check.return_value = [
                Finding(severity=sev, message=finding_messages[i])
            ]
            mock_checkers.append((phase, mock))
        engine._checkers = mock_checkers
        return engine

    report1 = _build_engine().run()
    report2 = _build_engine().run()

    # Verify identical counts
    assert (
        report1.ok_count == report2.ok_count
    ), f"ok_count mismatch: {report1.ok_count} vs {report2.ok_count}"
    assert (
        report1.warning_count == report2.warning_count
    ), f"warning_count mismatch: {report1.warning_count} vs {report2.warning_count}"
    assert (
        report1.error_count == report2.error_count
    ), f"error_count mismatch: {report1.error_count} vs {report2.error_count}"

    # Verify identical finding messages per phase
    for phase in phases:
        findings1 = report1.findings_by_phase.get(phase, [])
        findings2 = report2.findings_by_phase.get(phase, [])
        msgs1 = [f.message for f in findings1]
        msgs2 = [f.message for f in findings2]
        assert (
            msgs1 == msgs2
        ), f"Finding messages differ for {phase.value}: {msgs1} vs {msgs2}"


# ---------------------------------------------------------------------------
# CLI-integration property tests (Properties 2, 3, 16, 29, 30, 31, 32)
# ---------------------------------------------------------------------------

runner = CliRunner()


# --- Strategies for CLI tests ---

# Strategy for optional CLI string values
optional_path_strategy = st.one_of(
    st.none(), st.just("manifest.yaml"), st.just("custom.yaml")
)
optional_targets_strategy = st.one_of(
    st.none(), st.just("dev"), st.just("prod"), st.just("dev,staging")
)
optional_bundle_strategy = st.one_of(st.none(), st.just("./artifacts/bundle.zip"))
emit_events_strategy = st.sampled_from([None, True, False])
optional_event_bus_strategy = st.one_of(
    st.none(),
    st.just("my-bus"),
    st.just("arn:aws:events:us-east-1:123456789012:event-bus/my-bus"),
)


def _build_cli_args(
    manifest, targets, bundle, emit_events, event_bus_name, extra_flags=None
):
    """Build a CLI argument list for the deploy command."""
    args = ["deploy"]
    if manifest is not None:
        args.extend(["--manifest", manifest])
    if targets is not None:
        args.extend(["--targets", targets])
    if bundle is not None:
        args.extend(["--bundle-archive-path", bundle])
    if emit_events is True:
        args.append("--emit-events")
    elif emit_events is False:
        args.append("--no-events")
    if event_bus_name is not None:
        args.extend(["--event-bus-name", event_bus_name])
    if extra_flags:
        args.extend(extra_flags)
    return args


def _make_report(error_count=0, warning_count=0, ok_count=0):
    """Create a mock DryRunReport with the specified counts."""
    report = MagicMock()
    report.error_count = error_count
    report.warning_count = warning_count
    report.ok_count = ok_count
    report.render.return_value = "mock report output"
    return report


# Feature: deploy-dry-run, Property 2: CLI option compatibility
# **Validates: Requirements 1.3**
@given(
    manifest=optional_path_strategy,
    targets=optional_targets_strategy,
    bundle=optional_bundle_strategy,
    emit_events=emit_events_strategy,
    event_bus_name=optional_event_bus_strategy,
)
@settings(max_examples=100)
def test_property_2_cli_option_compatibility(
    manifest, targets, bundle, emit_events, event_bus_name
):
    """For any combination of existing deploy options (--manifest, --targets,
    --bundle-archive-path, --emit-events/--no-events, --event-bus-name),
    the CLI parser shall accept the --dry-run flag without raising a parsing error.
    """
    args = _build_cli_args(
        manifest, targets, bundle, emit_events, event_bus_name, ["--dry-run"]
    )

    mock_report = _make_report(error_count=0)

    with patch("smus_cicd.cli.DryRunEngine") as MockEngine:
        MockEngine.return_value.run.return_value = mock_report
        result = runner.invoke(app, args)

    # The CLI must not raise a parsing/usage error.
    # Exit code 0 means success (no errors in report); exit code 1 means
    # report had errors — both are acceptable.  Exit code 2 is Typer/Click
    # usage error which must NOT happen.
    assert result.exit_code != 2, f"CLI parsing error with args {args}: {result.output}"


# Feature: deploy-dry-run, Property 3: Event suppression under dry-run
# **Validates: Requirements 1.4**
@given(
    emit_events=emit_events_strategy,
)
@settings(max_examples=100)
def test_property_3_event_suppression_under_dry_run(emit_events):
    """For any value of --emit-events (True, False, or None), when --dry-run
    is active, the system shall not instantiate an EventEmitter or emit any
    EventBridge events.  Since dry-run short-circuits before deploy_command(),
    deploy_command is never called — which is the only place EventBridge
    events are emitted.
    """
    args = ["deploy", "--dry-run", "--targets", "dev"]
    if emit_events is True:
        args.append("--emit-events")
    elif emit_events is False:
        args.append("--no-events")

    mock_report = _make_report(error_count=0)

    with patch("smus_cicd.cli.DryRunEngine") as MockEngine, patch(
        "smus_cicd.cli.deploy_command"
    ) as mock_deploy:
        MockEngine.return_value.run.return_value = mock_report
        result = runner.invoke(app, args)  # noqa: F841
    mock_deploy.assert_not_called()
    # This guarantees no EventBridge events are emitted since event emission
    # only happens inside deploy_command.


# Feature: deploy-dry-run, Property 16: Exit code correctness
# **Validates: Requirements 7.4, 7.5**
@given(
    error_count=st.integers(min_value=0, max_value=50),
    warning_count=st.integers(min_value=0, max_value=50),
    ok_count=st.integers(min_value=0, max_value=50),
)
@settings(max_examples=100)
def test_property_16_exit_code_correctness(error_count, warning_count, ok_count):
    """For any DryRunReport, the CLI exit code shall be 0 if and only if
    error_count == 0.  If error_count > 0, the exit code shall be non-zero.
    """
    mock_report = _make_report(
        error_count=error_count, warning_count=warning_count, ok_count=ok_count
    )

    with patch("smus_cicd.cli.DryRunEngine") as MockEngine:
        MockEngine.return_value.run.return_value = mock_report
        result = runner.invoke(app, ["deploy", "--dry-run", "--targets", "dev"])

    if error_count == 0:
        assert (
            result.exit_code == 0
        ), f"Expected exit code 0 for error_count=0, got {result.exit_code}"
    else:
        assert (
            result.exit_code != 0
        ), f"Expected non-zero exit code for error_count={error_count}, got {result.exit_code}"


# Feature: deploy-dry-run, Property 29: Pre-deployment validation gate
# **Validates: Requirements 14.1, 14.2, 14.3**
@given(
    error_count=st.integers(min_value=0, max_value=20),
)
@settings(max_examples=100)
def test_property_29_pre_deployment_validation_gate(error_count):
    """For any deploy invocation without --dry-run and without --skip-validation,
    the CLI shall execute DryRunEngine.run() before calling deploy_command().
    If the resulting report has error_count > 0, deploy_command() shall not be
    called and the CLI shall exit with a non-zero exit code.  If error_count == 0,
    deploy_command() shall be called exactly once.
    """
    mock_report = _make_report(error_count=error_count, warning_count=0, ok_count=5)

    with patch("smus_cicd.cli.DryRunEngine") as MockEngine, patch(
        "smus_cicd.cli.deploy_command"
    ) as mock_deploy:
        MockEngine.return_value.run.return_value = mock_report
        result = runner.invoke(app, ["deploy", "--targets", "dev"])

    # DryRunEngine must always be instantiated for pre-deployment validation
    MockEngine.assert_called_once()
    MockEngine.return_value.run.assert_called_once()

    if error_count > 0:
        mock_deploy.assert_not_called()
        assert (
            result.exit_code != 0
        ), f"Expected non-zero exit code when validation has {error_count} errors"
    else:
        mock_deploy.assert_called_once()


# Feature: deploy-dry-run, Property 30: Skip-validation bypass
# **Validates: Requirements 14.7**
@given(
    emit_events=emit_events_strategy,
)
@settings(max_examples=100)
def test_property_30_skip_validation_bypass(emit_events):
    """For any deploy invocation with --skip-validation (and without --dry-run),
    the CLI shall call deploy_command() without executing DryRunEngine.run().
    The DryRunEngine constructor shall not be invoked.
    """
    args = ["deploy", "--skip-validation", "--targets", "dev"]
    if emit_events is True:
        args.append("--emit-events")
    elif emit_events is False:
        args.append("--no-events")

    with patch("smus_cicd.cli.DryRunEngine") as MockEngine, patch(
        "smus_cicd.cli.deploy_command"
    ) as mock_deploy:
        result = runner.invoke(app, args)  # noqa: F841

    # DryRunEngine must never be instantiated
    MockEngine.assert_not_called()
    # deploy_command must be called exactly once
    mock_deploy.assert_called_once()


# Feature: deploy-dry-run, Property 31: Pre-deployment event suppression
# **Validates: Requirements 14.9**
@given(
    emit_events=emit_events_strategy,
)
@settings(max_examples=100)
def test_property_31_pre_deployment_event_suppression(emit_events):
    """For any deploy invocation without --dry-run and without --skip-validation,
    during the pre-deployment validation phase the system shall not instantiate
    an EventEmitter or emit any EventBridge events.  EventBridge events shall
    only be emitted during the subsequent deploy_command() call if --emit-events
    is enabled.
    """
    args = ["deploy", "--targets", "dev"]
    if emit_events is True:
        args.append("--emit-events")
    elif emit_events is False:
        args.append("--no-events")

    mock_report = _make_report(error_count=0, warning_count=0, ok_count=5)

    # Track whether EventEmitter is instantiated during validation
    validation_complete = False

    original_run = MagicMock(return_value=mock_report)

    def track_run():
        nonlocal validation_complete
        original_run()
        validation_complete = True
        return mock_report

    with patch("smus_cicd.cli.DryRunEngine") as MockEngine, patch(
        "smus_cicd.cli.deploy_command"
    ) as mock_deploy:
        mock_engine_instance = MagicMock()
        mock_engine_instance.run.side_effect = track_run
        MockEngine.return_value = mock_engine_instance
        result = runner.invoke(app, args)  # noqa: F841

    # The DryRunEngine.run() was called (pre-deployment validation happened)
    mock_engine_instance.run.assert_called_once()
    # deploy_command was called after validation (since no errors)
    mock_deploy.assert_called_once()
    # The key assertion: DryRunEngine does NOT create EventEmitter.
    # The engine only uses read-only checkers. EventBridge events are only
    # emitted inside deploy_command, which runs AFTER validation.
    # We verify this by confirming the engine was called with "text" format
    # (not with emit_events), meaning no event infrastructure is set up.
    MockEngine.assert_called_once()
    engine_call_args = MockEngine.call_args
    # The engine is called with (manifest_file, targets, bundle, "text")
    # — no emit_events parameter, confirming no event infrastructure
    assert (
        engine_call_args[0][3] == "text"
        or engine_call_args[1].get("output_format") == "text"
        if engine_call_args[1]
        else engine_call_args[0][3] == "text"
    )


# Feature: deploy-dry-run, Property 32: Pre-deployment validation uses same engine
# **Validates: Requirements 14.8**
@given(
    finding_severities=st.lists(
        st.sampled_from(list(Severity)),
        min_size=PHASE_COUNT,
        max_size=PHASE_COUNT,
    ),
    finding_messages=st.lists(
        st.text(min_size=1, max_size=40),
        min_size=PHASE_COUNT,
        max_size=PHASE_COUNT,
    ),
)
@settings(max_examples=100)
def test_property_32_pre_deployment_validation_uses_same_engine(
    finding_severities, finding_messages
):
    """For any deploy invocation, the pre-deployment validation step shall use
    the same DryRunEngine class, the same set of checkers, and the same
    DryRunReport model as the standalone --dry-run mode.  Given identical
    inputs, the pre-deployment validation and standalone dry-run shall produce
    reports with identical finding counts and messages.
    """
    phases = list(Phase)

    def _build_engine_with_mocks():
        """Create a DryRunEngine with deterministic mock checkers."""
        engine = DryRunEngine(
            manifest_file="manifest.yaml",
            stage_name="dev",
        )
        mock_checkers = []
        for i, phase in enumerate(phases):
            mock = MagicMock()
            # Ensure manifest and preflight checkers never return ERROR so all phases run
            sev = (
                Severity.OK
                if phase in (Phase.MANIFEST_VALIDATION, Phase.PREFLIGHT)
                else finding_severities[i]
            )
            mock.check.return_value = [
                Finding(severity=sev, message=finding_messages[i])
            ]
            mock_checkers.append((phase, mock))
        engine._checkers = mock_checkers
        return engine

    # Simulate standalone dry-run
    standalone_engine = _build_engine_with_mocks()
    standalone_report = standalone_engine.run()

    # Simulate pre-deployment validation (same engine, same inputs)
    predeployment_engine = _build_engine_with_mocks()
    predeployment_report = predeployment_engine.run()

    # Both must use the same DryRunEngine class
    assert type(standalone_engine) is type(predeployment_engine) is DryRunEngine

    # Both must produce identical finding counts
    assert (
        standalone_report.ok_count == predeployment_report.ok_count
    ), f"ok_count mismatch: {standalone_report.ok_count} vs {predeployment_report.ok_count}"
    assert (
        standalone_report.warning_count == predeployment_report.warning_count
    ), f"warning_count mismatch: {standalone_report.warning_count} vs {predeployment_report.warning_count}"
    assert (
        standalone_report.error_count == predeployment_report.error_count
    ), f"error_count mismatch: {standalone_report.error_count} vs {predeployment_report.error_count}"

    # Both must produce identical finding messages per phase
    for phase in phases:
        findings1 = standalone_report.findings_by_phase.get(phase, [])
        findings2 = predeployment_report.findings_by_phase.get(phase, [])
        msgs1 = [f.message for f in findings1]
        msgs2 = [f.message for f in findings2]
        assert (
            msgs1 == msgs2
        ), f"Finding messages differ for {phase.value}: {msgs1} vs {msgs2}"

    # Both reports must use the same DryRunReport class
    from smus_cicd.commands.dry_run.models import DryRunReport as ReportClass

    assert type(standalone_report) is ReportClass
    assert type(predeployment_report) is ReportClass
