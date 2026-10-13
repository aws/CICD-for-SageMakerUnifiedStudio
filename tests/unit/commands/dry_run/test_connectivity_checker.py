"""Unit tests for ConnectivityChecker.

Tests reachable/unreachable domain, project existence, S3 bucket accessibility,
and Airflow environment reachability.

Requirements: 10.6
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from smus_cicd.commands.dry_run.checkers.connectivity_checker import (
    ConnectivityChecker,
)
from smus_cicd.commands.dry_run.models import DryRunContext, Severity

# ---------------------------------------------------------------------------
# Stub dataclasses mirroring the real manifest models
# ---------------------------------------------------------------------------


@dataclass
class _StorageConfig:
    name: str
    connectionName: Optional[str] = "default.s3_shared"
    targetDirectory: str = ""


@dataclass
class _DeploymentConfiguration:
    storage: List[_StorageConfig] = field(default_factory=list)
    git: list = field(default_factory=list)


@dataclass
class _BootstrapAction:
    type: str
    parameters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class _BootstrapConfig:
    actions: List[_BootstrapAction] = field(default_factory=list)


@dataclass
class _ProjectConfig:
    name: str
    create: bool = False


@dataclass
class _TargetConfig:
    deployment_configuration: Optional[_DeploymentConfiguration] = None
    bootstrap: Optional[_BootstrapConfig] = None
    project: Optional[_ProjectConfig] = None


def _make_context(
    storage_names: list[str] | None = None,
    bootstrap_actions: list[str] | None = None,
    domain_id: str | None = "dzd_test",
    project_name: str | None = "test-project",
    project_create: bool = False,
    region: str = "us-east-1",
) -> DryRunContext:
    """Build a DryRunContext with the given features enabled.

    ``project_create`` populates ``target_config.project.create`` — the
    checker reads it to decide whether a missing project is a WARNING
    (will be created) or an ERROR (deployment would fail), matching
    ``ProjectChecker``'s own ``create`` handling.
    """
    storage = [_StorageConfig(name=n) for n in (storage_names or [])]
    deploy_cfg = _DeploymentConfiguration(storage=storage)

    bootstrap = None
    if bootstrap_actions:
        bootstrap = _BootstrapConfig(
            actions=[_BootstrapAction(type=t) for t in bootstrap_actions]
        )

    project_cfg = (
        _ProjectConfig(name=project_name, create=project_create)
        if project_name is not None
        else None
    )

    target = _TargetConfig(
        deployment_configuration=deploy_cfg,
        bootstrap=bootstrap,
        project=project_cfg,
    )

    config: Dict[str, Any] = {"region": region}
    if domain_id is not None:
        config["domain_id"] = domain_id
    if project_name is not None:
        config["project_name"] = project_name

    return DryRunContext(
        manifest_file="manifest.yaml",
        target_config=target,
        config=config,
    )


@pytest.fixture
def checker():
    return ConnectivityChecker()


# -----------------------------------------------------------------------
# Test: No target config → skip with WARNING
# -----------------------------------------------------------------------


# TestConnectivityCheckerNoConfig tests moved to test_preflight_checker.py

# -----------------------------------------------------------------------
# Test: DataZone domain reachability (Req 6.1)
# -----------------------------------------------------------------------


class TestDomainReachability:
    """Tests for DataZone domain reachability via GetDomain."""

    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    def test_domain_reachable(self, mock_create_client, checker):
        mock_dz = MagicMock()
        mock_dz.get_domain.return_value = {"id": "dzd_test", "name": "TestDomain"}
        mock_create_client.return_value = mock_dz

        context = _make_context(domain_id="dzd_test")
        findings = checker.check(context)

        domain_findings = [
            f for f in findings if f.resource == "dzd_test" and f.service == "datazone"
        ]
        ok_findings = [f for f in domain_findings if f.severity == Severity.OK]
        assert len(ok_findings) >= 1
        assert "reachable" in ok_findings[0].message.lower()

    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    def test_domain_unreachable_client_error(self, mock_create_client, checker):
        mock_dz = MagicMock()
        mock_dz.get_domain.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "Not found"}},
            "GetDomain",
        )
        mock_create_client.return_value = mock_dz

        context = _make_context(domain_id="dzd_missing")
        findings = checker.check(context)

        error_findings = [
            f
            for f in findings
            if f.severity == Severity.ERROR
            and f.resource == "dzd_missing"
            and f.service == "datazone"
        ]
        assert len(error_findings) >= 1
        assert "unreachable" in error_findings[0].message.lower()
        assert error_findings[0].details["error_code"] == "ResourceNotFoundException"

    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    def test_domain_unreachable_generic_error(self, mock_create_client, checker):
        mock_dz = MagicMock()
        mock_dz.get_domain.side_effect = Exception("Network timeout")
        mock_create_client.return_value = mock_dz

        context = _make_context(domain_id="dzd_timeout")
        findings = checker.check(context)

        error_findings = [
            f
            for f in findings
            if f.severity == Severity.ERROR and f.resource == "dzd_timeout"
        ]
        assert len(error_findings) >= 1
        assert "unreachable" in error_findings[0].message.lower()

    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    def test_no_domain_id_produces_warning(self, mock_create_client, checker):
        context = _make_context(domain_id=None)
        findings = checker.check(context)

        error_findings = [
            f
            for f in findings
            if f.severity == Severity.ERROR and f.service == "datazone"
        ]
        assert any("domain_id" in f.message.lower() for f in error_findings)


# -----------------------------------------------------------------------
# Test: DataZone project existence (Req 6.2)
# -----------------------------------------------------------------------


class TestProjectExistence:
    """Tests for DataZone project existence check."""

    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    @patch(
        "smus_cicd.commands.dry_run.checkers.connectivity_checker"
        ".ConnectivityChecker._check_project"
    )
    def test_project_exists(self, mock_check_project, mock_create_client, checker):
        """Use a direct mock to test project found path."""
        # We'll test _check_project directly instead
        pass

    @patch("smus_cicd.helpers.datazone.get_project_by_name")
    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    def test_project_found(self, mock_create_client, mock_get_project, checker):
        mock_dz = MagicMock()
        mock_dz.get_domain.return_value = {"id": "dzd_test"}
        mock_create_client.return_value = mock_dz

        mock_get_project.return_value = {"id": "proj-123", "name": "test-project"}

        context = _make_context(project_name="test-project", domain_id="dzd_test")
        findings = checker.check(context)

        project_ok = [
            f
            for f in findings
            if f.severity == Severity.OK
            and f.resource == "test-project"
            and f.service == "datazone"
        ]
        assert len(project_ok) >= 1
        assert "exists" in project_ok[0].message.lower()

    @patch("smus_cicd.helpers.datazone.get_project_by_name")
    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    def test_project_not_found_create_false_is_error(
        self, mock_create_client, mock_get_project, checker
    ):
        """create defaults to False (no project config at all here), so a
        missing project must be an ERROR — matching ProjectChecker's own
        create=False behavior for the identical fact, not a WARNING. See
        test_project_not_found_create_true_is_warning below for the other
        branch.
        """
        mock_dz = MagicMock()
        mock_dz.get_domain.return_value = {"id": "dzd_test"}
        mock_create_client.return_value = mock_dz

        mock_get_project.return_value = None

        context = _make_context(project_name="missing-project", domain_id="dzd_test")
        findings = checker.check(context)

        project_errors = [
            f
            for f in findings
            if f.severity == Severity.ERROR
            and f.resource == "missing-project"
            and f.service == "datazone"
        ]
        assert len(project_errors) >= 1
        assert "not found" in project_errors[0].message.lower()
        assert "create=false" in project_errors[0].message.lower()

        # Must NOT also report this as a WARNING — that was the bug: this
        # exact fact landing as both a severity in the same report.
        project_warnings = [
            f
            for f in findings
            if f.resource == "missing-project" and f.severity == Severity.WARNING
        ]
        assert len(project_warnings) == 0

    @patch("smus_cicd.helpers.datazone.get_project_by_name")
    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    def test_project_not_found_create_true_is_warning(
        self, mock_create_client, mock_get_project, checker
    ):
        """A project target_config with create=True set must downgrade a
        missing project to WARNING, matching ProjectChecker's OK-but-flagged
        treatment of the same case (ProjectChecker reports OK; this phase
        reports WARNING since the project genuinely does not exist yet).
        """
        mock_dz = MagicMock()
        mock_dz.get_domain.return_value = {"id": "dzd_test"}
        mock_create_client.return_value = mock_dz

        mock_get_project.return_value = None

        context = _make_context(
            project_name="missing-project",
            domain_id="dzd_test",
            project_create=True,
        )
        findings = checker.check(context)

        project_warnings = [
            f
            for f in findings
            if f.severity == Severity.WARNING
            and f.resource == "missing-project"
            and f.service == "datazone"
        ]
        assert len(project_warnings) >= 1
        assert "not found" in project_warnings[0].message.lower()
        assert "create=true" in project_warnings[0].message.lower()

        project_errors = [
            f
            for f in findings
            if f.resource == "missing-project" and f.severity == Severity.ERROR
        ]
        assert len(project_errors) == 0

    @patch("smus_cicd.helpers.datazone.get_project_by_name")
    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    def test_project_check_error(self, mock_create_client, mock_get_project, checker):
        mock_dz = MagicMock()
        mock_dz.get_domain.return_value = {"id": "dzd_test"}
        mock_create_client.return_value = mock_dz

        mock_get_project.side_effect = Exception("API error")

        context = _make_context(project_name="err-project", domain_id="dzd_test")
        findings = checker.check(context)

        project_errors = [
            f
            for f in findings
            if f.severity == Severity.ERROR and f.resource == "err-project"
        ]
        assert len(project_errors) >= 1
        assert "failed" in project_errors[0].message.lower()

    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    def test_no_project_name_produces_warning(self, mock_create_client, checker):
        mock_dz = MagicMock()
        mock_dz.get_domain.return_value = {"id": "dzd_test"}
        mock_create_client.return_value = mock_dz

        context = _make_context(project_name=None, domain_id="dzd_test")
        findings = checker.check(context)

        project_warnings = [
            f
            for f in findings
            if f.severity == Severity.ERROR and "project_name" in f.message.lower()
        ]
        assert len(project_warnings) >= 1


# -----------------------------------------------------------------------
# Test: S3 bucket accessibility (Req 6.3, 6.5)
# -----------------------------------------------------------------------


class TestS3BucketAccessibility:
    """Tests for S3 bucket accessibility via HeadBucket.

    The new code resolves connection names to real bucket names via
    ``_get_project_connections`` → DataZone.  We mock that method to
    return connection dicts with ``s3Uri`` so the resolver can extract
    the bucket name without hitting AWS.
    """

    @patch(
        "smus_cicd.commands.dry_run.checkers.connectivity_checker.get_project_connections"
    )
    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    def test_bucket_accessible(self, mock_create_client, mock_get_conns, checker):
        mock_dz = MagicMock()
        mock_dz.get_domain.return_value = {"id": "dzd_test"}
        mock_s3 = MagicMock()
        mock_s3.head_bucket.return_value = {}

        mock_create_client.side_effect = lambda svc, **kw: (
            mock_dz if svc == "datazone" else mock_s3
        )
        mock_get_conns.return_value = {
            "default.s3_shared": {"s3Uri": "s3://my-real-bucket/prefix"},
        }

        context = _make_context(storage_names=["my-data"])
        findings = checker.check(context)

        s3_ok = [f for f in findings if f.severity == Severity.OK and f.service == "s3"]
        assert len(s3_ok) >= 1
        assert "accessible" in s3_ok[0].message.lower()
        assert s3_ok[0].resource == "my-real-bucket"

    @patch(
        "smus_cicd.commands.dry_run.checkers.connectivity_checker.get_project_connections"
    )
    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    def test_bucket_not_accessible(self, mock_create_client, mock_get_conns, checker):
        mock_dz = MagicMock()
        mock_dz.get_domain.return_value = {"id": "dzd_test"}
        mock_s3 = MagicMock()
        mock_s3.head_bucket.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}},
            "HeadBucket",
        )

        mock_create_client.side_effect = lambda svc, **kw: (
            mock_dz if svc == "datazone" else mock_s3
        )
        mock_get_conns.return_value = {
            "default.s3_shared": {"s3Uri": "s3://missing-bucket"},
        }

        context = _make_context(storage_names=["missing-bucket"])
        findings = checker.check(context)

        s3_errors = [
            f for f in findings if f.severity == Severity.ERROR and f.service == "s3"
        ]
        assert len(s3_errors) >= 1
        assert "not accessible" in s3_errors[0].message.lower()
        assert s3_errors[0].details["error_code"] == "404"

    @patch(
        "smus_cicd.commands.dry_run.checkers.connectivity_checker.get_project_connections"
    )
    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    def test_bucket_generic_error(self, mock_create_client, mock_get_conns, checker):
        mock_dz = MagicMock()
        mock_dz.get_domain.return_value = {"id": "dzd_test"}
        mock_s3 = MagicMock()
        mock_s3.head_bucket.side_effect = Exception("Connection refused")

        mock_create_client.side_effect = lambda svc, **kw: (
            mock_dz if svc == "datazone" else mock_s3
        )
        mock_get_conns.return_value = {
            "default.s3_shared": {"s3Uri": "s3://err-bucket"},
        }

        context = _make_context(storage_names=["err-bucket"])
        findings = checker.check(context)

        s3_errors = [
            f for f in findings if f.severity == Severity.ERROR and f.service == "s3"
        ]
        assert len(s3_errors) >= 1
        assert "not accessible" in s3_errors[0].message.lower()

    @patch(
        "smus_cicd.commands.dry_run.checkers.connectivity_checker.get_project_connections"
    )
    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    def test_multiple_buckets_deduplicated(
        self, mock_create_client, mock_get_conns, checker
    ):
        """Two storage items with the same connection resolve to the same bucket."""
        mock_dz = MagicMock()
        mock_dz.get_domain.return_value = {"id": "dzd_test"}
        mock_s3 = MagicMock()
        mock_s3.head_bucket.return_value = {}

        mock_create_client.side_effect = lambda svc, **kw: (
            mock_dz if svc == "datazone" else mock_s3
        )
        # Both storage items have connectionName "default.s3_shared"
        mock_get_conns.return_value = {
            "default.s3_shared": {"s3Uri": "s3://shared-bucket/data"},
        }

        context = _make_context(storage_names=["data1", "data2"])
        findings = checker.check(context)

        s3_ok = [f for f in findings if f.severity == Severity.OK and f.service == "s3"]
        # Should only have one HeadBucket check since both map to same bucket
        assert len(s3_ok) == 1
        assert s3_ok[0].resource == "shared-bucket"

    @patch(
        "smus_cicd.commands.dry_run.checkers.connectivity_checker.get_project_connections"
    )
    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    def test_no_storage_items_no_s3_findings(
        self, mock_create_client, mock_get_conns, checker
    ):
        mock_dz = MagicMock()
        mock_dz.get_domain.return_value = {"id": "dzd_test"}
        mock_create_client.return_value = mock_dz

        context = _make_context(storage_names=[])
        findings = checker.check(context)

        s3_findings = [f for f in findings if f.service == "s3"]
        assert len(s3_findings) == 0

    @patch(
        "smus_cicd.commands.dry_run.checkers.connectivity_checker.get_project_connections"
    )
    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    def test_unresolved_connection_produces_warning(
        self, mock_create_client, mock_get_conns, checker
    ):
        """When _get_project_connections returns None, connections are unresolved."""
        mock_dz = MagicMock()
        mock_dz.get_domain.return_value = {"id": "dzd_test"}
        mock_create_client.return_value = mock_dz
        mock_get_conns.return_value = None

        context = _make_context(storage_names=["data"])
        findings = checker.check(context)

        s3_warnings = [
            f for f in findings if f.severity == Severity.WARNING and f.service == "s3"
        ]
        assert len(s3_warnings) >= 1
        assert "could not resolve" in s3_warnings[0].message.lower()
        assert s3_warnings[0].resource == "default.s3_shared"

    @patch(
        "smus_cicd.commands.dry_run.checkers.connectivity_checker.get_project_connections"
    )
    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    def test_connection_without_s3uri_is_unresolved(
        self, mock_create_client, mock_get_conns, checker
    ):
        """Connection exists but has no s3Uri → treated as unresolved."""
        mock_dz = MagicMock()
        mock_dz.get_domain.return_value = {"id": "dzd_test"}
        mock_create_client.return_value = mock_dz
        mock_get_conns.return_value = {
            "default.s3_shared": {"type": "S3"},  # no s3Uri
        }

        context = _make_context(storage_names=["data"])
        findings = checker.check(context)

        s3_warnings = [
            f for f in findings if f.severity == Severity.WARNING and f.service == "s3"
        ]
        assert len(s3_warnings) >= 1
        assert "could not resolve" in s3_warnings[0].message.lower()


# -----------------------------------------------------------------------
# Test: Airflow environment reachability (Req 6.4)
# -----------------------------------------------------------------------


class TestAirflowReachability:
    """Tests for Airflow environment reachability."""

    @patch("smus_cicd.helpers.airflow_serverless.list_workflows")
    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    def test_airflow_reachable(self, mock_create_client, mock_list_workflows, checker):
        mock_dz = MagicMock()
        mock_dz.get_domain.return_value = {"id": "dzd_test"}
        mock_create_client.return_value = mock_dz

        mock_list_workflows.return_value = []

        context = _make_context(bootstrap_actions=["workflow.create"])
        findings = checker.check(context)

        airflow_ok = [
            f
            for f in findings
            if f.severity == Severity.OK and f.service == "airflow-serverless"
        ]
        assert len(airflow_ok) >= 1
        assert "reachable" in airflow_ok[0].message.lower()

    @patch("smus_cicd.helpers.airflow_serverless.list_workflows")
    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    def test_airflow_unreachable(
        self, mock_create_client, mock_list_workflows, checker
    ):
        mock_dz = MagicMock()
        mock_dz.get_domain.return_value = {"id": "dzd_test"}
        mock_create_client.return_value = mock_dz

        mock_list_workflows.side_effect = Exception("Service unavailable")

        context = _make_context(bootstrap_actions=["workflow.run"])
        findings = checker.check(context)

        airflow_errors = [
            f
            for f in findings
            if f.severity == Severity.ERROR and f.service == "airflow-serverless"
        ]
        assert len(airflow_errors) >= 1
        assert "unreachable" in airflow_errors[0].message.lower()

    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    def test_no_workflow_actions_skips_airflow(self, mock_create_client, checker):
        mock_dz = MagicMock()
        mock_dz.get_domain.return_value = {"id": "dzd_test"}
        mock_create_client.return_value = mock_dz

        context = _make_context(bootstrap_actions=["quicksight.refresh_dataset"])
        findings = checker.check(context)

        airflow_findings = [f for f in findings if f.service == "airflow-serverless"]
        assert len(airflow_findings) == 0

    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    def test_no_bootstrap_skips_airflow(self, mock_create_client, checker):
        mock_dz = MagicMock()
        mock_dz.get_domain.return_value = {"id": "dzd_test"}
        mock_create_client.return_value = mock_dz

        context = _make_context(bootstrap_actions=None)
        findings = checker.check(context)

        airflow_findings = [f for f in findings if f.service == "airflow-serverless"]
        assert len(airflow_findings) == 0

    @patch("smus_cicd.helpers.airflow_serverless.list_workflows")
    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    def test_workflow_monitor_triggers_airflow_check(
        self, mock_create_client, mock_list_workflows, checker
    ):
        mock_dz = MagicMock()
        mock_dz.get_domain.return_value = {"id": "dzd_test"}
        mock_create_client.return_value = mock_dz

        mock_list_workflows.return_value = []

        context = _make_context(bootstrap_actions=["workflow.monitor"])
        findings = checker.check(context)

        airflow_ok = [
            f
            for f in findings
            if f.severity == Severity.OK and f.service == "airflow-serverless"
        ]
        assert len(airflow_ok) >= 1


# -----------------------------------------------------------------------
# Test: Error reporting includes resource identifier and service (Req 6.5)
# -----------------------------------------------------------------------


class TestErrorReporting:
    """Tests that error findings include resource identifier and service."""

    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    def test_domain_error_includes_resource_and_service(
        self, mock_create_client, checker
    ):
        mock_dz = MagicMock()
        mock_dz.get_domain.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "Denied"}},
            "GetDomain",
        )
        mock_create_client.return_value = mock_dz

        context = _make_context(domain_id="dzd_denied")
        findings = checker.check(context)

        error_findings = [
            f
            for f in findings
            if f.severity == Severity.ERROR and f.resource == "dzd_denied"
        ]
        assert len(error_findings) >= 1
        assert error_findings[0].service == "datazone"

    @patch(
        "smus_cicd.commands.dry_run.checkers.connectivity_checker.get_project_connections"
    )
    @patch("smus_cicd.commands.dry_run.checkers.connectivity_checker.create_client")
    def test_s3_error_includes_bucket_name_and_service(
        self, mock_create_client, mock_get_conns, checker
    ):
        mock_dz = MagicMock()
        mock_dz.get_domain.return_value = {"id": "dzd_test"}
        mock_s3 = MagicMock()
        mock_s3.head_bucket.side_effect = ClientError(
            {"Error": {"Code": "403", "Message": "Forbidden"}},
            "HeadBucket",
        )

        mock_create_client.side_effect = lambda svc, **kw: (
            mock_dz if svc == "datazone" else mock_s3
        )
        mock_get_conns.return_value = {
            "default.s3_shared": {"s3Uri": "s3://forbidden-bucket"},
        }

        context = _make_context(storage_names=["forbidden-bucket"])
        findings = checker.check(context)

        s3_errors = [
            f for f in findings if f.severity == Severity.ERROR and f.service == "s3"
        ]
        assert len(s3_errors) >= 1
        assert s3_errors[0].resource is not None
