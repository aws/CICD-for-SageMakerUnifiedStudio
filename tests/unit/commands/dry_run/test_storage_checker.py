"""Unit tests for StorageChecker (Phase 7 — Storage Deployment Simulation).

Tests cover:
- No target config → WARNING
- No storage items → OK skip
- Single storage item with matching bundle files
- Multiple storage items with varying file counts
- Storage item with no matching files (zero count)
- Bucket derivation from connectionName
- Prefix from targetDirectory
- Finding metadata (resource, service, details)
- Local-filesystem storage items (no connectionName, no bundle): file count
  resolved from disk via include patterns, not context.bundle_files
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Set
from unittest.mock import patch

import pytest

from smus_cicd.commands.dry_run.checkers.storage_checker import StorageChecker
from smus_cicd.commands.dry_run.models import DryRunContext, Severity

# ---------------------------------------------------------------------------
# Lightweight stub dataclasses
# ---------------------------------------------------------------------------


@dataclass
class _StorageConfig:
    name: str
    connectionName: Optional[str] = "default.s3_shared"
    targetDirectory: str = ""


@dataclass
class _DeploymentConfiguration:
    storage: List[_StorageConfig] = field(default_factory=list)


@dataclass
class _TargetConfig:
    deployment_configuration: Optional[_DeploymentConfiguration] = None


@dataclass
class _ContentStorageItem:
    """Stub for a manifest.content.storage entry (source-side config)."""

    name: str
    connectionName: Optional[str] = None
    include: List[str] = field(default_factory=list)


@dataclass
class _ContentConfig:
    storage: List[_ContentStorageItem] = field(default_factory=list)


@dataclass
class _Manifest:
    content: Optional[_ContentConfig] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_context(
    storage_items: Optional[List[_StorageConfig]] = None,
    bundle_files: Optional[Set[str]] = None,
    manifest_file: str = "manifest.yaml",
    content_items: Optional[List[_ContentStorageItem]] = None,
) -> DryRunContext:
    """Build a DryRunContext with storage configuration.

    ``manifest_file`` sets ``context.manifest_file`` (its directory is where
    local ``include`` patterns are resolved relative to). ``content_items``
    populates ``context.manifest.content.storage`` — the source-side entries
    the checker consults to decide whether an item is local-filesystem-only.
    """
    dep_cfg = _DeploymentConfiguration(storage=storage_items or [])
    target = _TargetConfig(deployment_configuration=dep_cfg)

    ctx = DryRunContext(
        manifest_file=manifest_file,
        target_config=target,
        config={"region": "us-east-1"},
    )
    if bundle_files is not None:
        ctx.bundle_files = bundle_files
    if content_items is not None:
        ctx.manifest = _Manifest(content=_ContentConfig(storage=content_items))
    return ctx


def _mock_connections(*connection_mappings: tuple) -> dict:
    """Build a mock connections dict from (connectionName, bucket) pairs.

    Example: ``_mock_connections(("default.s3_shared", "real-bucket-123"))``
    """
    conns = {}
    for conn_name, bucket in connection_mappings:
        conns[conn_name] = {"s3Uri": f"s3://{bucket}"}
    return conns


@pytest.fixture
def checker() -> StorageChecker:
    return StorageChecker()


# ---------------------------------------------------------------------------
# No config / empty config
# ---------------------------------------------------------------------------


# TestStorageCheckerNoConfig tests moved to test_preflight_checker.py

# ---------------------------------------------------------------------------
# Single storage item
# ---------------------------------------------------------------------------


class TestSingleStorageItem:
    """A single storage item with matching bundle files."""

    @patch(
        "smus_cicd.commands.dry_run.checkers.storage_checker.get_project_connections"
    )
    def test_reports_bucket_prefix_and_file_count(self, mock_conns, checker):
        mock_conns.return_value = _mock_connections(
            ("default.s3_shared", "real-bucket-123")
        )
        items = [
            _StorageConfig(
                name="code", connectionName="default.s3_shared", targetDirectory="src"
            )
        ]
        bundle = {"code/main.py", "code/utils.py", "other/readme.md"}
        ctx = _make_context(storage_items=items, bundle_files=bundle)

        findings = checker.check(ctx)

        assert len(findings) == 1
        f = findings[0]
        assert f.severity == Severity.OK
        assert "code" in f.message
        assert "real-bucket-123" in f.message
        assert "src" in f.message
        assert "2 file(s)" in f.message

    @patch(
        "smus_cicd.commands.dry_run.checkers.storage_checker.get_project_connections"
    )
    def test_zero_matching_files(self, mock_conns, checker):
        mock_conns.return_value = _mock_connections(
            ("default.s3_shared", "real-bucket-123")
        )
        items = [
            _StorageConfig(
                name="data", connectionName="default.s3_shared", targetDirectory="data"
            )
        ]
        bundle = {"code/main.py"}
        ctx = _make_context(storage_items=items, bundle_files=bundle)

        findings = checker.check(ctx)

        assert len(findings) == 1
        assert "0 file(s)" in findings[0].message

    @patch(
        "smus_cicd.commands.dry_run.checkers.storage_checker.get_project_connections"
    )
    def test_empty_bundle(self, mock_conns, checker):
        mock_conns.return_value = _mock_connections(
            ("default.s3_shared", "real-bucket-123")
        )
        items = [
            _StorageConfig(
                name="code", connectionName="default.s3_shared", targetDirectory=""
            )
        ]
        ctx = _make_context(storage_items=items, bundle_files=set())

        findings = checker.check(ctx)

        assert len(findings) == 1
        assert "0 file(s)" in findings[0].message


# ---------------------------------------------------------------------------
# Multiple storage items
# ---------------------------------------------------------------------------


class TestMultipleStorageItems:
    """Multiple storage items produce one finding each."""

    @patch(
        "smus_cicd.commands.dry_run.checkers.storage_checker.get_project_connections"
    )
    def test_two_items_produce_two_findings(self, mock_conns, checker):
        mock_conns.return_value = _mock_connections(
            ("default.s3_shared", "real-bucket-123"),
            ("project.s3_data", "data-bucket-456"),
        )
        items = [
            _StorageConfig(
                name="code", connectionName="default.s3_shared", targetDirectory="src"
            ),
            _StorageConfig(
                name="data",
                connectionName="project.s3_data",
                targetDirectory="datasets",
            ),
        ]
        bundle = {"code/a.py", "code/b.py", "data/file.csv"}
        ctx = _make_context(storage_items=items, bundle_files=bundle)

        findings = checker.check(ctx)

        assert len(findings) == 2
        # First item
        assert "code" in findings[0].message
        assert "real-bucket-123" in findings[0].message
        assert "2 file(s)" in findings[0].message
        # Second item
        assert "data" in findings[1].message
        assert "data-bucket-456" in findings[1].message
        assert "1 file(s)" in findings[1].message


# ---------------------------------------------------------------------------
# Bucket derivation from connectionName
# ---------------------------------------------------------------------------


class TestBucketDerivation:
    """Bucket name is resolved from DataZone project connections."""

    @patch(
        "smus_cicd.commands.dry_run.checkers.storage_checker.get_project_connections"
    )
    def test_resolved_connection_uses_real_bucket(self, mock_conns, checker):
        mock_conns.return_value = _mock_connections(
            ("default.my_bucket", "actual-bucket-name")
        )
        items = [
            _StorageConfig(
                name="x", connectionName="default.my_bucket", targetDirectory=""
            )
        ]
        ctx = _make_context(storage_items=items, bundle_files=set())

        findings = checker.check(ctx)
        assert "actual-bucket-name" in findings[0].message

    @patch(
        "smus_cicd.commands.dry_run.checkers.storage_checker.get_project_connections"
    )
    def test_unresolved_connection_shows_placeholder(self, mock_conns, checker):
        """When connection can't be resolved, show an unresolved placeholder."""
        mock_conns.return_value = {}
        items = [
            _StorageConfig(name="x", connectionName="mybucket", targetDirectory="")
        ]
        ctx = _make_context(storage_items=items, bundle_files=set())

        findings = checker.check(ctx)
        assert "<unresolved:mybucket>" in findings[0].message

    @patch(
        "smus_cicd.commands.dry_run.checkers.storage_checker.get_project_connections"
    )
    def test_empty_connection_name(self, mock_conns, checker):
        mock_conns.return_value = {}
        items = [_StorageConfig(name="x", connectionName="", targetDirectory="")]
        ctx = _make_context(storage_items=items, bundle_files=set())

        findings = checker.check(ctx)
        # Empty connection → no connection to resolve, shows unknown
        assert "<unknown>" in findings[0].message

    @patch(
        "smus_cicd.commands.dry_run.checkers.storage_checker.get_project_connections"
    )
    def test_none_connection_name(self, mock_conns, checker):
        mock_conns.return_value = {}
        items = [_StorageConfig(name="x", connectionName=None, targetDirectory="")]
        ctx = _make_context(storage_items=items, bundle_files=set())

        findings = checker.check(ctx)
        assert "<unknown>" in findings[0].message

    @patch(
        "smus_cicd.commands.dry_run.checkers.storage_checker.get_project_connections"
    )
    def test_null_project_connections_shows_unresolved(self, mock_conns, checker):
        """When _get_project_connections returns None, show unresolved."""
        mock_conns.return_value = None
        items = [
            _StorageConfig(
                name="x", connectionName="default.s3_shared", targetDirectory=""
            )
        ]
        ctx = _make_context(storage_items=items, bundle_files=set())

        findings = checker.check(ctx)
        assert "<unresolved:default.s3_shared>" in findings[0].message


# ---------------------------------------------------------------------------
# Prefix from targetDirectory
# ---------------------------------------------------------------------------


class TestPrefixReporting:
    """targetDirectory is reported as the S3 prefix."""

    @patch(
        "smus_cicd.commands.dry_run.checkers.storage_checker.get_project_connections"
    )
    def test_non_empty_prefix(self, mock_conns, checker):
        mock_conns.return_value = _mock_connections(
            ("default.s3_shared", "real-bucket")
        )
        items = [
            _StorageConfig(
                name="x",
                connectionName="default.s3_shared",
                targetDirectory="my/prefix",
            )
        ]
        ctx = _make_context(storage_items=items, bundle_files=set())

        findings = checker.check(ctx)
        assert "my/prefix" in findings[0].message

    @patch(
        "smus_cicd.commands.dry_run.checkers.storage_checker.get_project_connections"
    )
    def test_empty_prefix(self, mock_conns, checker):
        mock_conns.return_value = _mock_connections(
            ("default.s3_shared", "real-bucket")
        )
        items = [
            _StorageConfig(
                name="x", connectionName="default.s3_shared", targetDirectory=""
            )
        ]
        ctx = _make_context(storage_items=items, bundle_files=set())

        findings = checker.check(ctx)
        assert "prefix ''" in findings[0].message

    @patch(
        "smus_cicd.commands.dry_run.checkers.storage_checker.get_project_connections"
    )
    def test_none_target_directory(self, mock_conns, checker):
        mock_conns.return_value = _mock_connections(
            ("default.s3_shared", "real-bucket")
        )
        items = [
            _StorageConfig(
                name="x", connectionName="default.s3_shared", targetDirectory=None
            )
        ]
        ctx = _make_context(storage_items=items, bundle_files=set())

        findings = checker.check(ctx)
        assert "prefix ''" in findings[0].message


# ---------------------------------------------------------------------------
# Finding metadata
# ---------------------------------------------------------------------------


class TestFindingMetadata:
    """Each finding carries resource, service, and details."""

    @patch(
        "smus_cicd.commands.dry_run.checkers.storage_checker.get_project_connections"
    )
    def test_finding_has_resource(self, mock_conns, checker):
        mock_conns.return_value = _mock_connections(
            ("default.s3_shared", "real-bucket")
        )
        items = [
            _StorageConfig(
                name="code", connectionName="default.s3_shared", targetDirectory="src"
            )
        ]
        ctx = _make_context(storage_items=items, bundle_files={"code/a.py"})

        findings = checker.check(ctx)
        assert findings[0].resource == "code"

    @patch(
        "smus_cicd.commands.dry_run.checkers.storage_checker.get_project_connections"
    )
    def test_finding_has_service(self, mock_conns, checker):
        mock_conns.return_value = _mock_connections(
            ("default.s3_shared", "real-bucket")
        )
        items = [
            _StorageConfig(
                name="code", connectionName="default.s3_shared", targetDirectory="src"
            )
        ]
        ctx = _make_context(storage_items=items, bundle_files=set())

        findings = checker.check(ctx)
        assert findings[0].service == "s3"

    @patch(
        "smus_cicd.commands.dry_run.checkers.storage_checker.get_project_connections"
    )
    def test_finding_has_details(self, mock_conns, checker):
        mock_conns.return_value = _mock_connections(
            ("default.s3_shared", "real-bucket")
        )
        items = [
            _StorageConfig(
                name="code", connectionName="default.s3_shared", targetDirectory="src"
            )
        ]
        ctx = _make_context(
            storage_items=items, bundle_files={"code/a.py", "code/b.py"}
        )

        findings = checker.check(ctx)
        details = findings[0].details
        assert details is not None
        assert details["bucket"] == "real-bucket"
        assert details["prefix"] == "src"
        assert details["file_count"] == 2

    def test_skip_finding_has_service(self, checker):
        ctx = _make_context(storage_items=[])
        findings = checker.check(ctx)
        assert findings[0].service == "s3"


# ---------------------------------------------------------------------------
# Local-filesystem storage items (no connectionName, no bundle)
# ---------------------------------------------------------------------------


class TestLocalFilesystemStorageItem:
    """Items with no connectionName on content.storage never ship in a bundle
    (see manifest_requires_bundle()); the checker must count their files on
    disk via `include`, not via the (always-empty) context.bundle_files.
    """

    @patch(
        "smus_cicd.commands.dry_run.checkers.storage_checker.get_project_connections"
    )
    def test_counts_files_from_disk_not_empty_bundle(self, mock_conns, checker, tmp_path):
        mock_conns.return_value = _mock_connections(
            ("default.s3_shared", "real-bucket")
        )
        (tmp_path / "notebooks").mkdir()
        (tmp_path / "notebooks" / "a.ipynb").write_text("{}")
        (tmp_path / "notebooks" / "b.ipynb").write_text("{}")
        manifest_file = str(tmp_path / "manifest.yaml")

        deploy_items = [
            _StorageConfig(
                name="notebooks",
                connectionName="default.s3_shared",
                targetDirectory="notebooks",
            )
        ]
        content_items = [
            _ContentStorageItem(
                name="notebooks", connectionName=None, include=["notebooks"]
            )
        ]
        # bundle_files intentionally left empty/unset — no bundle exists for
        # a manifest with only local-filesystem storage items.
        ctx = _make_context(
            storage_items=deploy_items,
            manifest_file=manifest_file,
            content_items=content_items,
        )

        findings = checker.check(ctx)

        assert len(findings) == 1
        assert "2 file(s)" in findings[0].message
        assert findings[0].details["file_count"] == 2

    @patch(
        "smus_cicd.commands.dry_run.checkers.storage_checker.get_project_connections"
    )
    def test_glob_pattern_counts_matching_files_only(
        self, mock_conns, checker, tmp_path
    ):
        mock_conns.return_value = _mock_connections(
            ("default.s3_shared", "real-bucket")
        )
        (tmp_path / "notebooks").mkdir()
        (tmp_path / "notebooks" / "a.ipynb").write_text("{}")
        (tmp_path / "notebooks" / "readme.md").write_text("x")
        manifest_file = str(tmp_path / "manifest.yaml")

        deploy_items = [
            _StorageConfig(
                name="notebooks",
                connectionName="default.s3_shared",
                targetDirectory="notebooks",
            )
        ]
        content_items = [
            _ContentStorageItem(
                name="notebooks",
                connectionName=None,
                include=["notebooks/*.ipynb"],
            )
        ]
        ctx = _make_context(
            storage_items=deploy_items,
            manifest_file=manifest_file,
            content_items=content_items,
        )

        findings = checker.check(ctx)

        assert "1 file(s)" in findings[0].message

    @patch(
        "smus_cicd.commands.dry_run.checkers.storage_checker.get_project_connections"
    )
    def test_no_matching_local_files_reports_zero(self, mock_conns, checker, tmp_path):
        mock_conns.return_value = _mock_connections(
            ("default.s3_shared", "real-bucket")
        )
        (tmp_path / "notebooks").mkdir()
        manifest_file = str(tmp_path / "manifest.yaml")

        deploy_items = [
            _StorageConfig(
                name="notebooks",
                connectionName="default.s3_shared",
                targetDirectory="notebooks",
            )
        ]
        content_items = [
            _ContentStorageItem(
                name="notebooks", connectionName=None, include=["notebooks"]
            )
        ]
        ctx = _make_context(
            storage_items=deploy_items,
            manifest_file=manifest_file,
            content_items=content_items,
        )

        findings = checker.check(ctx)
        assert "0 file(s)" in findings[0].message

    @patch(
        "smus_cicd.commands.dry_run.checkers.storage_checker.get_project_connections"
    )
    def test_item_with_connection_name_still_uses_bundle_files(
        self, mock_conns, checker, tmp_path
    ):
        """A content item WITH a connectionName is bundle-backed — even if a
        local directory of the same name happens to exist on disk, the
        checker must keep counting from context.bundle_files, not disk.
        """
        mock_conns.return_value = _mock_connections(
            ("default.s3_shared", "real-bucket")
        )
        (tmp_path / "code").mkdir()
        (tmp_path / "code" / "a.py").write_text("x")
        (tmp_path / "code" / "b.py").write_text("x")
        (tmp_path / "code" / "c.py").write_text("x")
        manifest_file = str(tmp_path / "manifest.yaml")

        deploy_items = [
            _StorageConfig(
                name="code", connectionName="default.s3_shared", targetDirectory="src"
            )
        ]
        content_items = [
            _ContentStorageItem(
                name="code", connectionName="default.s3_shared", include=["code"]
            )
        ]
        bundle = {"code/a.py"}  # only one file in the bundle
        ctx = _make_context(
            storage_items=deploy_items,
            manifest_file=manifest_file,
            content_items=content_items,
            bundle_files=bundle,
        )

        findings = checker.check(ctx)
        # Must reflect the bundle (1), not the 3 files sitting on disk.
        assert "1 file(s)" in findings[0].message

    @patch(
        "smus_cicd.commands.dry_run.checkers.storage_checker.get_project_connections"
    )
    def test_no_manifest_on_context_falls_back_to_bundle_files(
        self, mock_conns, checker
    ):
        """Without context.manifest populated (e.g. a caller that never ran
        ManifestChecker), the checker must not crash and should fall back to
        the bundle-files count.
        """
        mock_conns.return_value = _mock_connections(
            ("default.s3_shared", "real-bucket")
        )
        items = [
            _StorageConfig(
                name="code", connectionName="default.s3_shared", targetDirectory="src"
            )
        ]
        ctx = _make_context(storage_items=items, bundle_files={"code/a.py"})

        findings = checker.check(ctx)
        assert "1 file(s)" in findings[0].message
