"""Unit tests for notebook sync integration in deploy.py.

Covers _sync_notebooks_from_bundle:
  - Missing bundle → skip silently (True)
  - Bundle without notebook manifest → skip silently (True)
  - Notebook sync disabled in deployment_configuration → skip (True)
  - Happy path: manifest present → sync invoked → created/updated/failed reported
  - S3 connection missing → return False
  - Project not found → return False
  - Sync failures → return False
  - Missing .ipynb files in bundle handled gracefully

Resolution note: domain/project/connections are resolved once via
``get_datazone_project_info`` (or reused from a pre-resolved ``project_info``
passed in by ``deploy_command``). These tests inject ``project_info`` directly
or patch ``get_datazone_project_info`` at its source module.
"""

import json
import os
import tempfile
import zipfile
from unittest.mock import MagicMock, patch

import pytest

from smus_cicd.commands.deploy import _sync_notebooks_from_bundle

# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_target_config():
    config = MagicMock()
    config.deployment_configuration = MagicMock()
    config.deployment_configuration.notebooks = None  # not disabled
    config.domain = MagicMock()
    config.domain.region = "us-east-1"
    config.project = MagicMock()
    config.project.name = "target-project"
    return config


@pytest.fixture
def mock_config():
    return {"region": "us-east-1"}


def _make_project_info(s3_uri="s3://bucket/shared", project_id="proj-target"):
    """Build a resolved project_info dict as returned by get_datazone_project_info."""
    return {
        "projectId": project_id,
        "project_id": project_id,
        "domain_id": "dom-1",
        "status": "ACTIVE",
        "owners": [],
        "contributors": [],
        "connections": {"default.s3_shared": {"s3Uri": s3_uri}},
    }


def _make_manifest_data(notebooks=None):
    notebooks = notebooks or []
    return {
        "metadata": {
            "sourceProjectId": "src-proj",
            "sourceDomainId": "dom-1",
            "exportTimestamp": "2024-01-01T00:00:00Z",
            "notebookCount": len(notebooks),
        },
        "notebooks": notebooks,
    }


def _make_notebook_entry(nb_id="nb-1"):
    return {
        "sourceNotebookId": nb_id,
        "name": "Test NB",
        "description": "",
        "filePath": f"notebooks/{nb_id}.ipynb",
        "exportedAt": "2024-01-01T00:00:00Z",
        "parameters": {},
        "metadata": {},
        "environmentConfiguration": None,
    }


def _create_bundle(manifest_data=None, ipynb_files=None):
    """Create a temporary bundle ZIP and return its path + temp dir."""
    temp_dir = tempfile.mkdtemp()
    bundle_path = os.path.join(temp_dir, "test-bundle.zip")

    with zipfile.ZipFile(bundle_path, "w") as zf:
        if manifest_data is not None:
            zf.writestr(
                "notebooks/notebook_export_manifest.json",
                json.dumps(manifest_data),
            )
        for path, content in (ipynb_files or {}).items():
            zf.writestr(path, content)

    return bundle_path, temp_dir


def _cleanup(temp_dir):
    import shutil

    shutil.rmtree(temp_dir, ignore_errors=True)


# Resolution now goes through get_datazone_project_info (source module) and
# sync_notebooks. The connection s3Uri is read from the resolved project_info.
PATCH_PROJECT_INFO = "smus_cicd.helpers.utils.get_datazone_project_info"
PATCH_SYNC = "smus_cicd.helpers.notebook_import.sync_notebooks"


# ---------------------------------------------------------------------------
# Tests: skip conditions
# ---------------------------------------------------------------------------


class TestSyncNotebooksSkipConditions:

    def test_no_bundle_path_returns_true(self, mock_target_config, mock_config):
        result = _sync_notebooks_from_bundle(None, mock_target_config, mock_config)
        assert result is True

    def test_bundle_without_manifest_returns_true(
        self, mock_target_config, mock_config
    ):
        bundle_path, temp_dir = _create_bundle()  # no manifest
        try:
            result = _sync_notebooks_from_bundle(
                bundle_path, mock_target_config, mock_config
            )
            assert result is True
        finally:
            _cleanup(temp_dir)

    def test_notebook_sync_disabled_returns_true(self, mock_target_config, mock_config):
        mock_target_config.deployment_configuration.notebooks = {"disable": True}
        bundle_path, temp_dir = _create_bundle(
            manifest_data=_make_manifest_data([_make_notebook_entry()])
        )
        try:
            result = _sync_notebooks_from_bundle(
                bundle_path, mock_target_config, mock_config
            )
            assert result is True
        finally:
            _cleanup(temp_dir)

    def test_disable_false_does_not_skip(self, mock_target_config, mock_config):
        """disable=False must NOT skip — sync should proceed."""
        mock_target_config.deployment_configuration.notebooks = {"disable": False}
        bundle_path, temp_dir = _create_bundle(
            manifest_data=_make_manifest_data([_make_notebook_entry()]),
            ipynb_files={"notebooks/nb-1.ipynb": b'{"cells":[]}'},
        )
        try:
            with patch(PATCH_SYNC) as mock_sync:
                from smus_cicd.helpers.notebook_import import NotebookSyncSummary

                mock_sync.return_value = NotebookSyncSummary(created=1)
                result = _sync_notebooks_from_bundle(
                    bundle_path,
                    mock_target_config,
                    mock_config,
                    project_info=_make_project_info(),
                )
                # sync_notebooks must have been called
                mock_sync.assert_called_once()
                assert result is True
        finally:
            _cleanup(temp_dir)


# ---------------------------------------------------------------------------
# Tests: project_info reuse (no re-resolution)
# ---------------------------------------------------------------------------


class TestProjectInfoReuse:

    def test_passed_project_info_avoids_lookup(self, mock_target_config, mock_config):
        """When project_info is passed in, get_datazone_project_info is NOT called."""
        bundle_path, temp_dir = _create_bundle(
            manifest_data=_make_manifest_data([_make_notebook_entry()]),
            ipynb_files={"notebooks/nb-1.ipynb": b'{"cells":[]}'},
        )
        try:
            with patch(PATCH_PROJECT_INFO) as mock_lookup, patch(
                PATCH_SYNC
            ) as mock_sync:
                from smus_cicd.helpers.notebook_import import NotebookSyncSummary

                mock_sync.return_value = NotebookSyncSummary(created=1)
                result = _sync_notebooks_from_bundle(
                    bundle_path,
                    mock_target_config,
                    mock_config,
                    project_info=_make_project_info(),
                )
            assert result is True
            mock_lookup.assert_not_called()
        finally:
            _cleanup(temp_dir)

    def test_config_project_info_reused(self, mock_target_config, mock_config):
        """When config carries project_info, it is reused without a lookup."""
        mock_config["project_info"] = _make_project_info()
        bundle_path, temp_dir = _create_bundle(
            manifest_data=_make_manifest_data([_make_notebook_entry()]),
            ipynb_files={"notebooks/nb-1.ipynb": b'{"cells":[]}'},
        )
        try:
            with patch(PATCH_PROJECT_INFO) as mock_lookup, patch(
                PATCH_SYNC
            ) as mock_sync:
                from smus_cicd.helpers.notebook_import import NotebookSyncSummary

                mock_sync.return_value = NotebookSyncSummary(created=1)
                result = _sync_notebooks_from_bundle(
                    bundle_path, mock_target_config, mock_config
                )
            assert result is True
            mock_lookup.assert_not_called()
        finally:
            _cleanup(temp_dir)

    def test_falls_back_to_lookup_when_no_project_info(
        self, mock_target_config, mock_config
    ):
        """With no project_info anywhere, falls back to get_datazone_project_info."""
        bundle_path, temp_dir = _create_bundle(
            manifest_data=_make_manifest_data([_make_notebook_entry()]),
            ipynb_files={"notebooks/nb-1.ipynb": b'{"cells":[]}'},
        )
        try:
            with patch(
                PATCH_PROJECT_INFO, return_value=_make_project_info()
            ) as mock_lookup, patch(PATCH_SYNC) as mock_sync:
                from smus_cicd.helpers.notebook_import import NotebookSyncSummary

                mock_sync.return_value = NotebookSyncSummary(created=1)
                result = _sync_notebooks_from_bundle(
                    bundle_path, mock_target_config, mock_config
                )
            assert result is True
            mock_lookup.assert_called_once()
        finally:
            _cleanup(temp_dir)


# ---------------------------------------------------------------------------
# Tests: happy path
# ---------------------------------------------------------------------------


class TestSyncNotebooksHappyPath:

    def test_creates_summary_reported_in_output(
        self, mock_target_config, mock_config, capsys
    ):
        bundle_path, temp_dir = _create_bundle(
            manifest_data=_make_manifest_data([_make_notebook_entry()]),
            ipynb_files={"notebooks/nb-1.ipynb": b'{"cells":[]}'},
        )
        try:
            with patch(PATCH_SYNC) as mock_sync:
                from smus_cicd.helpers.notebook_import import NotebookSyncSummary

                mock_sync.return_value = NotebookSyncSummary(
                    created=1, updated=0, failed=0
                )
                result = _sync_notebooks_from_bundle(
                    bundle_path,
                    mock_target_config,
                    mock_config,
                    project_info=_make_project_info(s3_uri="s3://b/shared"),
                )

            assert result is True
            captured = capsys.readouterr()
            assert "Created: 1" in captured.out
            assert "Updated: 0" in captured.out
            assert "Failed:  0" in captured.out
        finally:
            _cleanup(temp_dir)

    def test_sync_notebooks_receives_correct_arguments(
        self, mock_target_config, mock_config
    ):
        entry = _make_notebook_entry("nb-abc")
        bundle_path, temp_dir = _create_bundle(
            manifest_data=_make_manifest_data([entry]),
            ipynb_files={"notebooks/nb-abc.ipynb": b'{"cells":[]}'},
        )
        try:
            with patch(PATCH_SYNC) as mock_sync:
                from smus_cicd.helpers.notebook_import import NotebookSyncSummary

                mock_sync.return_value = NotebookSyncSummary(created=1)
                _sync_notebooks_from_bundle(
                    bundle_path,
                    mock_target_config,
                    mock_config,
                    project_info=_make_project_info(s3_uri="s3://bucket/data"),
                )

            call_args = mock_sync.call_args
            # Verify domain_id, project_id, region, s3_uri threaded through
            assert "dom-1" in str(call_args)
            assert "proj-target" in str(call_args)
            assert "s3://bucket/data" in str(call_args)
        finally:
            _cleanup(temp_dir)


# ---------------------------------------------------------------------------
# Tests: failure paths
# ---------------------------------------------------------------------------


class TestSyncNotebooksFailurePaths:

    def test_missing_s3_connection_returns_false(self, mock_target_config, mock_config):
        bundle_path, temp_dir = _create_bundle(
            manifest_data=_make_manifest_data([_make_notebook_entry()])
        )
        try:
            result = _sync_notebooks_from_bundle(
                bundle_path,
                mock_target_config,
                mock_config,
                project_info=_make_project_info(s3_uri=""),
            )
            assert result is False
        finally:
            _cleanup(temp_dir)

    def test_project_not_found_returns_false(self, mock_target_config, mock_config):
        bundle_path, temp_dir = _create_bundle(
            manifest_data=_make_manifest_data([_make_notebook_entry()])
        )
        try:
            project_info = _make_project_info()
            project_info["projectId"] = None
            project_info["project_id"] = None
            result = _sync_notebooks_from_bundle(
                bundle_path,
                mock_target_config,
                mock_config,
                project_info=project_info,
            )
            assert result is False
        finally:
            _cleanup(temp_dir)

    def test_project_info_error_returns_false(self, mock_target_config, mock_config):
        bundle_path, temp_dir = _create_bundle(
            manifest_data=_make_manifest_data([_make_notebook_entry()])
        )
        try:
            result = _sync_notebooks_from_bundle(
                bundle_path,
                mock_target_config,
                mock_config,
                project_info={"error": "Domain not found"},
            )
            assert result is False
        finally:
            _cleanup(temp_dir)

    def test_sync_failures_returns_false(self, mock_target_config, mock_config):
        bundle_path, temp_dir = _create_bundle(
            manifest_data=_make_manifest_data([_make_notebook_entry()]),
            ipynb_files={"notebooks/nb-1.ipynb": b'{"cells":[]}'},
        )
        try:
            with patch(PATCH_SYNC) as mock_sync:
                from smus_cicd.helpers.notebook_import import NotebookSyncSummary

                mock_sync.return_value = NotebookSyncSummary(
                    created=0, updated=0, failed=1, failed_ids=["nb-1"]
                )
                result = _sync_notebooks_from_bundle(
                    bundle_path,
                    mock_target_config,
                    mock_config,
                    project_info=_make_project_info(s3_uri="s3://b/shared"),
                )
            assert result is False
        finally:
            _cleanup(temp_dir)

    def test_malformed_manifest_json_returns_false(
        self, mock_target_config, mock_config
    ):
        temp_dir = tempfile.mkdtemp()
        bundle_path = os.path.join(temp_dir, "bundle.zip")
        try:
            with zipfile.ZipFile(bundle_path, "w") as zf:
                zf.writestr(
                    "notebooks/notebook_export_manifest.json", "{ invalid json }"
                )
            result = _sync_notebooks_from_bundle(
                bundle_path,
                mock_target_config,
                mock_config,
                project_info=_make_project_info(),
            )
            assert result is False
        finally:
            _cleanup(temp_dir)
