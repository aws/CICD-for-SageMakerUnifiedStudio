"""Unit tests for local-filesystem storage deployment.

Regression coverage for the multi-folder bug: a single storage item whose
`include` lists several folders must stage files from ALL folders (not just
the first), so every file reaches S3. Previously the staged path for every
file was computed relative to include[0], sending other folders' files
outside the temp dir where `aws s3 sync` never uploaded them.
"""

import os
from dataclasses import dataclass, field
from typing import List, Optional
from unittest.mock import patch

from smus_cicd.commands.deploy import _deploy_local_storage_item


@dataclass
class _StorageConfig:
    name: str = "workflows"
    connectionName: Optional[str] = "default.s3_shared"
    include: List[str] = field(default_factory=list)
    targetDirectory: str = "app/workflows"
    compression: Optional[str] = None


def _make_multifolder_tree(root):
    """Create SLH/PSD/data-migration folders with uniquely-named files."""
    layout = {
        "SLH": ["SLH_a.yaml", "SLH_b.yaml"],
        "PSD": ["PSD_a.yaml", "PSD_b.yaml"],
        "data-migration": ["dm_a.yaml", "dm_b.yaml"],
    }
    for folder, files in layout.items():
        os.makedirs(os.path.join(root, folder), exist_ok=True)
        for f in files:
            with open(os.path.join(root, folder, f), "w") as fh:
                fh.write(f"content of {f}")
    return layout


class _CaptureDeploy:
    """Stand-in for deployment.deploy_files that snapshots the staged files."""

    def __init__(self):
        self.staged_files = None

    def __call__(
        self, temp_dir, connection, target_directory, region, source_folder, *a, **k
    ):
        # Snapshot the files present in the staging dir at upload time (the
        # real temp dir is deleted once the function returns).
        staged = []
        for r, _dirs, files in os.walk(source_folder):
            for f in files:
                staged.append(os.path.relpath(os.path.join(r, f), source_folder))
        self.staged_files = sorted(staged)
        return len(staged)  # deploy_files returns files_synced (truthy)


def test_multifolder_include_stages_all_folders(tmp_path):
    """All files across every include folder are staged for upload."""
    manifest_dir = str(tmp_path)
    _make_multifolder_tree(manifest_dir)

    storage_config = _StorageConfig(include=["SLH/", "PSD/", "data-migration/"])

    capture = _CaptureDeploy()
    with patch(
        "smus_cicd.commands.deploy._get_project_connection",
        return_value={"s3Uri": "s3://bucket/shared/"},
    ), patch("smus_cicd.commands.deploy.deployment.deploy_files", side_effect=capture):
        deployed_files, s3_uri = _deploy_local_storage_item(
            manifest_dir,
            storage_config,
            storage_config,
            "test-project",
            {"region": "us-east-1"},
        )

    # Every file from all three folders must be staged (flat layout).
    assert capture.staged_files == sorted(
        [
            "SLH_a.yaml",
            "SLH_b.yaml",
            "PSD_a.yaml",
            "PSD_b.yaml",
            "dm_a.yaml",
            "dm_b.yaml",
        ]
    )
    # And none staged outside the temp dir (no "../" escaping).
    assert all(not f.startswith("..") for f in capture.staged_files)
    assert len(deployed_files) == 6
    assert s3_uri == "s3://bucket/shared/"


def test_single_folder_include_unchanged(tmp_path):
    """A single-folder include still stages that folder's files flat."""
    manifest_dir = str(tmp_path)
    os.makedirs(os.path.join(manifest_dir, "workflows"))
    for f in ["a.yaml", "b.yaml"]:
        with open(os.path.join(manifest_dir, "workflows", f), "w") as fh:
            fh.write("x")

    storage_config = _StorageConfig(include=["workflows/"])

    capture = _CaptureDeploy()
    with patch(
        "smus_cicd.commands.deploy._get_project_connection",
        return_value={"s3Uri": "s3://bucket/shared/"},
    ), patch("smus_cicd.commands.deploy.deployment.deploy_files", side_effect=capture):
        _deploy_local_storage_item(
            manifest_dir,
            storage_config,
            storage_config,
            "test-project",
            {"region": "us-east-1"},
        )

    assert capture.staged_files == ["a.yaml", "b.yaml"]


def test_no_files_returns_empty(tmp_path):
    """An include pointing at nothing returns empty without calling deploy."""
    storage_config = _StorageConfig(include=["does-not-exist/"])

    with patch("smus_cicd.commands.deploy._get_project_connection") as mock_conn, patch(
        "smus_cicd.commands.deploy.deployment.deploy_files"
    ) as mock_deploy:
        deployed_files, s3_uri = _deploy_local_storage_item(
            str(tmp_path),
            storage_config,
            storage_config,
            "test-project",
            {"region": "us-east-1"},
        )

    assert deployed_files == []
    assert s3_uri is None
    mock_conn.assert_not_called()
    mock_deploy.assert_not_called()
