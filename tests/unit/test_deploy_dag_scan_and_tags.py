"""Unit tests for the workflow-deploy fixes:

1. _find_dag_files_in_s3 scans each YAML once (single pass) and fails fast with
   WorkflowYamlNotFoundError when a manifest workflow has no matching YAML.
2. _ensure_workflow_tags adds missing tags AND updates changed values, but
   never deletes tags that are absent from the desired set.
"""

from unittest.mock import MagicMock, patch

import pytest
import yaml

from smus_cicd.commands.deploy import (
    WorkflowYamlNotFoundError,
    _find_dag_files_in_s3,
)
from smus_cicd.bootstrap.handlers.workflow_create_handler import _ensure_workflow_tags


def _make_target_config(target_directory="workflows"):
    """Target config exposing a single storage entry with a target directory."""
    storage_item = MagicMock()
    storage_item.target_directory = target_directory

    target_config = MagicMock()
    target_config.deployment_configuration.storage = [storage_item]
    return target_config


def _make_manifest(workflow_names):
    manifest = MagicMock()
    manifest.content.workflows = [{"workflowName": n} for n in workflow_names]
    return manifest


class _FakeS3:
    """Minimal S3 client stub that counts get_object calls.

    Objects is a dict of {s3_key: yaml_dict}. All keys are returned under a
    single list_objects_v2 page for whatever prefix is requested.
    """

    def __init__(self, objects):
        self._objects = objects
        self.get_object_calls = 0

    def get_paginator(self, _op):
        objects = self._objects

        class _Paginator:
            def paginate(self, Bucket=None, Prefix=None):
                yield {"Contents": [{"Key": k} for k in objects]}

        return _Paginator()

    def get_object(self, Bucket=None, Key=None):
        self.get_object_calls += 1
        body = MagicMock()
        body.read.return_value = yaml.safe_dump(self._objects[Key]).encode("utf-8")
        return {"Body": body}


def test_find_dags_matches_by_top_level_key():
    s3 = _FakeS3(
        {
            "test-prefix/workflows/a.yaml": {"workflow_a": {"dag_id": "workflow_a"}},
            "test-prefix/workflows/b.yaml": {"workflow_b": {"dag_id": "workflow_b"}},
        }
    )
    manifest = _make_manifest(["workflow_a", "workflow_b"])

    result = _find_dag_files_in_s3(
        s3, "bucket", "test-prefix/", manifest, _make_target_config()
    )

    assert ("test-prefix/workflows/a.yaml", "workflow_a") in result
    assert ("test-prefix/workflows/b.yaml", "workflow_b") in result


def test_find_dags_matches_by_dag_id():
    s3 = _FakeS3(
        {"test-prefix/workflows/a.yaml": {"top_key": {"dag_id": "real_dag_id"}}}
    )
    manifest = _make_manifest(["real_dag_id"])

    result = _find_dag_files_in_s3(
        s3, "bucket", "test-prefix/", manifest, _make_target_config()
    )

    assert result == [("test-prefix/workflows/a.yaml", "real_dag_id")]


def test_find_dags_scans_each_yaml_once():
    """Single-pass scan: each YAML is downloaded exactly once regardless of the
    number of manifest workflows (previously it was O(workflows x files))."""
    s3 = _FakeS3(
        {
            "test-prefix/workflows/a.yaml": {"workflow_a": {"dag_id": "workflow_a"}},
            "test-prefix/workflows/b.yaml": {"workflow_b": {"dag_id": "workflow_b"}},
            "test-prefix/workflows/c.yaml": {"workflow_c": {"dag_id": "workflow_c"}},
        }
    )
    manifest = _make_manifest(["workflow_a", "workflow_b", "workflow_c"])

    _find_dag_files_in_s3(
        s3, "bucket", "test-prefix/", manifest, _make_target_config()
    )

    # 3 files scanned once each — not 3 workflows x 3 files.
    assert s3.get_object_calls == 3


def test_find_dags_fails_fast_on_missing_yaml():
    """A manifest workflow with no matching YAML raises immediately."""
    s3 = _FakeS3(
        {"test-prefix/workflows/a.yaml": {"workflow_a": {"dag_id": "workflow_a"}}}
    )
    manifest = _make_manifest(["workflow_a", "missing_workflow"])

    with pytest.raises(WorkflowYamlNotFoundError) as exc:
        _find_dag_files_in_s3(
            s3, "bucket", "test-prefix/", manifest, _make_target_config()
        )

    assert "missing_workflow" in str(exc.value)


def _tag_client(current_tags):
    client = MagicMock()
    client.list_tags_for_resource.return_value = {"Tags": dict(current_tags)}
    return client


def test_ensure_tags_adds_missing():
    client = _tag_client({"Existing": "1"})
    with patch(
        "smus_cicd.bootstrap.handlers.workflow_create_handler.airflow_serverless"
    ) as mock_af:
        mock_af.create_airflow_serverless_client.return_value = client
        _ensure_workflow_tags("arn:w", {"Existing": "1", "New": "2"}, "us-east-1")

    client.tag_resource.assert_called_once_with(
        ResourceArn="arn:w", Tags={"New": "2"}
    )


def test_ensure_tags_updates_changed_value():
    """Changing a tag value in the manifest is now reflected on the workflow."""
    client = _tag_client({"Env": "dev"})
    with patch(
        "smus_cicd.bootstrap.handlers.workflow_create_handler.airflow_serverless"
    ) as mock_af:
        mock_af.create_airflow_serverless_client.return_value = client
        _ensure_workflow_tags("arn:w", {"Env": "test"}, "us-east-1")

    client.tag_resource.assert_called_once_with(
        ResourceArn="arn:w", Tags={"Env": "test"}
    )


def test_ensure_tags_noop_when_all_match():
    client = _tag_client({"Env": "test", "Pipeline": "demo"})
    with patch(
        "smus_cicd.bootstrap.handlers.workflow_create_handler.airflow_serverless"
    ) as mock_af:
        mock_af.create_airflow_serverless_client.return_value = client
        _ensure_workflow_tags(
            "arn:w", {"Env": "test", "Pipeline": "demo"}, "us-east-1"
        )

    client.tag_resource.assert_not_called()


def test_ensure_tags_never_deletes_unmanaged():
    """Tags present on the workflow but absent from desired are left untouched."""
    client = _tag_client({"Env": "dev", "ManualTag": "keep-me"})
    with patch(
        "smus_cicd.bootstrap.handlers.workflow_create_handler.airflow_serverless"
    ) as mock_af:
        mock_af.create_airflow_serverless_client.return_value = client
        _ensure_workflow_tags("arn:w", {"Env": "test"}, "us-east-1")

    # No untag/delete call of any kind.
    client.untag_resource.assert_not_called()
    # Only the changed key is applied; ManualTag is not referenced.
    client.tag_resource.assert_called_once_with(
        ResourceArn="arn:w", Tags={"Env": "test"}
    )
