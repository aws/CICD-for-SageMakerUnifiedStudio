"""Unit tests for the workflow-deploy fixes:

1. _find_dag_files_in_s3 scans each YAML once (single pass) and fails fast with
   WorkflowYamlNotFoundError when a manifest workflow has no matching YAML.
2. _apply_workflow_tags always overwrites the workflow's tags with the
   manifest-derived set without inspecting current tags, but never deletes tags
   that are absent from the desired set.
"""

from unittest.mock import MagicMock, patch

import pytest
import yaml

from smus_cicd.commands.deploy import (
    WorkflowYamlNotFoundError,
    _find_dag_files_in_s3,
)
from smus_cicd.bootstrap.handlers.workflow_create_handler import _apply_workflow_tags


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


def _make_manifest_with_code(workflow_code):
    """workflow_code: {workflowName: code_path_or_None}."""
    manifest = MagicMock()
    workflows = []
    for name, code in workflow_code.items():
        entry = {"workflowName": name}
        if code is not None:
            entry["code"] = code
        workflows.append(entry)
    manifest.content.workflows = workflows
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


class _CodeS3:
    """S3 stub for _resolve_and_upload_operator_code.

    Serves a set of deployed object keys via list_objects_v2 pagination and
    records copy_object calls (source key -> operatorFiles/ dest key). The code
    is read from S3 (not a bundle) and copied within S3, so no downloads occur.
    """

    def __init__(self, keys):
        self._keys = list(keys)
        self.copies = []  # list of (source_key, dest_key)

    def get_paginator(self, _op):
        keys = self._keys

        class _Paginator:
            def paginate(self, Bucket=None, Prefix=None):
                yield {
                    "Contents": [
                        {"Key": k} for k in keys if k.startswith(Prefix or "")
                    ]
                }

        return _Paginator()

    def copy_object(self, Bucket=None, CopySource=None, Key=None):
        self.copies.append((CopySource["Key"], Key))


def _make_code_target_config(target_directory="python-bash-operators/bundle"):
    storage_item = MagicMock()
    storage_item.target_directory = target_directory
    tc = MagicMock()
    tc.deployment_configuration.storage = [storage_item]
    return tc


def test_operator_code_single_file_copied_to_operator_files():
    from smus_cicd.commands.deploy import _resolve_and_upload_operator_code

    s3 = _CodeS3(["proj/dev/python-bash-operators/bundle/greet.sh"])
    manifest = _make_manifest_with_code({"wf": "code/bash/greet.sh"})

    result = _resolve_and_upload_operator_code(
        s3, "bucket", "proj/dev/", manifest, _make_code_target_config()
    )

    key = result["wf"]
    # Copied into the operatorFiles/ prefix, keeping the .sh extension.
    assert key.startswith("proj/dev/operatorFiles/wf-")
    assert key.endswith(".sh")
    assert s3.copies == [
        ("proj/dev/python-bash-operators/bundle/greet.sh", key)
    ]


def test_operator_code_zip_copied_as_is():
    from smus_cicd.commands.deploy import _resolve_and_upload_operator_code

    s3 = _CodeS3(["proj/dev/python-bash-operators/bundle/hello_package.zip"])
    manifest = _make_manifest_with_code({"wf": "code/python/hello_package.zip"})

    result = _resolve_and_upload_operator_code(
        s3, "bucket", "proj/dev/", manifest, _make_code_target_config()
    )

    # .zip resolved by basename and copied verbatim (no repackaging).
    assert result["wf"].endswith(".zip")
    src, dest = s3.copies[0]
    assert src == "proj/dev/python-bash-operators/bundle/hello_package.zip"
    assert dest == result["wf"]


def test_operator_code_directory_ref_is_rejected():
    """A `code` naming a directory (no extension) is a hard error - no zipping."""
    from smus_cicd.commands.deploy import _resolve_and_upload_operator_code

    s3 = _CodeS3(["proj/dev/python-bash-operators/bundle/mypkg/mod.py"])
    manifest = _make_manifest_with_code({"wf": "code/mypkg"})

    with pytest.raises(WorkflowYamlNotFoundError) as exc:
        _resolve_and_upload_operator_code(
            s3, "bucket", "proj/dev/", manifest, _make_code_target_config()
        )

    assert "single .py/.sh file or a pre-built .zip" in str(exc.value)
    assert s3.copies == []  # rejected before any copy


def test_operator_code_skips_workflows_without_code():
    from smus_cicd.commands.deploy import _resolve_and_upload_operator_code

    s3 = _CodeS3(["proj/dev/python-bash-operators/bundle/greet.sh"])
    manifest = _make_manifest_with_code({"wf_a": "code/greet.sh", "wf_b": None})

    result = _resolve_and_upload_operator_code(
        s3, "bucket", "proj/dev/", manifest, _make_code_target_config()
    )

    assert list(result) == ["wf_a"]


def test_operator_code_returns_empty_when_none_declared():
    from smus_cicd.commands.deploy import _resolve_and_upload_operator_code

    s3 = _CodeS3(["proj/dev/python-bash-operators/bundle/greet.sh"])
    manifest = _make_manifest(["wf_a", "wf_b"])

    result = _resolve_and_upload_operator_code(
        s3, "bucket", "proj/dev/", manifest, _make_code_target_config()
    )

    assert result == {}
    assert s3.copies == []


def test_operator_code_fails_fast_when_missing_from_s3():
    from smus_cicd.commands.deploy import _resolve_and_upload_operator_code

    s3 = _CodeS3(["proj/dev/python-bash-operators/bundle/present.py"])
    manifest = _make_manifest_with_code({"wf": "code/missing.zip"})

    with pytest.raises(WorkflowYamlNotFoundError) as exc:
        _resolve_and_upload_operator_code(
            s3, "bucket", "proj/dev/", manifest, _make_code_target_config()
        )

    assert "missing.zip" in str(exc.value)


def _tag_client():
    return MagicMock()


def test_apply_tags_overwrites_full_set():
    """The full desired set is written verbatim, overwriting existing values."""
    client = _tag_client()
    with patch(
        "smus_cicd.bootstrap.handlers.workflow_create_handler.airflow_serverless"
    ) as mock_af:
        mock_af.create_airflow_serverless_client.return_value = client
        _apply_workflow_tags("arn:w", {"Env": "test", "Pipeline": "demo"}, "us-east-1")

    client.tag_resource.assert_called_once_with(
        ResourceArn="arn:w", Tags={"Env": "test", "Pipeline": "demo"}
    )


def test_apply_tags_does_not_read_current_state():
    """Tags are applied without inspecting the workflow's existing tags."""
    client = _tag_client()
    with patch(
        "smus_cicd.bootstrap.handlers.workflow_create_handler.airflow_serverless"
    ) as mock_af:
        mock_af.create_airflow_serverless_client.return_value = client
        _apply_workflow_tags("arn:w", {"Env": "test"}, "us-east-1")

    client.list_tags_for_resource.assert_not_called()
    client.tag_resource.assert_called_once_with(
        ResourceArn="arn:w", Tags={"Env": "test"}
    )


def test_apply_tags_never_deletes_unmanaged():
    """Tags absent from the desired set are never removed."""
    client = _tag_client()
    with patch(
        "smus_cicd.bootstrap.handlers.workflow_create_handler.airflow_serverless"
    ) as mock_af:
        mock_af.create_airflow_serverless_client.return_value = client
        _apply_workflow_tags("arn:w", {"Env": "test"}, "us-east-1")

    client.untag_resource.assert_not_called()


def test_apply_tags_noop_when_empty():
    """An empty desired set makes no AWS calls at all."""
    client = _tag_client()
    with patch(
        "smus_cicd.bootstrap.handlers.workflow_create_handler.airflow_serverless"
    ) as mock_af:
        mock_af.create_airflow_serverless_client.return_value = client
        _apply_workflow_tags("arn:w", {}, "us-east-1")

    mock_af.create_airflow_serverless_client.assert_not_called()
    client.tag_resource.assert_not_called()
