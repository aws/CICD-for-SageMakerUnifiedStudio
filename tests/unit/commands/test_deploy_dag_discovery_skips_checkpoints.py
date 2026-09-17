"""Unit tests: editor/build artifacts are skipped in both S3 workflow-discovery
paths (``_find_dag_files_in_s3`` and ``_resolve_and_upload_workflows``) via the
shared ``_is_editor_or_build_artifact`` helper.

Regression coverage: a JupyterLab checkpoint copy of a workflow YAML in the
project's shared S3 prefix (e.g. workflows/.ipynb_checkpoints/<name>-checkpoint.yaml)
must NOT be matched as a DAG definition (discovery path), nor be resolved and
re-uploaded in place (resolve path). A checkpoint copy can share the same
top-level key / dag_id as the real file, so both loops must skip these keys.
"""

from unittest.mock import MagicMock, patch

from smus_cicd.commands.deploy import (
    _find_dag_files_in_s3,
    _is_editor_or_build_artifact,
    _resolve_and_upload_workflows,
)


def _obj(key):
    return {"Key": key}


def _target_config():
    """Minimal target_config whose deployment_configuration has one storage
    item with target_directory 'workflows', so search prefix is '<prefix>workflows/'."""
    storage_item = MagicMock()
    storage_item.target_directory = "workflows"
    tc = MagicMock()
    tc.deployment_configuration.storage = [storage_item]
    return tc


def _manifest():
    manifest = MagicMock()
    manifest.content.workflows = [{"workflowName": "silver_etl"}]
    return manifest


def test_find_dag_files_skips_ipynb_checkpoints():
    real = "shared/workflows/silver_etl.yaml"
    ckpt = "shared/workflows/.ipynb_checkpoints/silver_etl-checkpoint.yaml"

    s3 = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [{"Contents": [_obj(ckpt), _obj(real)]}]
    s3.get_paginator.return_value = paginator

    body = MagicMock()
    body.read.return_value = b"silver_etl:\n  dag_id: silver_etl\n"
    s3.get_object.return_value = {"Body": body}

    found = _find_dag_files_in_s3(
        s3, "my-bucket", "shared/", _manifest(), _target_config()
    )

    keys = [k for k, _ in found]
    assert real in keys
    assert ckpt not in keys
    # The checkpoint copy must never be downloaded/parsed either.
    fetched = [c.kwargs.get("Key") for c in s3.get_object.call_args_list]
    assert ckpt not in fetched


def test_is_editor_or_build_artifact():
    assert _is_editor_or_build_artifact(
        "shared/workflows/.ipynb_checkpoints/silver_etl-checkpoint.yaml"
    )
    assert _is_editor_or_build_artifact("shared/jobs/__pycache__/x.pyc")
    assert not _is_editor_or_build_artifact("shared/workflows/silver_etl.yaml")


def _resolve_target_config():
    tc = MagicMock()
    tc.project.name = "example_project"
    tc.name = "dev"
    tc.environment_variables = {}
    return tc


def _make_resolve_s3(pages, real_key, real_yaml):
    """Build an S3 mock whose paginator yields the given pages. download_file
    writes real_yaml for real_key and a non-workflow YAML for any other key, so
    only the real workflow is resolved/re-uploaded."""
    s3 = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [
        {"Contents": [_obj(k) for k in page]} for page in pages
    ]
    s3.get_paginator.return_value = paginator

    def _download_file(bucket, key, dest):
        # A plain (non-workflow) YAML: no top-level key with dag_id + tasks.
        content = real_yaml if key == real_key else "not_a_workflow:\n  foo: bar\n"
        with open(dest, "w") as fh:
            fh.write(content)

    s3.download_file.side_effect = _download_file
    return s3


def _run_resolve(s3):
    manifest = MagicMock()
    manifest.content.workflows = [{"workflowName": "silver_etl"}]
    resolver = MagicMock()
    resolver.resolve.side_effect = lambda content: content
    with patch("smus_cicd.commands.deploy.create_client", return_value=s3), patch(
        "smus_cicd.helpers.context_resolver.ContextResolver", return_value=resolver
    ):
        _resolve_and_upload_workflows(
            s3_bucket="my-bucket",
            s3_prefix="shared/",
            target_config=_resolve_target_config(),
            config={"region": "us-west-2"},
            stage_name="dev",
            project_info={"domain_id": "dzd-xxxxxxxxxxxxxx"},
            manifest=manifest,
        )


def test_resolve_and_upload_skips_ipynb_checkpoints():
    """A checkpoint copy must never be downloaded, resolved, or re-uploaded."""
    real = "shared/workflows/silver_etl.yaml"
    ckpt = "shared/workflows/.ipynb_checkpoints/silver_etl-checkpoint.yaml"
    real_yaml = "silver_etl:\n  dag_id: silver_etl\n  tasks: []\n"

    s3 = _make_resolve_s3([[ckpt, real]], real, real_yaml)
    _run_resolve(s3)

    downloaded = [c.args[1] for c in s3.download_file.call_args_list]
    uploaded = [c.args[2] for c in s3.upload_file.call_args_list]
    # Checkpoint copy is neither downloaded nor re-uploaded; real file round-trips.
    assert ckpt not in downloaded
    assert real in downloaded
    assert uploaded == [real]


def test_resolve_and_upload_paginates_beyond_first_page():
    """A workflow YAML on the second page must still be resolved and re-uploaded
    (regression: single list_objects_v2 call capped at 1000 objects)."""
    real = "shared/workflows/silver_etl.yaml"
    real_yaml = "silver_etl:\n  dag_id: silver_etl\n  tasks: []\n"

    # Page 1 holds only non-workflow YAMLs; the real workflow is on page 2.
    page1 = [f"shared/workflows/filler_{i}.yaml" for i in range(3)]
    page2 = [real]
    s3 = _make_resolve_s3([page1, page2], real, real_yaml)
    _run_resolve(s3)

    uploaded = [c.args[2] for c in s3.upload_file.call_args_list]
    # Only the real workflow is re-uploaded; page-1 fillers are not workflows.
    assert uploaded == [real]
