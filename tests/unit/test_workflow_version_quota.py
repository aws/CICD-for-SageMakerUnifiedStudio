"""Unit test for workflow version quota exceeded error handling."""

from unittest.mock import MagicMock, patch

import pytest

_PATCH_TARGET = "smus_cicd.helpers.airflow_serverless.create_airflow_serverless_client"
_WORKFLOW_ARN = "arn:aws:airflow-serverless:us-east-1:123:workflow/test-wf"
_CONFLICT = Exception("ConflictException: workflow already exists")
_QUOTA = Exception(
    "ServiceQuotaExceededException: Max workflows versions per workflow exceeded"
)


@patch(_PATCH_TARGET)
def test_create_workflow_quota_exceeded_raises_with_helpful_message(mock_factory, capsys):
    """ServiceQuotaExceededException: raises and prints actionable error with CLI commands."""
    client = MagicMock()
    client.create_workflow.side_effect = _CONFLICT
    client.update_workflow.side_effect = _QUOTA
    client.list_workflows.return_value = {
        "Workflows": [{"Name": "test-workflow", "WorkflowArn": _WORKFLOW_ARN}]
    }
    mock_factory.return_value = client

    from smus_cicd.helpers.airflow_serverless import create_workflow

    with pytest.raises(Exception):
        create_workflow(
            workflow_name="test-workflow",
            dag_s3_location="s3://bucket/key.yaml",
            role_arn="arn:aws:iam::123:role/role",
            region="us-east-1",
        )

    captured = capsys.readouterr()
    assert "version quota limit" in captured.out
    assert "delete-workflow" in captured.out
    # Workflow must NOT be deleted automatically
    client.delete_workflow.assert_not_called()


@patch(_PATCH_TARGET)
def test_create_workflow_sends_code_param_on_create(mock_factory):
    """When code_s3_location is provided, CreateWorkflow gets a Code S3Location."""
    client = MagicMock()
    client.create_workflow.return_value = {
        "WorkflowArn": _WORKFLOW_ARN,
        "WorkflowVersion": "1",
        "CreatedAt": "now",
        "RevisionId": "rev-1",
    }
    mock_factory.return_value = client

    from smus_cicd.helpers.airflow_serverless import create_workflow

    create_workflow(
        workflow_name="test-workflow",
        dag_s3_location="s3://bucket/dags/wf.yaml",
        role_arn="arn:aws:iam::123:role/role",
        region="us-east-1",
        code_s3_location="s3://bucket/code/my_package.zip",
    )

    _, kwargs = client.create_workflow.call_args
    assert kwargs["Code"] == {
        "S3Location": {"Bucket": "bucket", "ObjectKey": "code/my_package.zip"}
    }
    # Definition and code are distinct S3 objects.
    assert kwargs["DefinitionS3Location"] == {
        "Bucket": "bucket",
        "ObjectKey": "dags/wf.yaml",
    }


@patch(_PATCH_TARGET)
def test_create_workflow_omits_code_when_not_provided(mock_factory):
    """No code_s3_location => no Code key (DAGs using only AWS operators)."""
    client = MagicMock()
    client.create_workflow.return_value = {
        "WorkflowArn": _WORKFLOW_ARN,
        "WorkflowVersion": "1",
        "CreatedAt": "now",
        "RevisionId": "rev-1",
    }
    mock_factory.return_value = client

    from smus_cicd.helpers.airflow_serverless import create_workflow

    create_workflow(
        workflow_name="test-workflow",
        dag_s3_location="s3://bucket/dags/wf.yaml",
        role_arn="arn:aws:iam::123:role/role",
        region="us-east-1",
    )

    _, kwargs = client.create_workflow.call_args
    assert "Code" not in kwargs


@patch(_PATCH_TARGET)
def test_update_workflow_sends_code_param_on_conflict(mock_factory):
    """On ConflictException, UpdateWorkflow re-applies the Code S3Location."""
    client = MagicMock()
    client.create_workflow.side_effect = _CONFLICT
    client.list_workflows.return_value = {
        "Workflows": [{"Name": "test-workflow", "WorkflowArn": _WORKFLOW_ARN}]
    }
    client.update_workflow.return_value = {"WorkflowVersion": "2"}
    mock_factory.return_value = client

    from smus_cicd.helpers.airflow_serverless import create_workflow

    result = create_workflow(
        workflow_name="test-workflow",
        dag_s3_location="s3://bucket/dags/wf.yaml",
        role_arn="arn:aws:iam::123:role/role",
        region="us-east-1",
        code_s3_location="s3://bucket/code/run.sh",
    )

    assert result["updated"] is True
    _, kwargs = client.update_workflow.call_args
    assert kwargs["Code"] == {
        "S3Location": {"Bucket": "bucket", "ObjectKey": "code/run.sh"}
    }
