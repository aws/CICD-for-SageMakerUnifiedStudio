"""Unit tests for Tooling-blueprint-derived workflow network/encryption config.

Covers:
- datazone.get_tooling_network_and_encryption_config resolution + defaults
- airflow_serverless network/encryption configuration builders
- airflow_serverless.create_workflow passing the config to the API
"""

from unittest.mock import MagicMock, patch

from smus_cicd.helpers import airflow_serverless, datazone

# ---------------------------------------------------------------------------
# datazone.get_tooling_network_and_encryption_config
# ---------------------------------------------------------------------------


def _tooling_env(
    provisioned_resources=None, aws_account_id="111", aws_region="us-east-1"
):
    return {
        "id": "env-1",
        "awsAccountId": aws_account_id,
        "awsAccountRegion": aws_region,
        "provisionedResources": provisioned_resources or [],
    }


def test_tooling_config_uses_provisioned_resources_when_no_vpc_connection():
    """No domain VPC connection -> fall back to provisionedResources (privateSubnets CSV)."""
    env_detail = _tooling_env(
        provisioned_resources=[
            {"name": "userRoleArn", "value": "arn:aws:iam::123:role/test"},
            {"name": "privateSubnets", "value": "subnet-a,subnet-b"},
            {"name": "securityGroup", "value": "sg-1"},
            {"name": "kmsKeyArn", "value": "arn:aws:kms:us-east-1:123:key/abc"},
        ]
    )

    with patch.object(
        datazone, "get_default_tooling_environment", return_value=env_detail
    ), patch.object(datazone, "_get_domain_scoped_vpc_connection", return_value=None):
        result = datazone.get_tooling_network_and_encryption_config(
            "proj", "domain-1", "us-east-1"
        )

    assert result["subnet_ids"] == ["subnet-a", "subnet-b"]
    assert result["security_group_ids"] == ["sg-1"]
    assert result["kms_key_id"] == "arn:aws:kms:us-east-1:123:key/abc"


def test_tooling_config_prefers_domain_scoped_vpc_connection():
    """A matching domain VPC connection ("global VPC") wins over provisionedResources."""
    env_detail = _tooling_env(
        provisioned_resources=[
            {"name": "privateSubnets", "value": "subnet-old"},
            {"name": "securityGroup", "value": "sg-old"},
            {"name": "kmsKeyArn", "value": "arn:aws:kms:us-east-1:123:key/abc"},
        ]
    )
    vpc_props = {
        "vpcId": "vpc-1",
        "subnetIds": ["subnet-x", "subnet-y"],
        "securityGroupId": "sg-new",
    }

    with patch.object(
        datazone, "get_default_tooling_environment", return_value=env_detail
    ), patch.object(
        datazone, "_get_domain_scoped_vpc_connection", return_value=vpc_props
    ):
        result = datazone.get_tooling_network_and_encryption_config(
            "proj", "domain-1", "us-east-1"
        )

    assert result["subnet_ids"] == ["subnet-x", "subnet-y"]
    assert result["security_group_ids"] == ["sg-new"]
    assert result["kms_key_id"] == "arn:aws:kms:us-east-1:123:key/abc"


def test_tooling_config_defaults_when_nothing_configured():
    env_detail = _tooling_env(
        provisioned_resources=[
            {"name": "userRoleArn", "value": "arn:aws:iam::123:role/test"}
        ]
    )

    with patch.object(
        datazone, "get_default_tooling_environment", return_value=env_detail
    ), patch.object(datazone, "_get_domain_scoped_vpc_connection", return_value=None):
        result = datazone.get_tooling_network_and_encryption_config(
            "proj", "domain-1", "us-east-1"
        )

    assert result == {
        "subnet_ids": [],
        "security_group_ids": [],
        "kms_key_id": None,
    }


def test_tooling_config_defaults_when_no_tooling_env():
    with patch.object(datazone, "get_default_tooling_environment", return_value=None):
        result = datazone.get_tooling_network_and_encryption_config(
            "proj", "domain-1", "us-east-1"
        )

    assert result == {
        "subnet_ids": [],
        "security_group_ids": [],
        "kms_key_id": None,
    }


# ---------------------------------------------------------------------------
# datazone.get_default_tooling_environment resolution
# ---------------------------------------------------------------------------


def test_get_default_tooling_environment_via_managed_blueprint():
    client = MagicMock()
    client.list_environment_blueprints.return_value = {
        "items": [
            {"id": "bp-ga", "name": "Tooling.GA", "provider": "prov"},
            {"id": "bp-other", "name": "SomethingElse", "provider": "prov"},
        ]
    }
    client.list_environments.return_value = {
        "items": [{"id": "env-ga", "status": "ACTIVE"}]
    }
    client.get_environment.return_value = {"id": "env-ga", "provisionedResources": []}

    with patch.object(
        datazone, "get_project_id_by_name", return_value="proj-1"
    ), patch.object(datazone, "_get_datazone_client", return_value=client):
        env = datazone.get_default_tooling_environment("proj", "domain-1", "us-east-1")

    assert env == {"id": "env-ga", "provisionedResources": []}
    # Filtered to managed Tooling blueprints only (GA used, SomethingElse ignored).
    client.list_environments.assert_called_once()
    _, kwargs = client.list_environments.call_args
    assert kwargs["environmentBlueprintIdentifier"] == "bp-ga"
    client.get_environment.assert_called_once_with(
        domainIdentifier="domain-1", identifier="env-ga"
    )


def test_get_default_tooling_environment_iam_connection_fallback():
    """Custom blueprint occupies tooling slot -> resolve via default IAM connection."""
    client = MagicMock()
    client.list_environment_blueprints.return_value = {
        "items": []
    }  # no managed Tooling
    client.list_connections.return_value = {
        "items": [{"props": {"iamProperties": {"environmentId": "env-iam"}}}]
    }
    client.get_environment.return_value = {"id": "env-iam", "provisionedResources": []}

    with patch.object(
        datazone, "get_project_id_by_name", return_value="proj-1"
    ), patch.object(datazone, "_get_datazone_client", return_value=client):
        env = datazone.get_default_tooling_environment("proj", "domain-1", "us-east-1")

    assert env == {"id": "env-iam", "provisionedResources": []}
    client.get_environment.assert_called_once_with(
        domainIdentifier="domain-1", identifier="env-iam"
    )


# ---------------------------------------------------------------------------
# airflow_serverless config builders
# ---------------------------------------------------------------------------


def test_build_network_configuration_requires_both():
    assert airflow_serverless._build_network_configuration(["sg-1"], ["subnet-1"]) == {
        "SecurityGroupIds": ["sg-1"],
        "SubnetIds": ["subnet-1"],
    }
    # Missing one side -> omit entirely (default worker VPC)
    assert airflow_serverless._build_network_configuration(["sg-1"], None) == {}
    assert airflow_serverless._build_network_configuration(None, ["subnet-1"]) == {}
    assert airflow_serverless._build_network_configuration(None, None) == {}


def test_build_encryption_configuration():
    assert airflow_serverless._build_encryption_configuration(
        "arn:aws:kms:us-east-1:123:key/abc"
    ) == {
        "KmsKeyId": "arn:aws:kms:us-east-1:123:key/abc",
        "Type": "CUSTOMER_MANAGED_KEY",
    }
    assert airflow_serverless._build_encryption_configuration(None) == {}
    assert airflow_serverless._build_encryption_configuration("  ") == {}


# ---------------------------------------------------------------------------
# airflow_serverless.create_workflow passes config to the API
# ---------------------------------------------------------------------------


def _mock_client_create_success():
    client = MagicMock()
    client.create_workflow.return_value = {
        "WorkflowArn": "arn:aws:airflow-serverless:us-east-1:123:workflow/w-abc123",
        "WorkflowVersion": "v1",
        "CreatedAt": "2026-01-01T00:00:00Z",
        "RevisionId": "rev-1",
    }
    return client


def test_create_workflow_includes_network_and_encryption():
    client = _mock_client_create_success()
    with patch.object(
        airflow_serverless, "create_airflow_serverless_client", return_value=client
    ):
        airflow_serverless.create_workflow(
            workflow_name="wf",
            dag_s3_location="s3://bucket/key.yaml",
            role_arn="arn:aws:iam::123:role/exec",
            region="us-east-1",
            security_group_ids=["sg-1"],
            subnet_ids=["subnet-1"],
            kms_key_id="arn:aws:kms:us-east-1:123:key/abc",
        )

    _, kwargs = client.create_workflow.call_args
    assert kwargs["NetworkConfiguration"] == {
        "SecurityGroupIds": ["sg-1"],
        "SubnetIds": ["subnet-1"],
    }
    assert kwargs["EncryptionConfiguration"] == {
        "KmsKeyId": "arn:aws:kms:us-east-1:123:key/abc",
        "Type": "CUSTOMER_MANAGED_KEY",
    }


def test_create_workflow_omits_config_when_defaults():
    client = _mock_client_create_success()
    with patch.object(
        airflow_serverless, "create_airflow_serverless_client", return_value=client
    ):
        airflow_serverless.create_workflow(
            workflow_name="wf",
            dag_s3_location="s3://bucket/key.yaml",
            role_arn="arn:aws:iam::123:role/exec",
            region="us-east-1",
        )

    _, kwargs = client.create_workflow.call_args
    assert "NetworkConfiguration" not in kwargs
    assert "EncryptionConfiguration" not in kwargs
    assert "LoggingConfiguration" not in kwargs


def test_update_path_sends_network_and_logging_but_not_encryption():
    """Existing workflow (ConflictException) -> update sends network + logging.

    VPC and logging are mutable via UpdateWorkflow, so an existing workflow
    converges toward the Tooling blueprint on re-deploy. Encryption is immutable
    (UpdateWorkflow has no EncryptionConfiguration field), so the CMK is omitted
    to avoid a ValidationException.
    """
    client = MagicMock()
    # First create raises ConflictException so the update path is taken.
    client.create_workflow.side_effect = Exception("ConflictException: exists")
    client.list_workflows.return_value = {
        "Workflows": [
            {
                "Name": "wf",
                "WorkflowArn": "arn:aws:airflow-serverless:us-east-1:123:workflow/wf-abc123",
            }
        ]
    }
    client.update_workflow.return_value = {"WorkflowVersion": "v2"}

    with patch.object(
        airflow_serverless, "create_airflow_serverless_client", return_value=client
    ):
        result = airflow_serverless.create_workflow(
            workflow_name="wf",
            dag_s3_location="s3://bucket/key.yaml",
            role_arn="arn:aws:iam::123:role/exec",
            region="us-east-1",
            security_group_ids=["sg-1"],
            subnet_ids=["subnet-1"],
            kms_key_id="arn:aws:kms:us-east-1:123:key/abc",
            log_group_name="/aws/mwaa-serverless/dzd-1-proj-1/wf",
        )

    assert result["success"] is True
    assert result.get("updated") is True
    _, kwargs = client.update_workflow.call_args
    assert kwargs["NetworkConfiguration"] == {
        "SecurityGroupIds": ["sg-1"],
        "SubnetIds": ["subnet-1"],
    }
    assert kwargs["LoggingConfiguration"] == {
        "LogGroupName": "/aws/mwaa-serverless/dzd-1-proj-1/wf"
    }
    # Encryption is immutable on update -> must NOT be sent.
    assert "EncryptionConfiguration" not in kwargs


def test_create_workflow_sets_log_group_when_provided():
    client = _mock_client_create_success()
    with patch.object(
        airflow_serverless, "create_airflow_serverless_client", return_value=client
    ):
        airflow_serverless.create_workflow(
            workflow_name="wf",
            dag_s3_location="s3://bucket/key.yaml",
            role_arn="arn:aws:iam::123:role/exec",
            region="us-east-1",
            log_group_name="/aws/mwaa-serverless/dzd-1-proj-1/wf",
        )

    _, kwargs = client.create_workflow.call_args
    assert kwargs["LoggingConfiguration"] == {
        "LogGroupName": "/aws/mwaa-serverless/dzd-1-proj-1/wf"
    }


# ---------------------------------------------------------------------------
# datazone.is_idc_domain
# ---------------------------------------------------------------------------


def test_is_idc_domain_true_for_idc():
    client = MagicMock()
    client.get_domain.return_value = {
        "singleSignOn": {
            "type": "IAM_IDC",
            "idcInstanceArn": "arn:aws:sso:::instance/ssoins-abc",
        }
    }
    with patch.object(datazone, "_get_datazone_client", return_value=client):
        assert datazone.is_idc_domain("dzd-1", "us-east-1") is True


def test_is_idc_domain_false_for_iam():
    client = MagicMock()
    client.get_domain.return_value = {"singleSignOn": {"type": "DISABLED"}}
    with patch.object(datazone, "_get_datazone_client", return_value=client):
        assert datazone.is_idc_domain("dzd-1", "us-east-1") is False


def test_is_idc_domain_defaults_false_on_error():
    client = MagicMock()
    client.get_domain.side_effect = Exception("boom")
    with patch.object(datazone, "_get_datazone_client", return_value=client):
        assert datazone.is_idc_domain("dzd-1", "us-east-1") is False
