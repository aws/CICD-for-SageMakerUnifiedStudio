"""Unit tests for workflow.create bootstrap action."""

import pytest
from unittest.mock import MagicMock, patch

from smus_cicd.bootstrap.handlers.workflow_create_handler import handle_workflow_create
from smus_cicd.bootstrap.models import BootstrapAction


@pytest.fixture
def mock_action():
    """Mock bootstrap action."""
    action = MagicMock(spec=BootstrapAction)
    action.parameters = {}
    return action


@pytest.fixture
def mock_context():
    """Mock execution context."""
    manifest = MagicMock()
    manifest.application_name = "TestApp"
    manifest.content.workflows = [
        {
            "workflowName": "test_workflow",
            "connectionName": "default.workflow_serverless",
        }
    ]

    target_config = MagicMock()
    target_config.project.name = "test-project"
    target_config.domain.name = "test-domain"

    return {
        "manifest": manifest,
        "target_config": target_config,
        "config": {"region": "us-east-1", "stage_name": "test"},
        "metadata": {
            "project_info": {
                "project_id": "project-123",
                "domain_id": "domain-123",
            },
            "s3_bucket": "test-bucket",
            "s3_prefix": "test-prefix",
            "bundle_path": None,
        },
    }


def test_handle_workflow_create_no_workflows(mock_action):
    """Test workflow.create with no workflows in manifest."""
    manifest = MagicMock()
    manifest.content.workflows = None

    context = {
        "manifest": manifest,
        "target_config": MagicMock(),
        "config": {"region": "us-east-1"},
        "metadata": {},
    }

    result = handle_workflow_create(mock_action, context)

    assert result is True


def test_handle_workflow_create_missing_s3_location(mock_action, mock_context):
    """Test workflow.create fails without S3 location."""
    mock_context["metadata"] = {}  # No S3 location

    result = handle_workflow_create(mock_action, mock_context)

    assert result is False


def test_handle_workflow_create_missing_project_info(mock_action, mock_context):
    """Test workflow.create fails without project info."""
    mock_context["metadata"]["project_info"] = {}  # No project_id/domain_id

    result = handle_workflow_create(mock_action, mock_context)

    assert result is False


def test_handle_workflow_create_specific_workflow(mock_action, mock_context):
    """Test workflow.create with specific workflow name."""
    mock_action.parameters = {"workflowName": "test_workflow"}

    with patch("smus_cicd.helpers.datazone.get_project_id_by_name") as mock_get_id:
        mock_get_id.return_value = "test-project-id"

        with patch(
            "smus_cicd.helpers.datazone.get_domain_id_by_name"
        ) as mock_get_domain:
            mock_get_domain.return_value = "dzd-test123"

            with patch(
                "smus_cicd.helpers.datazone.get_project_environments"
            ) as mock_get_envs:
                mock_get_envs.return_value = [
                    {
                        "name": "ToolingLite",
                        "provisionedResources": [
                            {
                                "name": "userRoleArn",
                                "value": "arn:aws:iam::123:role/test",
                            }
                        ],
                    }
                ]

                with patch(
                    "smus_cicd.helpers.datazone.get_project_user_role_arn"
                ) as mock_get_role:
                    mock_get_role.return_value = "arn:aws:iam::123:role/test"

                    with patch(
                        "smus_cicd.helpers.connections.get_project_connections"
                    ) as mock_get_conns:
                        mock_get_conns.return_value = {}

                        with patch(
                            "smus_cicd.helpers.datazone.get_tooling_network_and_encryption_config"
                        ) as mock_tooling, patch(
                            "smus_cicd.helpers.datazone.is_idc_domain"
                        ) as mock_idc:
                            mock_tooling.return_value = {
                                "subnet_ids": [],
                                "security_group_ids": [],
                                "kms_key_id": None,
                            }
                            mock_idc.return_value = False

                            with patch(
                                "smus_cicd.commands.deploy._find_dag_files_in_s3"
                            ) as mock_find:
                                mock_find.return_value = []  # No DAG files

                                result = handle_workflow_create(
                                    mock_action, mock_context
                                )

                                assert result is True


def test_handle_workflow_create_passes_tooling_config(mock_action, mock_context):
    """Workflows inherit Tooling blueprint VPC + CMK config on create."""
    with patch(
        "smus_cicd.bootstrap.handlers.workflow_create_handler.datazone"
    ) as mock_dz, patch(
        "smus_cicd.bootstrap.handlers.workflow_create_handler.create_client"
    ), patch(
        "smus_cicd.commands.deploy._find_dag_files_in_s3"
    ) as mock_find, patch(
        "smus_cicd.commands.deploy._generate_workflow_name"
    ) as mock_name, patch(
        "smus_cicd.helpers.context_resolver.ContextResolver"
    ) as mock_resolver, patch(
        "smus_cicd.bootstrap.handlers.workflow_create_handler.airflow_serverless"
    ) as mock_airflow, patch(
        "tempfile.NamedTemporaryFile"
    ), patch(
        "os.path.exists", return_value=False
    ), patch(
        "builtins.open"
    ):

        mock_dz.get_project_user_role_arn.return_value = "arn:aws:iam::123:role/test"
        mock_dz.get_tooling_network_and_encryption_config.return_value = {
            "subnet_ids": ["subnet-1", "subnet-2"],
            "security_group_ids": ["sg-1"],
            "kms_key_id": "arn:aws:kms:us-east-1:123:key/abc",
        }
        mock_dz.is_idc_domain.return_value = False
        mock_find.return_value = [("s3-key.yaml", "test_workflow")]
        mock_name.return_value = "TestApp_test_project_test_workflow"
        mock_resolver.return_value.resolve.return_value = "resolved: yaml"
        mock_airflow.create_workflow.return_value = {
            "success": True,
            "workflow_arn": "arn:aws:airflow-serverless:us-east-1:123:workflow/w-abc",
        }
        mock_airflow.get_workflow_status.return_value = {
            "success": True,
            "status": "READY",
        }

        result = handle_workflow_create(mock_action, mock_context)

    assert result is True
    _, kwargs = mock_airflow.create_workflow.call_args
    assert kwargs["subnet_ids"] == ["subnet-1", "subnet-2"]
    assert kwargs["security_group_ids"] == ["sg-1"]
    assert kwargs["kms_key_id"] == "arn:aws:kms:us-east-1:123:key/abc"
    # IAM-based domain -> no explicit log group
    assert kwargs["log_group_name"] is None


def test_handle_workflow_create_idc_log_group(mock_action, mock_context):
    """IdC-based domains get a domain/project-namespaced log group."""
    with patch(
        "smus_cicd.bootstrap.handlers.workflow_create_handler.datazone"
    ) as mock_dz, patch(
        "smus_cicd.bootstrap.handlers.workflow_create_handler.create_client"
    ), patch(
        "smus_cicd.commands.deploy._find_dag_files_in_s3"
    ) as mock_find, patch(
        "smus_cicd.commands.deploy._generate_workflow_name"
    ) as mock_name, patch(
        "smus_cicd.helpers.context_resolver.ContextResolver"
    ) as mock_resolver, patch(
        "smus_cicd.bootstrap.handlers.workflow_create_handler.airflow_serverless"
    ) as mock_airflow, patch(
        "tempfile.NamedTemporaryFile"
    ), patch(
        "os.path.exists", return_value=False
    ), patch(
        "builtins.open"
    ):

        mock_dz.get_project_user_role_arn.return_value = "arn:aws:iam::123:role/test"
        mock_dz.get_tooling_network_and_encryption_config.return_value = {
            "subnet_ids": [],
            "security_group_ids": [],
            "kms_key_id": None,
        }
        mock_dz.is_idc_domain.return_value = True
        mock_find.return_value = [("s3-key.yaml", "test_workflow")]
        mock_name.return_value = "my_workflow"
        mock_resolver.return_value.resolve.return_value = "resolved: yaml"
        mock_airflow.create_workflow.return_value = {
            "success": True,
            "workflow_arn": "arn:aws:airflow-serverless:us-east-1:123:workflow/w-abc",
        }
        mock_airflow.get_workflow_status.return_value = {
            "success": True,
            "status": "READY",
        }

        result = handle_workflow_create(mock_action, mock_context)

    assert result is True
    _, kwargs = mock_airflow.create_workflow.call_args
    # domain_id/project_id come from mock_context metadata.project_info
    assert kwargs["log_group_name"] == (
        "/aws/mwaa-serverless/domain-123-project-123/my_workflow"
    )


def test_handle_workflow_create_role_lookup_failure(mock_action, mock_context):
    """Test workflow.create fails when project user role not found."""
    with patch(
        "smus_cicd.bootstrap.handlers.workflow_create_handler.datazone"
    ) as mock_dz:
        mock_dz.get_project_user_role_arn.return_value = None  # Role not found

        result = handle_workflow_create(mock_action, mock_context)

        # Should fail when role not found
        assert result is False

        # Verify it was called with correct parameters
        mock_dz.get_project_user_role_arn.assert_called_once_with(
            "test-project", "test-domain", "us-east-1"
        )


def test_handle_workflow_create_workflow_not_found(mock_action, mock_context):
    """Test workflow.create with non-existent workflow name."""
    mock_action.parameters = {"workflowName": "nonexistent_workflow"}

    result = handle_workflow_create(mock_action, mock_context)

    assert result is False


def test_handle_workflow_create_reserved_tag_key_is_hard_error(
    mock_action, mock_context
):
    """A custom tag key that collides with a reserved SMUS tag fails the deploy."""
    mock_action.parameters = {"tags": {"CostCenter": "1234", "STAGE": "override"}}

    result = handle_workflow_create(mock_action, mock_context)

    # Fails fast, before any workflow is created.
    assert result is False


def test_handle_workflow_create_non_dict_tags_is_hard_error(mock_action, mock_context):
    """Non-mapping 'tags' value fails the deploy."""
    mock_action.parameters = {"tags": ["not", "a", "dict"]}

    result = handle_workflow_create(mock_action, mock_context)

    assert result is False


def test_handle_workflow_create_merges_custom_tags(mock_action, mock_context):
    """Custom tags are merged with SMUS-managed tags and passed to create_workflow."""
    mock_action.parameters = {"tags": {"CostCenter": "1234", "Team": "analytics"}}

    with patch(
        "smus_cicd.bootstrap.handlers.workflow_create_handler.datazone"
    ) as mock_dz, patch(
        "smus_cicd.bootstrap.handlers.workflow_create_handler.create_client"
    ), patch(
        "smus_cicd.commands.deploy._find_dag_files_in_s3"
    ) as mock_find, patch(
        "smus_cicd.commands.deploy._generate_workflow_name"
    ) as mock_name, patch(
        "smus_cicd.helpers.context_resolver.ContextResolver"
    ) as mock_resolver, patch(
        "smus_cicd.bootstrap.handlers.workflow_create_handler.airflow_serverless"
    ) as mock_airflow, patch(
        "tempfile.NamedTemporaryFile"
    ), patch(
        "os.path.exists", return_value=False
    ), patch(
        "builtins.open"
    ):

        mock_dz.get_project_user_role_arn.return_value = "arn:aws:iam::123:role/test"
        mock_dz.get_tooling_network_and_encryption_config.return_value = {
            "subnet_ids": [],
            "security_group_ids": [],
            "kms_key_id": None,
        }
        mock_dz.is_idc_domain.return_value = False
        mock_find.return_value = [("s3-key.yaml", "test_workflow")]
        mock_name.return_value = "TestApp_test_project_test_workflow"
        mock_resolver.return_value.resolve.return_value = "resolved: yaml"
        mock_airflow.create_workflow.return_value = {
            "success": True,
            "workflow_arn": "arn:aws:airflow-serverless:us-east-1:123:workflow/w-abc",
        }
        mock_airflow.get_workflow_status.return_value = {
            "success": True,
            "status": "READY",
        }

        result = handle_workflow_create(mock_action, mock_context)

    assert result is True
    _, kwargs = mock_airflow.create_workflow.call_args
    tags = kwargs["tags"]
    # Custom tags present...
    assert tags["CostCenter"] == "1234"
    assert tags["Team"] == "analytics"
    # ...alongside the SMUS-managed tags.
    assert tags["CreatedBy"] == "SMUS-CICD"
    assert tags["AmazonDataZoneDomain"] == "domain-123"
    assert tags["AmazonDataZoneProject"] == "project-123"
