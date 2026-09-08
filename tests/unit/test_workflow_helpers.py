"""Tests for workflow helper functions."""

import pytest
from unittest.mock import MagicMock, patch

from smus_cicd.helpers import airflow_serverless, datazone


class TestWorkflowNameGeneration:
    """Test workflow name generation helper."""

    def test_generate_workflow_name_basic(self):
        """Test basic workflow name generation."""
        result = airflow_serverless.generate_workflow_name(
            bundle_name="MyApp", project_name="test-project", dag_name="etl_pipeline"
        )
        assert result == "MyApp_test_project_etl_pipeline"

    def test_generate_workflow_name_with_hyphens(self):
        """Test workflow name generation with hyphens."""
        result = airflow_serverless.generate_workflow_name(
            bundle_name="my-app", project_name="test-project", dag_name="etl-pipeline"
        )
        assert result == "my_app_test_project_etl_pipeline"

    def test_generate_workflow_name_mixed(self):
        """Test workflow name generation with mixed characters."""
        result = airflow_serverless.generate_workflow_name(
            bundle_name="MyApp-v2",
            project_name="prod-analytics",
            dag_name="data-processing-dag",
        )
        assert result == "MyApp_v2_prod_analytics_data_processing_dag"


class TestWorkflowArnLookup:
    """Test workflow ARN lookup helper."""

    def test_find_workflow_arn_success(self):
        """Test successful workflow ARN lookup."""
        with patch("smus_cicd.helpers.airflow_serverless.list_workflows") as mock_list:
            mock_list.return_value = [
                {
                    "name": "app_project_dag1",
                    "workflow_arn": "arn:aws:airflow:us-east-1:123:workflow/app_project_dag1",
                },
                {
                    "name": "app_project_dag2",
                    "workflow_arn": "arn:aws:airflow:us-east-1:123:workflow/app_project_dag2",
                },
            ]

            result = airflow_serverless.find_workflow_arn(
                workflow_name="app_project_dag1", region="us-east-1"
            )

            assert result == "arn:aws:airflow:us-east-1:123:workflow/app_project_dag1"
            mock_list.assert_called_once_with(region="us-east-1", connection_info=None)

    def test_find_workflow_arn_not_found(self):
        """Test workflow ARN lookup when workflow not found."""
        with patch("smus_cicd.helpers.airflow_serverless.list_workflows") as mock_list:
            mock_list.return_value = [
                {
                    "name": "app_project_dag1",
                    "workflow_arn": "arn:aws:airflow:us-east-1:123:workflow/app_project_dag1",
                },
            ]

            with pytest.raises(Exception, match="Workflow 'nonexistent' not found"):
                airflow_serverless.find_workflow_arn(
                    workflow_name="nonexistent", region="us-east-1"
                )


class TestWorkflowStartVerification:
    """Test workflow start verification helper."""

    def test_start_workflow_run_verified_success_immediate(self):
        """Test workflow start with immediate success status."""
        with patch(
            "smus_cicd.helpers.airflow_serverless.start_workflow_run"
        ) as mock_start:
            mock_start.return_value = {
                "success": True,
                "run_id": "run123",
                "status": "STARTING",
                "workflow_arn": "arn:aws:airflow:us-east-1:123:workflow/test",
            }

            result = airflow_serverless.start_workflow_run_verified(
                workflow_arn="arn:aws:airflow:us-east-1:123:workflow/test",
                region="us-east-1",
                verify_started=True,
            )

            assert result["success"] is True
            assert result["run_id"] == "run123"
            assert result["status"] == "STARTING"

    def test_start_workflow_run_verified_with_retry(self):
        """Test workflow start with status verification retry."""
        with patch(
            "smus_cicd.helpers.airflow_serverless.start_workflow_run"
        ) as mock_start, patch(
            "smus_cicd.helpers.airflow_serverless.get_workflow_status"
        ) as mock_status, patch(
            "smus_cicd.helpers.airflow_serverless.time.sleep"
        ):

            mock_start.return_value = {
                "success": True,
                "run_id": "run123",
                "status": "READY",  # Not a running state
                "workflow_arn": "arn:aws:airflow:us-east-1:123:workflow/test",
            }

            mock_status.return_value = {
                "success": True,
                "status": "RUNNING",  # After retry, it's running
            }

            result = airflow_serverless.start_workflow_run_verified(
                workflow_arn="arn:aws:airflow:us-east-1:123:workflow/test",
                region="us-east-1",
                verify_started=True,
                wait_seconds=10,
            )

            assert result["success"] is True
            assert result["status"] == "RUNNING"
            mock_status.assert_called_once()

    def test_start_workflow_run_verified_failure(self):
        """Test workflow start verification failure."""
        with patch(
            "smus_cicd.helpers.airflow_serverless.start_workflow_run"
        ) as mock_start, patch(
            "smus_cicd.helpers.airflow_serverless.get_workflow_status"
        ) as mock_status, patch(
            "smus_cicd.helpers.airflow_serverless.time.sleep"
        ):

            mock_start.return_value = {
                "success": True,
                "run_id": "run123",
                "status": "READY",
                "workflow_arn": "arn:aws:airflow:us-east-1:123:workflow/test",
            }

            mock_status.return_value = {
                "success": True,
                "status": "READY",  # Still not running after retry
            }

            with pytest.raises(Exception, match="may not have actually started"):
                airflow_serverless.start_workflow_run_verified(
                    workflow_arn="arn:aws:airflow:us-east-1:123:workflow/test",
                    region="us-east-1",
                    verify_started=True,
                )

    def test_start_workflow_run_verified_no_verification(self):
        """Test workflow start without verification."""
        with patch(
            "smus_cicd.helpers.airflow_serverless.start_workflow_run"
        ) as mock_start:
            mock_start.return_value = {
                "success": True,
                "run_id": "run123",
                "status": "READY",
                "workflow_arn": "arn:aws:airflow:us-east-1:123:workflow/test",
            }

            result = airflow_serverless.start_workflow_run_verified(
                workflow_arn="arn:aws:airflow:us-east-1:123:workflow/test",
                region="us-east-1",
                verify_started=False,  # Skip verification
            )

            assert result["success"] is True
            assert result["status"] == "READY"  # Status not changed


class TestWorkflowLogs:
    """Test workflow logs helper."""

    def test_get_workflow_logs(self):
        """Test workflow logs retrieval."""
        with patch(
            "smus_cicd.helpers.airflow_serverless.get_cloudwatch_logs"
        ) as mock_logs, patch(
            "smus_cicd.helpers.airflow_serverless.create_airflow_serverless_client"
        ) as mock_client:
            mock_client.return_value.get_workflow.return_value = {
                "LoggingConfiguration": {
                    "LogGroupName": "/aws/mwaa-serverless/test_workflow/"
                }
            }
            mock_logs.return_value = [
                {
                    "timestamp": 1700000000000,
                    "log_stream_name": "dag/task1",
                    "message": "Task started",
                },
                {
                    "timestamp": 1700000001000,
                    "log_stream_name": "dag/task2",
                    "message": "Task completed",
                },
            ]

            result = airflow_serverless.get_workflow_logs(
                workflow_arn="arn:aws:airflow:us-east-1:123:workflow/test_workflow",
                run_id="run123",
                region="us-east-1",
                max_lines=100,
            )

            assert len(result) == 2
            assert "[dag/task1]" in result[0]
            assert "Task started" in result[0]
            assert "[dag/task2]" in result[1]
            assert "Task completed" in result[1]


class TestLogGroupNameHelpers:
    """The log-group naming scheme lives in one place; pin both formats."""

    def test_idc_log_group_name_format(self):
        assert (
            airflow_serverless.build_idc_log_group_name(
                "dzd-abc123", "proj456", "MyApp_wf"
            )
            == "/aws/mwaa-serverless/dzd-abc123-proj456/MyApp_wf"
        )

    def test_legacy_log_group_name_format(self):
        assert (
            airflow_serverless.legacy_log_group_name("MyApp_wf")
            == "/aws/mwaa-serverless/MyApp_wf/"
        )


class TestResolveWorkflowLogGroup:
    """Test log group resolution for IAM and IdC-based domains."""

    IDC_ARN = (
        "arn:aws:airflow-serverless:us-east-1:123456789012:workflow/"
        "MyApp_test_marketing_notebooks_workflow-ecFcfJbTRz"
    )

    def test_uses_service_reported_namespaced_log_group(self):
        """IdC-namespaced log group is read from the workflow's LoggingConfiguration."""
        namespaced = (
            "/aws/mwaa-serverless/dzd-4ulsbebrrnvn8n-3tdzaz70s0hfvr/"
            "MyApp_test_marketing_notebooks_workflow"
        )
        with patch(
            "smus_cicd.helpers.airflow_serverless.create_airflow_serverless_client"
        ) as mock_client:
            mock_client.return_value.get_workflow.return_value = {
                "LoggingConfiguration": {"LogGroupName": namespaced}
            }
            result = airflow_serverless.resolve_workflow_log_group(
                self.IDC_ARN, region="us-east-1"
            )
        assert result == namespaced

    def test_falls_back_when_no_logging_configuration(self):
        """When the service reports no log group, use the legacy workflow-name format."""
        with patch(
            "smus_cicd.helpers.airflow_serverless.create_airflow_serverless_client"
        ) as mock_client:
            mock_client.return_value.get_workflow.return_value = {}
            result = airflow_serverless.resolve_workflow_log_group(
                self.IDC_ARN, region="us-east-1"
            )
        assert result == (
            "/aws/mwaa-serverless/MyApp_test_marketing_notebooks_workflow-ecFcfJbTRz/"
        )

    def test_falls_back_on_api_error(self):
        """A GetWorkflow error is swallowed and the legacy format is used."""
        with patch(
            "smus_cicd.helpers.airflow_serverless.create_airflow_serverless_client"
        ) as mock_client:
            mock_client.return_value.get_workflow.side_effect = Exception("boom")
            result = airflow_serverless.resolve_workflow_log_group(
                self.IDC_ARN, region="us-east-1"
            )
        assert result == (
            "/aws/mwaa-serverless/MyApp_test_marketing_notebooks_workflow-ecFcfJbTRz/"
        )

    def test_blank_log_group_falls_back(self):
        """A blank/whitespace LogGroupName falls back to the legacy format."""
        with patch(
            "smus_cicd.helpers.airflow_serverless.create_airflow_serverless_client"
        ) as mock_client:
            mock_client.return_value.get_workflow.return_value = {
                "LoggingConfiguration": {"LogGroupName": "   "}
            }
            result = airflow_serverless.resolve_workflow_log_group(
                self.IDC_ARN, region="us-east-1"
            )
        assert result.startswith("/aws/mwaa-serverless/")
        assert result.endswith("-ecFcfJbTRz/")


class TestDataZoneConnectionDetection:
    """Test DataZone connection detection helpers."""

    def test_is_connection_serverless_airflow_true(self):
        """Test serverless Airflow connection detection (no MWAA ARN)."""
        with patch("smus_cicd.helpers.datazone._get_datazone_client") as mock_client:
            mock_dz = MagicMock()
            mock_client.return_value = mock_dz
            mock_dz.list_connections.return_value = {
                "items": [
                    {
                        "name": "default.workflow_serverless",
                        "type": "WORKFLOWS_MWAA",
                        "physicalEndpoints": [],  # No MWAA ARN = serverless
                    }
                ]
            }

            result = datazone.is_connection_serverless_airflow(
                connection_name="default.workflow_serverless",
                domain_id="dzd_123",
                project_id="prj_456",
                region="us-east-1",
            )

            assert result is True

    def test_is_connection_serverless_airflow_false(self):
        """Test MWAA connection detection (has MWAA ARN)."""
        with patch("smus_cicd.helpers.datazone._get_datazone_client") as mock_client:
            mock_dz = MagicMock()
            mock_client.return_value = mock_dz
            mock_dz.list_connections.return_value = {
                "items": [
                    {
                        "name": "default.workflow_mwaa",
                        "type": "WORKFLOWS_MWAA",
                        "physicalEndpoints": [
                            {
                                "glueConnection": "arn:aws:airflow:us-east-1:123:environment/my-mwaa"
                            }
                        ],
                    }
                ]
            }

            result = datazone.is_connection_serverless_airflow(
                connection_name="default.workflow_mwaa",
                domain_id="dzd_123",
                project_id="prj_456",
                region="us-east-1",
            )

            assert result is False

    def test_target_uses_serverless_airflow_true(self):
        """Test target uses serverless Airflow."""
        manifest = MagicMock()
        manifest.content.workflows = [{"connectionName": "default.workflow_serverless"}]

        target_config = MagicMock()
        target_config.domain.region = "us-east-1"
        target_config.domain.name = "test-domain"
        target_config.domain.tags = {}
        target_config.project.name = "test-project"

        with patch(
            "smus_cicd.helpers.datazone.resolve_domain_id"
        ) as mock_resolve, patch(
            "smus_cicd.helpers.datazone.get_project_id_by_name"
        ) as mock_project, patch(
            "smus_cicd.helpers.datazone.is_connection_serverless_airflow"
        ) as mock_check:

            mock_resolve.return_value = ("dzd_123", "test-domain")
            mock_project.return_value = "prj_456"
            mock_check.return_value = True

            result = datazone.target_uses_serverless_airflow(manifest, target_config)

            assert result is True
            mock_check.assert_called_once()

    def test_target_uses_serverless_airflow_no_workflows(self):
        """Test target with no workflows."""
        manifest = MagicMock()
        manifest.content.workflows = None

        target_config = MagicMock()

        result = datazone.target_uses_serverless_airflow(manifest, target_config)

        assert result is False


class TestListWorkflowRuns:
    """Test list_workflow_runs error handling."""

    def test_raises_on_api_error(self):
        """list_workflow_runs should raise instead of returning empty list on API error."""
        with patch(
            "smus_cicd.helpers.airflow_serverless.create_airflow_serverless_client"
        ) as mock_client:
            mock_airflow = MagicMock()
            mock_client.return_value = mock_airflow
            mock_airflow.list_workflow_runs.side_effect = Exception(
                "An error occurred (ValidationException) when calling the "
                "ListWorkflowRuns operation: Validation failed. "
                "Arn account must match caller account"
            )

            with pytest.raises(
                Exception, match="Arn account must match caller account"
            ):
                airflow_serverless.list_workflow_runs(
                    workflow_arn="arn:aws:airflow-serverless:ca-central-1:111111111111:workflow/test",
                    region="ca-central-1",
                )

    def test_returns_runs_on_success(self):
        """list_workflow_runs should return parsed runs on success."""
        with patch(
            "smus_cicd.helpers.airflow_serverless.create_airflow_serverless_client"
        ) as mock_client:
            mock_airflow = MagicMock()
            mock_client.return_value = mock_airflow
            mock_airflow.list_workflow_runs.return_value = {
                "WorkflowRuns": [
                    {
                        "RunId": "run-abc",
                        "WorkflowArn": "arn:aws:airflow-serverless:us-east-1:123:workflow/test",
                        "RunDetailSummary": {"Status": "SUCCEEDED"},
                    }
                ]
            }

            runs = airflow_serverless.list_workflow_runs(
                workflow_arn="arn:aws:airflow-serverless:us-east-1:123:workflow/test",
                region="us-east-1",
            )

            assert len(runs) == 1
            assert runs[0]["run_id"] == "run-abc"
            assert runs[0]["status"] == "SUCCEEDED"


class TestMonitorWorkflowLogsLive:
    """Test monitor_workflow_logs_live error handling and exit conditions."""

    def test_exits_after_max_consecutive_errors(self):
        """Should return error result after MAX_CONSECUTIVE_ERRORS consecutive failures."""
        with patch(
            "smus_cicd.helpers.airflow_serverless.list_workflow_runs"
        ) as mock_runs, patch("smus_cicd.helpers.airflow_serverless.time.sleep"):
            mock_runs.side_effect = Exception("Arn account must match caller account")

            result = airflow_serverless.monitor_workflow_logs_live(
                workflow_arn="arn:aws:airflow-serverless:ca-central-1:111111111111:workflow/test",
                region="ca-central-1",
            )

            assert result["success"] is False
            assert result["final_status"] == "ERROR"
            assert "Arn account must match caller account" in result["error"]
            # Should have been called exactly MAX_CONSECUTIVE_ERRORS times
            assert mock_runs.call_count == 10

    def test_resets_error_count_on_success(self):
        """Consecutive error count should reset after a successful poll."""
        completed_run = {
            "run_id": "run-1",
            "status": "SUCCEEDED",
            "ended_at": "2024-01-01T00:00:00",
            "started_at": "2024-01-01T00:00:00",
        }

        call_count = {"n": 0}

        def side_effect(*args, **kwargs):
            call_count["n"] += 1
            # Fail 9 times, then succeed on 10th (resets counter), then complete
            if call_count["n"] < 10:
                raise Exception("transient error")
            return [completed_run]

        with patch(
            "smus_cicd.helpers.airflow_serverless.list_workflow_runs",
            side_effect=side_effect,
        ), patch(
            "smus_cicd.helpers.airflow_serverless.get_cloudwatch_logs",
            return_value=[],
        ), patch(
            "smus_cicd.helpers.airflow_serverless.is_workflow_run_active",
            return_value=False,
        ), patch(
            "smus_cicd.helpers.airflow_serverless.time.sleep"
        ):
            result = airflow_serverless.monitor_workflow_logs_live(
                workflow_arn="arn:aws:airflow-serverless:us-east-1:123:workflow/test",
                region="us-east-1",
            )

            # 9 errors then 1 success — should complete, not error out
            assert (
                result["success"] is False
            )  # SUCCEEDED not in ["COMPLETED", "SUCCESS"]
            assert result["final_status"] == "SUCCEEDED"
