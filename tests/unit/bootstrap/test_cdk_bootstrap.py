"""Unit tests for CDK bootstrap handler."""

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from smus_cicd.bootstrap import BootstrapAction, executor, registry
from smus_cicd.bootstrap.handlers.cdk_handler import (
    _build_cdk_context,
    _build_env_vars,
    _resolve_app_path,
    deploy_cdk_stack,
    destroy_cdk_stack,
    diff_cdk_stack,
    handle_cdk_action,
    synth_cdk_stack,
)


class TestCdkHandlerRegistration:
    """Test CDK handler is registered in the action registry."""

    def test_cdk_handler_registered(self):
        """Test that cdk handler is registered."""
        handler = registry.get_handler("cdk.deploy")
        assert handler is not None

    def test_cdk_action_routes_to_handler(self):
        """Test that cdk.* actions route to the CDK handler."""
        handler = registry.get_handler("cdk.synth")
        assert handler is not None


class TestHandleCdkAction:
    """Test handle_cdk_action routing."""

    @patch("smus_cicd.bootstrap.handlers.cdk_handler.deploy_cdk_stack")
    def test_routes_deploy(self, mock_deploy):
        """Test routing to deploy."""
        mock_deploy.return_value = {"success": True}
        action = BootstrapAction(type="cdk.deploy", parameters={"appPath": "/app"})
        handle_cdk_action(action, {})
        mock_deploy.assert_called_once()

    @patch("smus_cicd.bootstrap.handlers.cdk_handler.destroy_cdk_stack")
    def test_routes_destroy(self, mock_destroy):
        """Test routing to destroy."""
        mock_destroy.return_value = {"success": True}
        action = BootstrapAction(type="cdk.destroy", parameters={"appPath": "/app"})
        handle_cdk_action(action, {})
        mock_destroy.assert_called_once()

    @patch("smus_cicd.bootstrap.handlers.cdk_handler.synth_cdk_stack")
    def test_routes_synth(self, mock_synth):
        """Test routing to synth."""
        mock_synth.return_value = {"success": True}
        action = BootstrapAction(type="cdk.synth", parameters={"appPath": "/app"})
        handle_cdk_action(action, {})
        mock_synth.assert_called_once()

    @patch("smus_cicd.bootstrap.handlers.cdk_handler.diff_cdk_stack")
    def test_routes_diff(self, mock_diff):
        """Test routing to diff."""
        mock_diff.return_value = {"success": True}
        action = BootstrapAction(type="cdk.diff", parameters={"appPath": "/app"})
        handle_cdk_action(action, {})
        mock_diff.assert_called_once()

    def test_unknown_action_raises(self):
        """Test unknown CDK action raises ValueError."""
        action = BootstrapAction(type="cdk.unknown", parameters={"appPath": "/app"})
        with pytest.raises(ValueError, match="Unknown CDK action: unknown"):
            handle_cdk_action(action, {})


class TestResolveAppPath:
    """Test _resolve_app_path."""

    def test_missing_app_path_raises(self):
        """Test missing appPath raises ValueError."""
        action = BootstrapAction(type="cdk.deploy", parameters={})
        with pytest.raises(ValueError, match="appPath is required"):
            _resolve_app_path(action, {})

    def test_absolute_path(self):
        """Test absolute path is used as-is."""
        with tempfile.TemporaryDirectory() as tmpdir:
            action = BootstrapAction(
                type="cdk.deploy", parameters={"appPath": tmpdir}
            )
            result = _resolve_app_path(action, {})
            assert result == tmpdir

    def test_relative_path_resolved_against_manifest(self):
        """Test relative path is resolved against manifest directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a subdirectory for the CDK app
            cdk_dir = os.path.join(tmpdir, "infra")
            os.makedirs(cdk_dir)

            manifest = MagicMock()
            manifest._file_path = os.path.join(tmpdir, "manifest.yaml")

            action = BootstrapAction(
                type="cdk.deploy", parameters={"appPath": "infra"}
            )
            context = {"manifest": manifest}

            result = _resolve_app_path(action, context)
            assert result == cdk_dir

    def test_nonexistent_path_raises(self):
        """Test non-existent path raises ValueError."""
        action = BootstrapAction(
            type="cdk.deploy", parameters={"appPath": "/nonexistent/path"}
        )
        with pytest.raises(ValueError, match="does not exist"):
            _resolve_app_path(action, {})


class TestBuildCdkContext:
    """Test _build_cdk_context."""

    def test_merges_action_context(self):
        """Test action context values are included."""
        action = BootstrapAction(
            type="cdk.deploy",
            parameters={"context": {"myKey": "myValue"}},
        )
        result = _build_cdk_context(action, {})
        assert result["myKey"] == "myValue"

    def test_injects_deployment_context(self):
        """Test deployment context values are injected."""
        action = BootstrapAction(type="cdk.deploy", parameters={})
        context = {
            "stage_name": "prod",
            "project_name": "my-project",
            "region": "us-west-2",
            "domain_id": "d-abc123",
        }

        result = _build_cdk_context(action, context)

        assert result["stage"] == "prod"
        assert result["projectName"] == "my-project"
        assert result["region"] == "us-west-2"
        assert result["domainId"] == "d-abc123"

    def test_action_context_takes_precedence(self):
        """Test explicit action context overrides deployment context."""
        action = BootstrapAction(
            type="cdk.deploy",
            parameters={"context": {"stage": "custom-stage"}},
        )
        context = {"stage_name": "prod"}

        result = _build_cdk_context(action, context)
        assert result["stage"] == "custom-stage"


class TestBuildEnvVars:
    """Test _build_env_vars."""

    def test_action_env_vars(self):
        """Test action environment variables."""
        action = BootstrapAction(
            type="cdk.deploy",
            parameters={"environmentVariables": {"MY_VAR": "value"}},
        )
        result = _build_env_vars(action, {})
        assert result["MY_VAR"] == "value"

    def test_stage_env_vars_merged(self):
        """Test stage environment variables are merged."""
        action = BootstrapAction(type="cdk.deploy", parameters={})
        context = {"env_vars": {"STAGE_VAR": "stage_value"}}

        result = _build_env_vars(action, context)
        assert result["STAGE_VAR"] == "stage_value"

    def test_action_env_vars_take_precedence(self):
        """Test action env vars override stage env vars."""
        action = BootstrapAction(
            type="cdk.deploy",
            parameters={"environmentVariables": {"KEY": "action_value"}},
        )
        context = {"env_vars": {"KEY": "stage_value"}}

        result = _build_env_vars(action, context)
        assert result["KEY"] == "action_value"


class TestDeployCdkStack:
    """Test deploy_cdk_stack."""

    @patch("smus_cicd.bootstrap.handlers.cdk_handler.cdk.deploy_stack")
    def test_successful_deploy(self, mock_deploy):
        """Test successful CDK deploy."""
        mock_deploy.return_value = {
            "success": True,
            "stdout": "deployed",
            "stderr": "",
            "return_code": 0,
            "outputs": {"MyStack": {"BucketName": "my-bucket"}},
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            action = BootstrapAction(
                type="cdk.deploy",
                parameters={"appPath": tmpdir, "stackName": "MyStack"},
            )
            context = {"region": "us-east-1"}

            result = deploy_cdk_stack(action, context)

        assert result["success"] is True
        assert result["action"] == "cdk.deploy"
        assert result["stack_name"] == "MyStack"
        assert "outputs" in result

    @patch("smus_cicd.bootstrap.handlers.cdk_handler.cdk.deploy_stack")
    def test_failed_deploy_raises(self, mock_deploy):
        """Test failed CDK deploy raises exception."""
        mock_deploy.return_value = {
            "success": False,
            "stdout": "",
            "stderr": "Stack creation failed",
            "return_code": 1,
            "outputs": {},
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            action = BootstrapAction(
                type="cdk.deploy", parameters={"appPath": tmpdir}
            )
            context = {"region": "us-east-1"}

            with pytest.raises(Exception, match="CDK deploy failed"):
                deploy_cdk_stack(action, context)

    @patch("smus_cicd.bootstrap.handlers.cdk_handler.cdk.deploy_stack")
    def test_deploy_passes_context_and_env(self, mock_deploy):
        """Test deploy passes CDK context and env vars."""
        mock_deploy.return_value = {
            "success": True,
            "stdout": "",
            "stderr": "",
            "return_code": 0,
            "outputs": {},
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            action = BootstrapAction(
                type="cdk.deploy",
                parameters={
                    "appPath": tmpdir,
                    "context": {"env": "prod"},
                    "environmentVariables": {"CDK_NEW_BOOTSTRAP": "1"},
                },
            )
            context = {"region": "us-west-2", "stage_name": "prod"}

            deploy_cdk_stack(action, context)

        call_kwargs = mock_deploy.call_args[1]
        assert call_kwargs["region"] == "us-west-2"
        assert "env" in call_kwargs["context"]
        assert "stage" in call_kwargs["context"]
        assert call_kwargs["env_vars"]["CDK_NEW_BOOTSTRAP"] == "1"


class TestDestroyCdkStack:
    """Test destroy_cdk_stack."""

    @patch("smus_cicd.bootstrap.handlers.cdk_handler.cdk.destroy_stack")
    def test_successful_destroy(self, mock_destroy):
        """Test successful CDK destroy."""
        mock_destroy.return_value = {
            "success": True,
            "stdout": "",
            "stderr": "",
            "return_code": 0,
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            action = BootstrapAction(
                type="cdk.destroy",
                parameters={"appPath": tmpdir, "stackName": "MyStack"},
            )
            context = {"region": "us-east-1"}

            result = destroy_cdk_stack(action, context)

        assert result["success"] is True
        assert result["action"] == "cdk.destroy"

    @patch("smus_cicd.bootstrap.handlers.cdk_handler.cdk.destroy_stack")
    def test_failed_destroy_raises(self, mock_destroy):
        """Test failed CDK destroy raises exception."""
        mock_destroy.return_value = {
            "success": False,
            "stdout": "",
            "stderr": "Cannot delete stack",
            "return_code": 1,
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            action = BootstrapAction(
                type="cdk.destroy", parameters={"appPath": tmpdir}
            )

            with pytest.raises(Exception, match="CDK destroy failed"):
                destroy_cdk_stack(action, {})


class TestSynthCdkStack:
    """Test synth_cdk_stack."""

    @patch("smus_cicd.bootstrap.handlers.cdk_handler.cdk.synth_stack")
    def test_successful_synth(self, mock_synth):
        """Test successful CDK synth."""
        template = "Resources:\n  Bucket:\n    Type: AWS::S3::Bucket"
        mock_synth.return_value = {
            "success": True,
            "stdout": template,
            "stderr": "",
            "return_code": 0,
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            action = BootstrapAction(
                type="cdk.synth", parameters={"appPath": tmpdir}
            )

            result = synth_cdk_stack(action, {})

        assert result["success"] is True
        assert result["action"] == "cdk.synth"
        assert "Bucket" in result["template"]

    @patch("smus_cicd.bootstrap.handlers.cdk_handler.cdk.synth_stack")
    def test_failed_synth_raises(self, mock_synth):
        """Test failed CDK synth raises exception."""
        mock_synth.return_value = {
            "success": False,
            "stdout": "",
            "stderr": "Synthesis error",
            "return_code": 1,
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            action = BootstrapAction(
                type="cdk.synth", parameters={"appPath": tmpdir}
            )

            with pytest.raises(Exception, match="CDK synth failed"):
                synth_cdk_stack(action, {})


class TestDiffCdkStack:
    """Test diff_cdk_stack."""

    @patch("smus_cicd.bootstrap.handlers.cdk_handler.cdk.diff_stack")
    def test_diff_no_changes(self, mock_diff):
        """Test diff with no changes."""
        mock_diff.return_value = {
            "success": True,
            "stdout": "There were no differences",
            "stderr": "",
            "return_code": 0,
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            action = BootstrapAction(
                type="cdk.diff", parameters={"appPath": tmpdir}
            )

            result = diff_cdk_stack(action, {})

        assert result["success"] is True
        assert result["has_changes"] is False

    @patch("smus_cicd.bootstrap.handlers.cdk_handler.cdk.diff_stack")
    def test_diff_with_changes(self, mock_diff):
        """Test diff with changes (exit code 1 means differences found)."""
        mock_diff.return_value = {
            "success": False,
            "stdout": "[+] AWS::S3::Bucket NewBucket",
            "stderr": "",
            "return_code": 1,
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            action = BootstrapAction(
                type="cdk.diff", parameters={"appPath": tmpdir}
            )

            result = diff_cdk_stack(action, {})

        assert result["success"] is True
        assert result["has_changes"] is True
        assert "NewBucket" in result["diff_output"]


class TestCdkBootstrapIntegrationWithExecutor:
    """Test CDK actions work through the bootstrap executor."""

    @patch("smus_cicd.bootstrap.handlers.cdk_handler.cdk.deploy_stack")
    def test_executor_runs_cdk_deploy(self, mock_deploy):
        """Test CDK deploy runs through the executor pipeline."""
        mock_deploy.return_value = {
            "success": True,
            "stdout": "",
            "stderr": "",
            "return_code": 0,
            "outputs": {},
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            actions = [
                BootstrapAction(
                    type="cdk.deploy",
                    parameters={"appPath": tmpdir, "stackName": "TestStack"},
                )
            ]
            context = {"stage": "test", "region": "us-east-1"}

            results = executor.execute_actions(actions, context)

        assert len(results) == 1
        assert results[0]["status"] == "success"
        assert results[0]["result"]["action"] == "cdk.deploy"

    @patch("smus_cicd.bootstrap.handlers.cdk_handler.cdk.deploy_stack")
    def test_executor_stops_on_cdk_failure(self, mock_deploy):
        """Test executor stops when CDK deploy fails."""
        mock_deploy.return_value = {
            "success": False,
            "stdout": "",
            "stderr": "Deploy failed",
            "return_code": 1,
            "outputs": {},
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            actions = [
                BootstrapAction(
                    type="cdk.deploy", parameters={"appPath": tmpdir}
                ),
                BootstrapAction(
                    type="cli.print", parameters={"message": "Should not run"}
                ),
            ]
            context = {"stage": "test"}

            with pytest.raises(Exception, match="CDK deploy failed"):
                executor.execute_actions(actions, context)

    @patch("smus_cicd.bootstrap.handlers.cdk_handler.cdk.deploy_stack")
    def test_cdk_deploy_with_other_actions(self, mock_deploy):
        """Test CDK deploy works alongside other bootstrap actions."""
        mock_deploy.return_value = {
            "success": True,
            "stdout": "",
            "stderr": "",
            "return_code": 0,
            "outputs": {},
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            actions = [
                BootstrapAction(
                    type="cli.print",
                    parameters={"message": "Starting CDK deploy"},
                ),
                BootstrapAction(
                    type="cdk.deploy",
                    parameters={"appPath": tmpdir},
                ),
                BootstrapAction(
                    type="cli.print",
                    parameters={"message": "CDK deploy complete"},
                ),
            ]
            context = {"stage": "dev"}

            results = executor.execute_actions(actions, context)

        assert len(results) == 3
        assert all(r["status"] == "success" for r in results)
        assert results[0]["result"]["message"] == "Starting CDK deploy"
        assert results[1]["result"]["action"] == "cdk.deploy"
        assert results[2]["result"]["message"] == "CDK deploy complete"
