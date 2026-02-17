"""Unit tests for CDK helper."""

import json
import os
import subprocess
from unittest.mock import MagicMock, mock_open, patch

import pytest

from smus_cicd.helpers.cdk import (
    deploy_stack,
    destroy_stack,
    diff_stack,
    get_stack_outputs,
    run_cdk_command,
    synth_stack,
)


class TestRunCdkCommand:
    """Test run_cdk_command."""

    @patch("smus_cicd.helpers.cdk.subprocess.run")
    @patch("smus_cicd.helpers.cdk.boto3.client")
    def test_basic_deploy(self, mock_boto, mock_run):
        """Test basic CDK deploy command."""
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
        mock_boto.return_value = mock_sts

        mock_run.return_value = MagicMock(
            returncode=0, stdout="Stack deployed", stderr=""
        )

        result = run_cdk_command("deploy", "/path/to/app")

        assert result["success"] is True
        assert result["return_code"] == 0

        call_args = mock_run.call_args
        cmd = call_args[0][0]
        assert "cdk" in cmd
        assert "deploy" in cmd
        assert "--require-approval=never" in cmd

    @patch("smus_cicd.helpers.cdk.subprocess.run")
    @patch("smus_cicd.helpers.cdk.boto3.client")
    def test_deploy_with_stack_name(self, mock_boto, mock_run):
        """Test deploy with specific stack name."""
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
        mock_boto.return_value = mock_sts

        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        run_cdk_command("deploy", "/app", stack_name="MyStack")

        cmd = mock_run.call_args[0][0]
        assert "MyStack" in cmd

    @patch("smus_cicd.helpers.cdk.subprocess.run")
    @patch("smus_cicd.helpers.cdk.boto3.client")
    def test_destroy_has_force_flag(self, mock_boto, mock_run):
        """Test destroy includes --force."""
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
        mock_boto.return_value = mock_sts

        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        run_cdk_command("destroy", "/app")

        cmd = mock_run.call_args[0][0]
        assert "--force" in cmd

    @patch("smus_cicd.helpers.cdk.subprocess.run")
    @patch("smus_cicd.helpers.cdk.boto3.client")
    def test_context_values(self, mock_boto, mock_run):
        """Test CDK context values are passed correctly."""
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
        mock_boto.return_value = mock_sts

        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        run_cdk_command(
            "deploy", "/app", context={"stage": "prod", "key": "value"}
        )

        cmd = mock_run.call_args[0][0]
        assert "-c" in cmd
        assert "stage=prod" in cmd
        assert "key=value" in cmd

    @patch("smus_cicd.helpers.cdk.subprocess.run")
    @patch("smus_cicd.helpers.cdk.boto3.client")
    def test_region_set_in_env(self, mock_boto, mock_run):
        """Test region is set in subprocess environment."""
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
        mock_boto.return_value = mock_sts

        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        run_cdk_command("synth", "/app", region="us-west-2")

        env = mock_run.call_args[1]["env"]
        assert env["AWS_DEFAULT_REGION"] == "us-west-2"
        assert env["CDK_DEFAULT_REGION"] == "us-west-2"

    @patch("smus_cicd.helpers.cdk.subprocess.run")
    @patch("smus_cicd.helpers.cdk.boto3.client")
    def test_failure_returns_error(self, mock_boto, mock_run):
        """Test failed command returns error info."""
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
        mock_boto.return_value = mock_sts

        mock_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="Error: stack failed"
        )

        result = run_cdk_command("deploy", "/app")

        assert result["success"] is False
        assert result["return_code"] == 1
        assert "stack failed" in result["stderr"]

    @patch("smus_cicd.helpers.cdk.subprocess.run")
    @patch("smus_cicd.helpers.cdk.boto3.client")
    def test_cdk_not_found(self, mock_boto, mock_run):
        """Test handling when CDK CLI is not installed."""
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
        mock_boto.return_value = mock_sts

        mock_run.side_effect = FileNotFoundError()

        result = run_cdk_command("deploy", "/app")

        assert result["success"] is False
        assert "CDK CLI not found" in result["stderr"]

    @patch("smus_cicd.helpers.cdk.subprocess.run")
    @patch("smus_cicd.helpers.cdk.boto3.client")
    def test_timeout(self, mock_boto, mock_run):
        """Test command timeout handling."""
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
        mock_boto.return_value = mock_sts

        mock_run.side_effect = subprocess.TimeoutExpired(cmd="cdk", timeout=1800)

        result = run_cdk_command("deploy", "/app")

        assert result["success"] is False
        assert "timed out" in result["stderr"]

    @patch("smus_cicd.helpers.cdk.subprocess.run")
    @patch("smus_cicd.helpers.cdk.boto3.client")
    def test_extra_args(self, mock_boto, mock_run):
        """Test extra CLI arguments are passed."""
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
        mock_boto.return_value = mock_sts

        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        run_cdk_command("deploy", "/app", extra_args=["--verbose", "--no-color"])

        cmd = mock_run.call_args[0][0]
        assert "--verbose" in cmd
        assert "--no-color" in cmd

    @patch("smus_cicd.helpers.cdk.subprocess.run")
    @patch("smus_cicd.helpers.cdk.boto3.client")
    def test_env_vars_passed(self, mock_boto, mock_run):
        """Test custom environment variables are passed."""
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
        mock_boto.return_value = mock_sts

        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        run_cdk_command("deploy", "/app", env_vars={"MY_VAR": "my_value"})

        env = mock_run.call_args[1]["env"]
        assert env["MY_VAR"] == "my_value"


class TestDeployStack:
    """Test deploy_stack."""

    @patch("smus_cicd.helpers.cdk.run_cdk_command")
    def test_deploy_with_outputs(self, mock_run):
        """Test deploy reads outputs file."""
        mock_run.return_value = {
            "success": True,
            "stdout": "",
            "stderr": "",
            "return_code": 0,
        }

        outputs_file = "/tmp/test-cdk-outputs.json"

        # Mock the outputs file
        with patch(
            "builtins.open", mock_open(read_data='{"MyStack": {"Key": "Value"}}')
        ):
            with patch("os.path.exists", return_value=True):
                result = deploy_stack("/app", outputs_file=outputs_file)

        assert result["success"] is True
        assert result["outputs"] == {"MyStack": {"Key": "Value"}}

    @patch("smus_cicd.helpers.cdk.run_cdk_command")
    def test_deploy_no_outputs_file(self, mock_run):
        """Test deploy without outputs file."""
        mock_run.return_value = {
            "success": True,
            "stdout": "",
            "stderr": "",
            "return_code": 0,
        }

        result = deploy_stack("/app")

        assert result["success"] is True
        assert result["outputs"] == {}


class TestDestroyStack:
    """Test destroy_stack."""

    @patch("smus_cicd.helpers.cdk.run_cdk_command")
    def test_destroy(self, mock_run):
        """Test destroy delegates to run_cdk_command."""
        mock_run.return_value = {
            "success": True,
            "stdout": "",
            "stderr": "",
            "return_code": 0,
        }

        result = destroy_stack("/app", stack_name="MyStack", region="us-east-1")

        assert result["success"] is True
        mock_run.assert_called_once_with(
            command="destroy",
            app_path="/app",
            stack_name="MyStack",
            region="us-east-1",
            context=None,
            env_vars=None,
        )


class TestSynthStack:
    """Test synth_stack."""

    @patch("smus_cicd.helpers.cdk.run_cdk_command")
    def test_synth(self, mock_run):
        """Test synth returns template."""
        mock_run.return_value = {
            "success": True,
            "stdout": "Resources:\n  MyBucket:\n    Type: AWS::S3::Bucket",
            "stderr": "",
            "return_code": 0,
        }

        result = synth_stack("/app")

        assert result["success"] is True
        assert "MyBucket" in result["stdout"]


class TestDiffStack:
    """Test diff_stack."""

    @patch("smus_cicd.helpers.cdk.run_cdk_command")
    def test_diff_no_changes(self, mock_run):
        """Test diff with no changes."""
        mock_run.return_value = {
            "success": True,
            "stdout": "There were no differences",
            "stderr": "",
            "return_code": 0,
        }

        result = diff_stack("/app")

        assert result["success"] is True


class TestGetStackOutputs:
    """Test get_stack_outputs."""

    @patch("smus_cicd.helpers.cdk.boto3.client")
    def test_get_outputs(self, mock_boto):
        """Test getting stack outputs."""
        mock_cf = MagicMock()
        mock_cf.describe_stacks.return_value = {
            "Stacks": [
                {
                    "Outputs": [
                        {"OutputKey": "BucketName", "OutputValue": "my-bucket"},
                        {"OutputKey": "TableArn", "OutputValue": "arn:aws:dynamodb:..."},
                    ]
                }
            ]
        }
        mock_boto.return_value = mock_cf

        outputs = get_stack_outputs("MyStack", "us-east-1")

        assert outputs["BucketName"] == "my-bucket"
        assert "TableArn" in outputs

    @patch("smus_cicd.helpers.cdk.boto3.client")
    def test_get_outputs_no_stack(self, mock_boto):
        """Test getting outputs for non-existent stack."""
        mock_cf = MagicMock()
        mock_cf.describe_stacks.return_value = {"Stacks": []}
        mock_boto.return_value = mock_cf

        outputs = get_stack_outputs("NonExistent", "us-east-1")

        assert outputs == {}

    @patch("smus_cicd.helpers.cdk.boto3.client")
    def test_get_outputs_error(self, mock_boto):
        """Test error handling when getting outputs."""
        mock_cf = MagicMock()
        mock_cf.describe_stacks.side_effect = Exception("Stack not found")
        mock_boto.return_value = mock_cf

        outputs = get_stack_outputs("BadStack", "us-east-1")

        assert outputs == {}
