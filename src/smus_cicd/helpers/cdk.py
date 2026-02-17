"""CDK helper for deploying CloudFormation stacks via AWS CDK."""

import json
import os
import subprocess
from typing import Any, Dict, List, Optional

import boto3

from .logger import get_logger

logger = get_logger("helpers.cdk")


def run_cdk_command(
    command: str,
    app_path: str,
    stack_name: Optional[str] = None,
    region: Optional[str] = None,
    context: Optional[Dict[str, str]] = None,
    extra_args: Optional[List[str]] = None,
    env_vars: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Run a CDK CLI command.

    Args:
        command: CDK command (deploy, destroy, synth, diff, list).
        app_path: Path to the CDK app directory.
        stack_name: Optional stack name to target.
        region: AWS region override.
        context: CDK context key-value pairs (-c key=value).
        extra_args: Additional CLI arguments.
        env_vars: Extra environment variables for the subprocess.

    Returns:
        Dict with success, stdout, stderr, and return_code.
    """
    cmd = ["cdk", command]

    if stack_name:
        cmd.append(stack_name)

    # --require-approval never for non-interactive CI/CD
    if command == "deploy":
        cmd.append("--require-approval=never")

    # Force destroy without confirmation
    if command == "destroy":
        cmd.append("--force")

    # Context values
    if context:
        for key, value in context.items():
            cmd.extend(["-c", f"{key}={value}"])

    if extra_args:
        cmd.extend(extra_args)

    # Build environment
    run_env = os.environ.copy()
    if region:
        run_env["AWS_DEFAULT_REGION"] = region
        run_env["CDK_DEFAULT_REGION"] = region
    if env_vars:
        run_env.update(env_vars)

    # Get AWS account for CDK_DEFAULT_ACCOUNT if not set
    if "CDK_DEFAULT_ACCOUNT" not in run_env:
        try:
            account = boto3.client("sts").get_caller_identity()["Account"]
            run_env["CDK_DEFAULT_ACCOUNT"] = account
        except Exception:
            pass

    logger.info(f"Running CDK command: {' '.join(cmd)}")
    logger.info(f"Working directory: {app_path}")

    try:
        result = subprocess.run(
            cmd,
            cwd=app_path,
            capture_output=True,
            text=True,
            env=run_env,
            timeout=1800,  # 30 min timeout
        )

        if result.returncode != 0:
            logger.error(f"CDK {command} failed: {result.stderr}")
        else:
            logger.info(f"CDK {command} succeeded")

        return {
            "success": result.returncode == 0,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "return_code": result.returncode,
        }

    except FileNotFoundError:
        msg = "CDK CLI not found. Install with: npm install -g aws-cdk"
        logger.error(msg)
        return {
            "success": False,
            "stdout": "",
            "stderr": msg,
            "return_code": -1,
        }
    except subprocess.TimeoutExpired:
        msg = f"CDK {command} timed out after 1800 seconds"
        logger.error(msg)
        return {
            "success": False,
            "stdout": "",
            "stderr": msg,
            "return_code": -1,
        }


def deploy_stack(
    app_path: str,
    stack_name: Optional[str] = None,
    region: Optional[str] = None,
    context: Optional[Dict[str, str]] = None,
    outputs_file: Optional[str] = None,
    extra_args: Optional[List[str]] = None,
    env_vars: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Deploy a CDK stack.

    Args:
        app_path: Path to the CDK app directory.
        stack_name: Optional stack name to deploy.
        region: AWS region.
        context: CDK context key-value pairs.
        outputs_file: Path to write stack outputs JSON.
        extra_args: Additional CLI arguments.
        env_vars: Extra environment variables.

    Returns:
        Dict with success status and stack outputs.
    """
    args = list(extra_args or [])
    if outputs_file:
        args.extend(["--outputs-file", outputs_file])

    result = run_cdk_command(
        command="deploy",
        app_path=app_path,
        stack_name=stack_name,
        region=region,
        context=context,
        extra_args=args,
        env_vars=env_vars,
    )

    # Parse outputs if file was written
    outputs = {}
    if outputs_file and os.path.exists(outputs_file):
        try:
            with open(outputs_file) as f:
                outputs = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Failed to read CDK outputs file: {e}")

    result["outputs"] = outputs
    return result


def destroy_stack(
    app_path: str,
    stack_name: Optional[str] = None,
    region: Optional[str] = None,
    context: Optional[Dict[str, str]] = None,
    env_vars: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Destroy a CDK stack.

    Args:
        app_path: Path to the CDK app directory.
        stack_name: Optional stack name to destroy.
        region: AWS region.
        context: CDK context key-value pairs.
        env_vars: Extra environment variables.

    Returns:
        Dict with success status.
    """
    return run_cdk_command(
        command="destroy",
        app_path=app_path,
        stack_name=stack_name,
        region=region,
        context=context,
        env_vars=env_vars,
    )


def synth_stack(
    app_path: str,
    stack_name: Optional[str] = None,
    region: Optional[str] = None,
    context: Optional[Dict[str, str]] = None,
    env_vars: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Synthesize a CDK stack (generate CloudFormation template).

    Args:
        app_path: Path to the CDK app directory.
        stack_name: Optional stack name to synthesize.
        region: AWS region.
        context: CDK context key-value pairs.
        env_vars: Extra environment variables.

    Returns:
        Dict with success status and synthesized template in stdout.
    """
    return run_cdk_command(
        command="synth",
        app_path=app_path,
        stack_name=stack_name,
        region=region,
        context=context,
        env_vars=env_vars,
    )


def diff_stack(
    app_path: str,
    stack_name: Optional[str] = None,
    region: Optional[str] = None,
    context: Optional[Dict[str, str]] = None,
    env_vars: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Show diff between deployed and local CDK stack.

    Args:
        app_path: Path to the CDK app directory.
        stack_name: Optional stack name.
        region: AWS region.
        context: CDK context key-value pairs.
        env_vars: Extra environment variables.

    Returns:
        Dict with success status and diff output.
    """
    return run_cdk_command(
        command="diff",
        app_path=app_path,
        stack_name=stack_name,
        region=region,
        context=context,
        env_vars=env_vars,
    )


def get_stack_outputs(stack_name: str, region: str) -> Dict[str, str]:
    """
    Get CloudFormation stack outputs for a deployed CDK stack.

    Args:
        stack_name: CloudFormation stack name.
        region: AWS region.

    Returns:
        Dict mapping output keys to values.
    """
    try:
        cf_client = boto3.client("cloudformation", region_name=region)
        response = cf_client.describe_stacks(StackName=stack_name)

        if not response.get("Stacks"):
            return {}

        outputs = {}
        for output in response["Stacks"][0].get("Outputs", []):
            outputs[output["OutputKey"]] = output["OutputValue"]

        return outputs

    except Exception as e:
        logger.error(f"Failed to get stack outputs for {stack_name}: {e}")
        return {}
