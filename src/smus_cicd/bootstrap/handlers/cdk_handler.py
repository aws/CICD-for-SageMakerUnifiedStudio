"""CDK bootstrap action handler."""

import os
import tempfile
from typing import Any, Dict

from ...helpers import cdk
from ...helpers.logger import get_logger
from ..models import BootstrapAction

logger = get_logger("bootstrap.handlers.cdk")


def handle_cdk_action(
    action: BootstrapAction, context: Dict[str, Any]
) -> Dict[str, Any]:
    """Handle CDK bootstrap actions."""
    _, api = action.type.split(".", 1)

    if api == "deploy":
        return deploy_cdk_stack(action, context)
    elif api == "destroy":
        return destroy_cdk_stack(action, context)
    elif api == "synth":
        return synth_cdk_stack(action, context)
    elif api == "diff":
        return diff_cdk_stack(action, context)
    else:
        raise ValueError(f"Unknown CDK action: {api}")


def _resolve_app_path(action: BootstrapAction, context: Dict[str, Any]) -> str:
    """Resolve the CDK app path from action parameters and context."""
    app_path = action.parameters.get("appPath")
    if not app_path:
        raise ValueError("appPath is required for CDK actions")

    # If relative, resolve against manifest directory
    if not os.path.isabs(app_path):
        manifest = context.get("manifest")
        if manifest and hasattr(manifest, "_file_path") and manifest._file_path:
            base_dir = os.path.dirname(os.path.abspath(manifest._file_path))
            app_path = os.path.join(base_dir, app_path)

    if not os.path.isdir(app_path):
        raise ValueError(f"CDK app path does not exist: {app_path}")

    return app_path


def _build_cdk_context(
    action: BootstrapAction, context: Dict[str, Any]
) -> Dict[str, str]:
    """Build CDK context values from action parameters and deployment context."""
    cdk_context = dict(action.parameters.get("context", {}))

    # Inject standard deployment context values
    stage_name = context.get("stage_name", context.get("stage", ""))
    project_name = context.get("project_name", "")
    region = context.get("region", "")
    domain_id = context.get("domain_id", "")

    if stage_name:
        cdk_context.setdefault("stage", stage_name)
    if project_name:
        cdk_context.setdefault("projectName", project_name)
    if region:
        cdk_context.setdefault("region", region)
    if domain_id:
        cdk_context.setdefault("domainId", domain_id)

    return cdk_context


def _build_env_vars(
    action: BootstrapAction, context: Dict[str, Any]
) -> Dict[str, str]:
    """Build environment variables for CDK subprocess."""
    env_vars = dict(action.parameters.get("environmentVariables", {}))

    # Merge stage environment variables
    stage_env = context.get("env_vars", {})
    for key, value in stage_env.items():
        env_vars.setdefault(key, str(value))

    return env_vars


def deploy_cdk_stack(
    action: BootstrapAction, context: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Deploy a CDK stack.

    Action parameters:
        appPath: Path to CDK app directory (required).
        stackName: Specific stack to deploy (optional, deploys all if omitted).
        context: CDK context key-value pairs (optional).
        environmentVariables: Extra env vars for CDK process (optional).
        extraArgs: Additional CDK CLI arguments (optional).
        outputsFile: Path to write stack outputs JSON (optional).
        region: AWS region override (optional).
    """
    app_path = _resolve_app_path(action, context)
    stack_name = action.parameters.get("stackName")
    region = action.parameters.get("region") or context.get("region")
    cdk_context = _build_cdk_context(action, context)
    env_vars = _build_env_vars(action, context)
    extra_args = action.parameters.get("extraArgs", [])

    # Default outputs file to temp location
    outputs_file = action.parameters.get("outputsFile")
    if not outputs_file:
        outputs_file = os.path.join(tempfile.gettempdir(), "cdk-outputs.json")

    logger.info(
        f"Bootstrap action: Deploying CDK stack "
        f"(app={app_path}, stack={stack_name or 'all'})"
    )

    result = cdk.deploy_stack(
        app_path=app_path,
        stack_name=stack_name,
        region=region,
        context=cdk_context,
        outputs_file=outputs_file,
        extra_args=extra_args,
        env_vars=env_vars,
    )

    if not result["success"]:
        raise Exception(
            f"CDK deploy failed (exit code {result['return_code']}): "
            f"{result['stderr']}"
        )

    logger.info("CDK deploy completed successfully")

    return {
        "action": "cdk.deploy",
        "stack_name": stack_name or "all",
        "app_path": app_path,
        "outputs": result.get("outputs", {}),
        "success": True,
    }


def destroy_cdk_stack(
    action: BootstrapAction, context: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Destroy a CDK stack.

    Action parameters:
        appPath: Path to CDK app directory (required).
        stackName: Specific stack to destroy (optional).
        context: CDK context key-value pairs (optional).
        environmentVariables: Extra env vars (optional).
        region: AWS region override (optional).
    """
    app_path = _resolve_app_path(action, context)
    stack_name = action.parameters.get("stackName")
    region = action.parameters.get("region") or context.get("region")
    cdk_context = _build_cdk_context(action, context)
    env_vars = _build_env_vars(action, context)

    logger.info(
        f"Bootstrap action: Destroying CDK stack "
        f"(app={app_path}, stack={stack_name or 'all'})"
    )

    result = cdk.destroy_stack(
        app_path=app_path,
        stack_name=stack_name,
        region=region,
        context=cdk_context,
        env_vars=env_vars,
    )

    if not result["success"]:
        raise Exception(
            f"CDK destroy failed (exit code {result['return_code']}): "
            f"{result['stderr']}"
        )

    logger.info("CDK destroy completed successfully")

    return {
        "action": "cdk.destroy",
        "stack_name": stack_name or "all",
        "app_path": app_path,
        "success": True,
    }


def synth_cdk_stack(
    action: BootstrapAction, context: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Synthesize a CDK stack (generate CloudFormation template).

    Action parameters:
        appPath: Path to CDK app directory (required).
        stackName: Specific stack to synthesize (optional).
        context: CDK context key-value pairs (optional).
        environmentVariables: Extra env vars (optional).
        region: AWS region override (optional).
    """
    app_path = _resolve_app_path(action, context)
    stack_name = action.parameters.get("stackName")
    region = action.parameters.get("region") or context.get("region")
    cdk_context = _build_cdk_context(action, context)
    env_vars = _build_env_vars(action, context)

    logger.info(
        f"Bootstrap action: Synthesizing CDK stack "
        f"(app={app_path}, stack={stack_name or 'all'})"
    )

    result = cdk.synth_stack(
        app_path=app_path,
        stack_name=stack_name,
        region=region,
        context=cdk_context,
        env_vars=env_vars,
    )

    if not result["success"]:
        raise Exception(
            f"CDK synth failed (exit code {result['return_code']}): "
            f"{result['stderr']}"
        )

    return {
        "action": "cdk.synth",
        "stack_name": stack_name or "all",
        "app_path": app_path,
        "template": result["stdout"],
        "success": True,
    }


def diff_cdk_stack(
    action: BootstrapAction, context: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Show diff for a CDK stack.

    Action parameters:
        appPath: Path to CDK app directory (required).
        stackName: Specific stack (optional).
        context: CDK context key-value pairs (optional).
        environmentVariables: Extra env vars (optional).
        region: AWS region override (optional).
    """
    app_path = _resolve_app_path(action, context)
    stack_name = action.parameters.get("stackName")
    region = action.parameters.get("region") or context.get("region")
    cdk_context = _build_cdk_context(action, context)
    env_vars = _build_env_vars(action, context)

    logger.info(
        f"Bootstrap action: Diffing CDK stack "
        f"(app={app_path}, stack={stack_name or 'all'})"
    )

    result = cdk.diff_stack(
        app_path=app_path,
        stack_name=stack_name,
        region=region,
        context=cdk_context,
        env_vars=env_vars,
    )

    # diff returns exit code 1 when there are differences — that's not a failure
    return {
        "action": "cdk.diff",
        "stack_name": stack_name or "all",
        "app_path": app_path,
        "has_changes": result["return_code"] == 1,
        "diff_output": result["stdout"],
        "success": True,
    }
