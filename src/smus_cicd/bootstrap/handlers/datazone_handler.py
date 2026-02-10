"""DataZone bootstrap action handler."""

from typing import Any, Dict

import boto3
import typer

from ...helpers import connections, datazone
from ...helpers.connection_creator import ConnectionCreator
from ...helpers.logger import get_logger
from ..models import BootstrapAction

logger = get_logger("bootstrap.handlers.datazone")


def handle_datazone_action(
    action: BootstrapAction, context: Dict[str, Any]
) -> Dict[str, Any]:
    """Handle DataZone/Project actions."""
    service, api = action.type.split(".", 1)

    if api == "create_environment":
        return create_environment(action, context)
    elif api == "create_connection":
        return create_connection(action, context)
    else:
        raise ValueError(f"Unknown DataZone/Project action: {api}")


def create_environment(
    action: BootstrapAction, context: Dict[str, Any]
) -> Dict[str, Any]:
    """Create DataZone environment."""
    logger.info("Creating DataZone environment")

    # TODO: Implement environment creation
    # This will use existing logic from initialization_handler.py

    return {"action": "project.create_environment", "status": "not_implemented"}


def create_connection(
    action: BootstrapAction, context: Dict[str, Any]
) -> Dict[str, Any]:
    """Create or update DataZone connection (idempotent)."""
    logger.info("Creating DataZone connection")

    # Extract context
    config = context.get("config")
    metadata = context.get("metadata", {})

    # Get connection parameters
    params = action.parameters
    name = params.get("name")
    connection_type = params.get("connection_type")
    properties = params.get("properties", {})

    if not name or not connection_type:
        raise ValueError("Connection name and type are required")

    # Get project info from metadata
    project_info = metadata.get("project_info", {})
    project_id = project_info.get("project_id")
    domain_id = project_info.get("domain_id")
    region = config.get("region")

    if not project_id or not domain_id:
        raise ValueError("Project info not available for connection creation")

    # Get project environments
    environments = datazone.get_project_environments(project_id, domain_id, region)
    if not environments:
        raise ValueError(f"No environments found for project {project_id}")

    # Use first environment for creation
    environment_id = environments[0].get("id")
    datazone_client = boto3.client("datazone", region_name=region)

    # Create internal client for WORKFLOWS_SERVERLESS connections
    internal_client = None
    if connection_type == "WORKFLOWS_SERVERLESS":
        internal_client = boto3.client("datazone-internal", region_name=region)

    # Check if connection already exists using the connections helper
    # This properly checks both project-level and all environment-level connections
    existing_connections = connections.get_project_connections(
        project_id, domain_id, region
    )

    # Check if helper returned an error
    if "error" in existing_connections:
        logger.warning(f"Failed to list connections: {existing_connections['error']}")
        existing_connections = {}

    logger.info(
        f"DEBUG: get_project_connections returned {len(existing_connections)} connections"
    )
    logger.info(f"DEBUG: Connection names: {list(existing_connections.keys())}")

    existing_connection = None
    if name in existing_connections:
        conn_info = existing_connections[name]
        connection_id = conn_info.get("connectionId")

        logger.info(f"DEBUG: Found connection '{name}' with ID {connection_id}")

        if connection_id:
            # Get full connection details to find its environment
            # Use internal client for WORKFLOWS_SERVERLESS connections
            try:
                client = internal_client if internal_client else datazone_client
                detail = client.get_connection(
                    domainIdentifier=domain_id, identifier=connection_id
                )
                existing_connection = detail
                # Use the environment where the connection exists
                if detail.get("environmentId"):
                    environment_id = detail["environmentId"]
                    logger.info(
                        f"DEBUG: Connection found in environment {environment_id}"
                    )
            except Exception as e:
                logger.warning(f"Failed to get connection details: {e}")
    else:
        logger.info(f"DEBUG: Connection '{name}' NOT found in existing connections")

    # Build desired properties
    creator = ConnectionCreator(domain_id=domain_id, region=region)
    desired_props = creator._build_connection_props(connection_type, **properties)

    if existing_connection:
        connection_id = existing_connection["connectionId"]
        typer.echo(f"🔍 Connection '{name}' exists: {connection_id}")

        # We already have full connection details from get_connection above
        current_props = existing_connection.get("props", {})

        # Compare properties
        if current_props == desired_props:
            typer.echo(f"✓ Connection '{name}' unchanged")
            return {
                "action": "datazone.create_connection",
                "status": "unchanged",
                "connection_id": connection_id,
            }

        # Update connection with new properties using ConnectionCreator
        try:
            connection_id = creator.update_connection(
                connection_id=connection_id,
                name=name,
                connection_type=connection_type,
                props=desired_props,
                environment_id=environment_id,
            )
            return {
                "action": "datazone.create_connection",
                "status": "updated",
                "connection_id": connection_id,
            }
        except Exception as e:
            typer.echo(f"❌ Failed to update connection '{name}': {e}")
            raise

    # Create new connection (either new or MLflow recreate)
    if not existing_connection:
        typer.echo(
            f"🔗 Creating {connection_type} connection '{name}' in environment {environment_id}"
        )

        try:
            connection_id = creator.create_connection(
                environment_id=environment_id,
                name=name,
                connection_type=connection_type,
                **properties,
            )

            typer.echo(f"✅ Connection '{name}' created: {connection_id}")
            return {
                "action": "datazone.create_connection",
                "status": "created",
                "connection_id": connection_id,
            }

        except Exception as e:
            typer.echo(f"❌ Failed to create connection '{name}': {e}")
            raise
