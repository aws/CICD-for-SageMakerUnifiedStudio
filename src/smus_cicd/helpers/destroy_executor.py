"""Destruction phase for the destroy command.

Executes resource deletion in the required dependency order for a single stage.
"""

from typing import List

from botocore.exceptions import ClientError
from rich.console import Console

from ..helpers import s3 as s3_helper
from ..helpers.airflow_serverless import delete_workflow
from ..helpers.boto3_client import create_client
from ..helpers.datazone import (
    delete_project,
    get_domain_from_target_config,
    get_project_id_by_name,
)
from ..helpers.operator_registry import OPERATOR_REGISTRY, ResourceNotFoundError
from .destroy_models import ResourceResult, ValidationResult

console = Console()
err_console = Console(stderr=True)


def _destroy_stage(
    stage_name: str,
    stage_config,
    manifest,
    validation_result: ValidationResult,
    region: str,
    output: str,
) -> List[ResourceResult]:
    """
    Execute deletion for one stage in the required dependency order.

    Order:
      a) Delete workflow-created resources (e.g. Glue jobs)
      b) Delete Airflow workflows
      c) Delete Bootstrap connections
      d) Delete QuickSight (dashboards → datasets → data sources)
      e) Delete S3 objects at declared prefixes
      f) Delete catalog resources (reverse dependency order)
      g) Delete DataZone project (only if project.create=True)
    """
    results: List[ResourceResult] = []
    effective_region = region or stage_config.domain.region
    is_json = output.upper() == "JSON"

    def _log(msg: str) -> None:
        if is_json:
            err_console.print(msg)
        else:
            console.print(msg)

    # -----------------------------------------------------------------------
    # Step a: Delete workflow-created resources (e.g. Glue jobs)
    # -----------------------------------------------------------------------
    _registry_resource_types = {
        entry["resource_type"] for entry in OPERATOR_REGISTRY.values()
    }
    _resource_type_to_registry = {
        entry["resource_type"]: entry for entry in OPERATOR_REGISTRY.values()
    }

    for resource in validation_result.resources:
        if resource.resource_type not in _registry_resource_types:
            continue

        operator_key = resource.metadata.get("operator", "")
        registry_entry = OPERATOR_REGISTRY.get(operator_key)
        if not registry_entry:
            registry_entry = _resource_type_to_registry.get(resource.resource_type)

        if not registry_entry:
            results.append(
                ResourceResult(
                    resource_type=resource.resource_type,
                    resource_id=resource.resource_id,
                    status="skipped",
                    message="No registry entry found",
                )
            )
            continue

        try:
            registry_entry["delete_fn"](resource.resource_id, effective_region)
            _log(f"  ✅ Deleted {resource.resource_type}: {resource.resource_id}")
            results.append(
                ResourceResult(
                    resource_type=resource.resource_type,
                    resource_id=resource.resource_id,
                    status="deleted",
                    message="Deleted successfully",
                )
            )
        except ResourceNotFoundError:
            _log(
                f"  [yellow]⚠️  {resource.resource_type} not found: {resource.resource_id}[/yellow]"
            )
            results.append(
                ResourceResult(
                    resource_type=resource.resource_type,
                    resource_id=resource.resource_id,
                    status="not_found",
                    message="Resource not found",
                )
            )
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "EntityNotFoundException":
                _log(
                    f"  [yellow]⚠️  {resource.resource_type} not found: {resource.resource_id}[/yellow]"
                )
                results.append(
                    ResourceResult(
                        resource_type=resource.resource_type,
                        resource_id=resource.resource_id,
                        status="not_found",
                        message="Resource not found",
                    )
                )
            else:
                _log(
                    f"  [red]❌ Error deleting {resource.resource_type} {resource.resource_id}: {e}[/red]"
                )
                results.append(
                    ResourceResult(
                        resource_type=resource.resource_type,
                        resource_id=resource.resource_id,
                        status="error",
                        message=str(e),
                    )
                )
        except Exception as e:
            _log(
                f"  [red]❌ Error deleting {resource.resource_type} {resource.resource_id}: {e}[/red]"
            )
            results.append(
                ResourceResult(
                    resource_type=resource.resource_type,
                    resource_id=resource.resource_id,
                    status="error",
                    message=str(e),
                )
            )

    # -----------------------------------------------------------------------
    # Step b: Delete Airflow workflows
    # -----------------------------------------------------------------------
    for resource in validation_result.resources:
        if resource.resource_type != "airflow_workflow":
            continue
        workflow_arn = resource.resource_id
        try:
            delete_workflow(workflow_arn, region=effective_region)
            _log(f"  ✅ Deleted Airflow workflow: {workflow_arn}")
            results.append(
                ResourceResult(
                    resource_type="airflow_workflow",
                    resource_id=workflow_arn,
                    status="deleted",
                    message="Deleted successfully",
                )
            )
        except Exception as e:
            err_str = str(e).lower()
            if "not found" in err_str or "resourcenotfound" in err_str:
                _log(
                    f"  [yellow]⚠️  Airflow workflow not found: {workflow_arn}[/yellow]"
                )
                results.append(
                    ResourceResult(
                        resource_type="airflow_workflow",
                        resource_id=workflow_arn,
                        status="not_found",
                        message="Workflow not found",
                    )
                )
            else:
                _log(
                    f"  [red]❌ Error deleting Airflow workflow {workflow_arn}: {e}[/red]"
                )
                results.append(
                    ResourceResult(
                        resource_type="airflow_workflow",
                        resource_id=workflow_arn,
                        status="error",
                        message=str(e),
                    )
                )

    # -----------------------------------------------------------------------
    # Step c: Delete Bootstrap_Connections
    # -----------------------------------------------------------------------
    for resource in validation_result.resources:
        if resource.resource_type != "datazone_connection":
            continue
        conn_name = resource.resource_id
        connection_id = resource.metadata.get("connection_id", "")
        domain_id = resource.metadata.get("domain_id", "")
        if not connection_id or not domain_id:
            _log(
                f"  [yellow]⚠️  Skipping connection '{conn_name}' — missing ID or domain[/yellow]"
            )
            results.append(
                ResourceResult(
                    resource_type="datazone_connection",
                    resource_id=conn_name,
                    status="skipped",
                    message="Missing connection ID or domain ID",
                )
            )
            continue
        try:
            dz_client = create_client("datazone", region=effective_region)
            dz_client.delete_connection(
                domainIdentifier=domain_id, identifier=connection_id
            )
            _log(f"  ✅ Deleted DataZone connection: {conn_name} ({connection_id})")
            results.append(
                ResourceResult(
                    resource_type="datazone_connection",
                    resource_id=conn_name,
                    status="deleted",
                    message="Deleted successfully",
                )
            )
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("ResourceNotFoundException", "EntityNotFoundException"):
                _log(f"  [yellow]⚠️  Connection not found: {conn_name}[/yellow]")
                results.append(
                    ResourceResult(
                        resource_type="datazone_connection",
                        resource_id=conn_name,
                        status="not_found",
                        message="Connection not found",
                    )
                )
            else:
                _log(f"  [red]❌ Error deleting connection '{conn_name}': {e}[/red]")
                results.append(
                    ResourceResult(
                        resource_type="datazone_connection",
                        resource_id=conn_name,
                        status="error",
                        message=str(e),
                    )
                )
        except Exception as e:
            _log(f"  [red]❌ Error deleting connection '{conn_name}': {e}[/red]")
            results.append(
                ResourceResult(
                    resource_type="datazone_connection",
                    resource_id=conn_name,
                    status="error",
                    message=str(e),
                )
            )

    # -----------------------------------------------------------------------
    # Step d: Delete QuickSight (dashboards → datasets → data sources)
    # -----------------------------------------------------------------------
    try:
        account_id = create_client(
            "sts", region=effective_region
        ).get_caller_identity()["Account"]
    except Exception:
        account_id = None

    qs_client = create_client("quicksight", region=effective_region)

    for qs_type in (
        "quicksight_dashboard",
        "quicksight_dataset",
        "quicksight_data_source",
    ):
        for resource in validation_result.resources:
            if resource.resource_type != qs_type:
                continue
            resource_id = resource.resource_id
            try:
                if qs_type == "quicksight_dashboard":
                    qs_client.delete_dashboard(
                        AwsAccountId=account_id, DashboardId=resource_id
                    )
                elif qs_type == "quicksight_dataset":
                    qs_client.delete_data_set(
                        AwsAccountId=account_id, DataSetId=resource_id
                    )
                elif qs_type == "quicksight_data_source":
                    qs_client.delete_data_source(
                        AwsAccountId=account_id, DataSourceId=resource_id
                    )
                _log(f"  ✅ Deleted {qs_type}: {resource_id}")
                results.append(
                    ResourceResult(
                        resource_type=qs_type,
                        resource_id=resource_id,
                        status="deleted",
                        message="Deleted successfully",
                    )
                )
            except ClientError as e:
                code = e.response["Error"]["Code"]
                if code == "ResourceNotFoundException":
                    _log(f"  [yellow]⚠️  {qs_type} not found: {resource_id}[/yellow]")
                    results.append(
                        ResourceResult(
                            resource_type=qs_type,
                            resource_id=resource_id,
                            status="not_found",
                            message="Resource not found",
                        )
                    )
                else:
                    _log(f"  [red]❌ Error deleting {qs_type} {resource_id}: {e}[/red]")
                    results.append(
                        ResourceResult(
                            resource_type=qs_type,
                            resource_id=resource_id,
                            status="error",
                            message=str(e),
                        )
                    )
            except Exception as e:
                _log(f"  [red]❌ Error deleting {qs_type} {resource_id}: {e}[/red]")
                results.append(
                    ResourceResult(
                        resource_type=qs_type,
                        resource_id=resource_id,
                        status="error",
                        message=str(e),
                    )
                )

    # -----------------------------------------------------------------------
    # Step e: Delete S3 objects
    # -----------------------------------------------------------------------
    for resource in validation_result.resources:
        if resource.resource_type != "s3_prefix":
            continue
        bucket = resource.metadata.get("bucket", "")
        prefix = resource.metadata.get("prefix", "")
        if not bucket:
            continue
        try:
            object_keys = [
                obj["Key"]
                for obj in s3_helper.list_objects(
                    bucket, prefix, region=effective_region
                )
            ]
            if not object_keys:
                _log(f"  [yellow]⚠️  S3 prefix empty: s3://{bucket}/{prefix}[/yellow]")
                results.append(
                    ResourceResult(
                        resource_type="s3_prefix",
                        resource_id=resource.resource_id,
                        status="not_found",
                        message="S3 prefix is empty or does not exist",
                    )
                )
                continue
            s3_helper.delete_objects(bucket, object_keys, region=effective_region)
            _log(f"  ✅ Deleted {len(object_keys)} objects from s3://{bucket}/{prefix}")
            results.append(
                ResourceResult(
                    resource_type="s3_prefix",
                    resource_id=resource.resource_id,
                    status="deleted",
                    message=f"Deleted {len(object_keys)} objects",
                )
            )
        except Exception as e:
            _log(
                f"  [red]❌ Error deleting S3 prefix s3://{bucket}/{prefix}: {e}[/red]"
            )
            results.append(
                ResourceResult(
                    resource_type="s3_prefix",
                    resource_id=resource.resource_id,
                    status="error",
                    message=str(e),
                )
            )

    # -----------------------------------------------------------------------
    # Step f: Delete catalog resources (reverse dependency order)
    # -----------------------------------------------------------------------
    CATALOG_DELETION_ORDER = [
        "catalog_data_product",
        "catalog_asset",
        "catalog_asset_type",
        "catalog_form_type",
        "catalog_glossary_term",
        "catalog_glossary",
    ]
    CATALOG_DELETE_API = {
        "catalog_data_product": lambda dz, did, rid: dz.delete_data_product(
            domainIdentifier=did, identifier=rid
        ),
        "catalog_asset": lambda dz, did, rid: dz.delete_asset(
            domainIdentifier=did, identifier=rid
        ),
        "catalog_asset_type": lambda dz, did, rid: dz.delete_asset_type(
            domainIdentifier=did, identifier=rid
        ),
        "catalog_form_type": lambda dz, did, rid: dz.delete_form_type(
            domainIdentifier=did, formTypeIdentifier=rid
        ),
        "catalog_glossary_term": lambda dz, did, rid: dz.delete_glossary_term(
            domainIdentifier=did, identifier=rid
        ),
        "catalog_glossary": lambda dz, did, rid: dz.delete_glossary(
            domainIdentifier=did, identifier=rid
        ),
    }

    for catalog_type in CATALOG_DELETION_ORDER:
        for resource in validation_result.resources:
            if resource.resource_type != catalog_type:
                continue
            resource_id = resource.resource_id
            domain_id_cat = resource.metadata.get("domain_id", "")
            resource_name = resource.metadata.get("name", resource_id)
            if not domain_id_cat:
                _log(
                    f"  [yellow]⚠️  Skipping {catalog_type} '{resource_name}' — missing domain ID[/yellow]"
                )
                results.append(
                    ResourceResult(
                        resource_type=catalog_type,
                        resource_id=resource_id,
                        status="skipped",
                        message="Missing domain ID",
                    )
                )
                continue
            try:
                dz_client = create_client("datazone", region=effective_region)
                CATALOG_DELETE_API[catalog_type](dz_client, domain_id_cat, resource_id)
                _log(f"  ✅ Deleted {catalog_type}: {resource_name} ({resource_id})")
                results.append(
                    ResourceResult(
                        resource_type=catalog_type,
                        resource_id=resource_id,
                        status="deleted",
                        message="Deleted successfully",
                    )
                )
            except ClientError as e:
                code = e.response["Error"]["Code"]
                if code in ("ResourceNotFoundException", "EntityNotFoundException"):
                    _log(
                        f"  [yellow]⚠️  {catalog_type} not found: {resource_name}[/yellow]"
                    )
                    results.append(
                        ResourceResult(
                            resource_type=catalog_type,
                            resource_id=resource_id,
                            status="not_found",
                            message="Resource not found",
                        )
                    )
                else:
                    _log(
                        f"  [red]❌ Error deleting {catalog_type} '{resource_name}': {e}[/red]"
                    )
                    results.append(
                        ResourceResult(
                            resource_type=catalog_type,
                            resource_id=resource_id,
                            status="error",
                            message=str(e),
                        )
                    )
            except Exception as e:
                _log(
                    f"  [red]❌ Error deleting {catalog_type} '{resource_name}': {e}[/red]"
                )
                results.append(
                    ResourceResult(
                        resource_type=catalog_type,
                        resource_id=resource_id,
                        status="error",
                        message=str(e),
                    )
                )

    # -----------------------------------------------------------------------
    # Step f.5: Delete notebooks (CI/CD-managed only, identified by metadata)
    # -----------------------------------------------------------------------
    for resource in validation_result.resources:
        if resource.resource_type != "notebook":
            continue
        notebook_id = resource.resource_id
        domain_id_nb = resource.metadata.get("domain_id", "")
        resource_name = resource.metadata.get("name", notebook_id)
        source_id = resource.metadata.get("source_notebook_id", "")

        if not domain_id_nb:
            _log(
                f"  [yellow]⚠️  Skipping notebook '{resource_name}' — missing domain ID[/yellow]"
            )
            results.append(
                ResourceResult(
                    resource_type="notebook",
                    resource_id=notebook_id,
                    status="skipped",
                    message="Missing domain ID",
                )
            )
            continue

        try:
            dz_client = create_client("datazone", region=effective_region)
            dz_client.delete_notebook(
                domainIdentifier=domain_id_nb,
                identifier=notebook_id,
            )
            _log(
                f"  ✅ Deleted notebook: {resource_name} ({notebook_id})"
                + (f" [source: {source_id}]" if source_id else "")
            )
            results.append(
                ResourceResult(
                    resource_type="notebook",
                    resource_id=notebook_id,
                    status="deleted",
                    message="Deleted successfully",
                )
            )
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("ResourceNotFoundException", "EntityNotFoundException"):
                _log(
                    f"  [yellow]⚠️  Notebook not found: {resource_name} ({notebook_id})[/yellow]"
                )
                results.append(
                    ResourceResult(
                        resource_type="notebook",
                        resource_id=notebook_id,
                        status="not_found",
                        message="Notebook not found",
                    )
                )
            else:
                _log(
                    f"  [red]❌ Error deleting notebook '{resource_name}' ({notebook_id}): {exc}[/red]"
                )
                results.append(
                    ResourceResult(
                        resource_type="notebook",
                        resource_id=notebook_id,
                        status="error",
                        message=str(exc),
                    )
                )
        except Exception as exc:
            _log(
                f"  [red]❌ Error deleting notebook '{resource_name}' ({notebook_id}): {exc}[/red]"
            )
            results.append(
                ResourceResult(
                    resource_type="notebook",
                    resource_id=notebook_id,
                    status="error",
                    message=str(exc),
                )
            )

    # -----------------------------------------------------------------------
    # Step g: Delete DataZone project (only if project.create=True)
    # -----------------------------------------------------------------------
    if stage_config.project.create is True:
        try:
            domain_id, domain_name = get_domain_from_target_config(
                stage_config, region=effective_region
            )
            project_id = get_project_id_by_name(
                stage_config.project.name, domain_id, effective_region
            )
            if not project_id:
                _log(
                    f"  [yellow]⚠️  DataZone project not found: '{stage_config.project.name}'[/yellow]"
                )
                results.append(
                    ResourceResult(
                        resource_type="datazone_project",
                        resource_id=stage_config.project.name,
                        status="not_found",
                        message="Project not found",
                    )
                )
            else:
                delete_project(domain_name, project_id, effective_region)
                _log(f"  ✅ Deleted DataZone project: {stage_config.project.name}")
                results.append(
                    ResourceResult(
                        resource_type="datazone_project",
                        resource_id=stage_config.project.name,
                        status="deleted",
                        message="Deleted successfully",
                    )
                )
        except Exception as e:
            err_str = str(e).lower()
            if "not found" in err_str or "resourcenotfound" in err_str:
                _log(
                    f"  [yellow]⚠️  DataZone project not found: '{stage_config.project.name}'[/yellow]"
                )
                results.append(
                    ResourceResult(
                        resource_type="datazone_project",
                        resource_id=stage_config.project.name,
                        status="not_found",
                        message="Project not found",
                    )
                )
            else:
                _log(
                    f"  [red]❌ Error deleting DataZone project '{stage_config.project.name}': {e}[/red]"
                )
                results.append(
                    ResourceResult(
                        resource_type="datazone_project",
                        resource_id=stage_config.project.name,
                        status="error",
                        message=str(e),
                    )
                )

    # Handle skipped resources from validation
    for resource in validation_result.resources:
        if resource.resource_type == "skipped":
            reason = resource.metadata.get("reason", "")
            _log(f"  [dim]⏭️  Skipped task '{resource.resource_id}': {reason}[/dim]")
            results.append(
                ResourceResult(
                    resource_type="skipped",
                    resource_id=resource.resource_id,
                    status="skipped",
                    message=reason,
                )
            )

    return results
