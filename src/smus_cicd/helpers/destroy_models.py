"""Data models and constants for the destroy command."""

from dataclasses import dataclass, field
from typing import Dict, List

# ---------------------------------------------------------------------------
# Resource types that destroy knows how to handle.
# A unit test asserts this matches DEPLOY_RESOURCE_TYPES from resource_types.py
# so that adding a new deploy capability without a destroy handler fails CI.
# ---------------------------------------------------------------------------
DESTROY_SUPPORTED_RESOURCE_TYPES = frozenset(
    {
        "s3_prefix",
        "airflow_workflow",
        "quicksight_dashboard",
        "quicksight_dataset",
        "quicksight_data_source",
        "glue_job",
        "datazone_connection",
        "datazone_project",
        "catalog_glossary",
        "catalog_glossary_term",
        "catalog_form_type",
        "catalog_asset_type",
        "catalog_asset",
        "catalog_data_product",
        # Notebook resources (via notebook sync)
        "notebook",
    }
)


@dataclass
class ResourceToDelete:
    """A single resource that will be deleted during the destroy phase."""

    resource_type: str
    resource_id: str
    stage: str
    metadata: dict = field(default_factory=dict)


@dataclass
class S3Target:
    """An S3 prefix to delete, resolved from a deployment_configuration entry."""

    bucket: str
    prefix: str
    connection_name: str


@dataclass
class ValidationResult:
    """Outcome of the validation phase for a single stage."""

    errors: List[str]
    warnings: List[str]
    resources: List[ResourceToDelete]
    active_workflow_runs: Dict[str, List[str]]


@dataclass
class ResourceResult:
    """Outcome of a single resource deletion attempt."""

    resource_type: str
    resource_id: str
    status: str  # "deleted" | "not_found" | "error" | "skipped"
    message: str
