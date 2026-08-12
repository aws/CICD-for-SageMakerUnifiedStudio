"""
Shared registry of resource types managed by deploy and destroy.

IMPORTANT: When adding a new resource type to deploy (e.g. a new AWS service,
catalog resource kind, or bootstrap action that creates infrastructure):

  1. Add the resource type string to DEPLOY_RESOURCE_TYPES below
  2. Add the same string to DESTROY_SUPPORTED_RESOURCE_TYPES in
     src/smus_cicd/commands/destroy.py
  3. Implement the corresponding deletion logic in destroy.py

A unit test (TestDeployDestroyDrift) asserts these two sets are identical.
If they drift apart, CI will fail with a message telling you exactly which
types are missing.
"""

# Resource types that the deploy command (including bootstrap actions and
# workflow-created resources) can create in a target environment.
# Each entry is a resource_type string used by both deploy and destroy.
DEPLOY_RESOURCE_TYPES = frozenset(
    {
        # Core deployment resources
        "s3_prefix",
        "airflow_workflow",
        # QuickSight resources
        "quicksight_dashboard",
        "quicksight_dataset",
        "quicksight_data_source",
        # Workflow-created resources (via operator registry)
        "glue_job",
        # Bootstrap-created resources
        "datazone_connection",
        # DataZone project (conditional on project.create)
        "datazone_project",
        # Catalog resources (via catalog import)
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
