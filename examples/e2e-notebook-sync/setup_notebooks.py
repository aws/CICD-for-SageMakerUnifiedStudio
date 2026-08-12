#!/usr/bin/env python3
"""
Setup script for e2e-notebook-sync example.

Creates 4 notebooks in the DEV environment with varying configurations:
  1. data-prep-etl        — parameters + environmentConfiguration + description
  2. feature-engineering  — parameters only, no environmentConfiguration
  3. model-training       — environmentConfiguration only (pip packages)
  4. exploratory-analysis — minimal (name + short description only)

The first 3 are included in the manifest's notebook_ids filter to test
selective export/sync. The 4th (exploratory-analysis) is intentionally
excluded to verify that filtering works.

All operations are idempotent — safe to run multiple times.
Notebooks are created via the DataZone CreateNotebook API.

Usage:
    # Run against the dev stage in manifest.yaml (IAM domain):
    python setup_notebooks.py --manifest examples/e2e-notebook-sync/manifest.yaml --stage dev

    # Run against the dev-idc stage in manifest-idc.yaml (IdC domain):
    python setup_notebooks.py --manifest examples/e2e-notebook-sync/manifest-idc.yaml --stage dev-idc

    # Dry run:
    python setup_notebooks.py --manifest examples/e2e-notebook-sync/manifest.yaml --dry-run
"""

import argparse
import json
import os
import re
import sys
import time

import boto3
import yaml

# ---------------------------------------------------------------------------
# Notebook .ipynb content generators
# ---------------------------------------------------------------------------

def _data_prep_notebook_content():
    return json.dumps({
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": ["# Data Preparation ETL\n", "Cleanses and transforms raw transaction data."],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "source": [
                    "import pandas as pd\n",
                    "import pyarrow.parquet as pq\n",
                    "\n",
                    "# Parameters injected by SMUS CI/CD\n",
                    "input_path = '${input_path}'\n",
                    "output_path = '${output_path}'\n",
                    "refresh_mode = '${refresh_mode}'\n",
                    "partition_keys = '${partition_keys}'.split(',')\n",
                ],
                "outputs": [],
                "execution_count": None,
            },
            {
                "cell_type": "code",
                "metadata": {},
                "source": [
                    "# Read raw data\n",
                    "df = pd.read_csv(input_path + 'transactions.csv')\n",
                    "print(f'Loaded {len(df)} rows')\n",
                    "\n",
                    "# Cleansing\n",
                    "df = df.dropna(subset=['transaction_id'])\n",
                    "df = df.drop_duplicates(subset=['transaction_id'])\n",
                    "df['amount'] = df['amount'].astype(float)\n",
                    "print(f'After cleansing: {len(df)} rows')\n",
                ],
                "outputs": [],
                "execution_count": None,
            },
        ],
        "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}},
        "nbformat": 4,
        "nbformat_minor": 5,
    }, indent=2)


def _feature_engineering_notebook_content():
    return json.dumps({
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": ["# Feature Engineering\n", "Computes lag features from transaction data."],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "source": [
                    "import pandas as pd\n",
                    "\n",
                    "feature_group = '${feature_group}'\n",
                    "lookback_days = int('${lookback_days}')\n",
                    "target_column = '${target_column}'\n",
                    "\n",
                    "print(f'Feature group: {feature_group}')\n",
                    "print(f'Lookback: {lookback_days} days')\n",
                ],
                "outputs": [],
                "execution_count": None,
            },
        ],
        "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}},
        "nbformat": 4,
        "nbformat_minor": 5,
    }, indent=2)


def _model_training_notebook_content():
    return json.dumps({
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": ["# Model Training\n", "XGBoost classifier with MLflow tracking."],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "source": [
                    "import xgboost as xgb\n",
                    "from sklearn.model_selection import train_test_split\n",
                    "import mlflow\n",
                    "\n",
                    "# Simple training pipeline\n",
                    "print('Starting model training...')\n",
                ],
                "outputs": [],
                "execution_count": None,
            },
        ],
        "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}},
        "nbformat": 4,
        "nbformat_minor": 5,
    }, indent=2)


def _exploratory_notebook_content():
    return json.dumps({
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": ["# Exploratory Analysis\n", "Quick scratch notebook for ad-hoc exploration."],
            },
            {
                "cell_type": "code",
                "metadata": {},
                "source": ["print('This notebook is NOT included in CI/CD sync')\n"],
                "outputs": [],
                "execution_count": None,
            },
        ],
        "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}},
        "nbformat": 4,
        "nbformat_minor": 5,
    }, indent=2)


# ---------------------------------------------------------------------------
# Notebook definitions
# ---------------------------------------------------------------------------

NOTEBOOKS = [
    {
        "name": "data-prep-etl",
        "description": (
            "End-to-end data preparation pipeline that reads raw CSV data from S3, "
            "applies cleansing rules (null removal, deduplication, type casting), "
            "and writes Parquet output to the curated zone. Supports incremental "
            "and full-refresh modes via the 'refresh_mode' parameter."
        ),
        "parameters": {
            "input_path": "s3://sample-data/raw/transactions/",
            "output_path": "s3://sample-data/curated/transactions/",
            "refresh_mode": "incremental",
            "partition_keys": "year,month",
        },
        "environmentConfiguration": {
            "imageVersion": "sagemaker-distribution-v2",
            "packageConfig": {
                "packageManager": "UV",
                "packageSpecification": "pandas>=2.0\npyarrow>=14.0\nboto3",
            },
        },
        "content": _data_prep_notebook_content(),
    },
    {
        "name": "feature-engineering",
        "description": (
            "Feature engineering notebook that computes derived features from "
            "the curated transaction dataset. Outputs a feature store-ready "
            "DataFrame with timestamp alignment and lag features."
        ),
        "parameters": {
            "feature_group": "transaction_features",
            "lookback_days": "90",
            "target_column": "is_fraud",
        },
        "environmentConfiguration": None,
        "content": _feature_engineering_notebook_content(),
    },
    {
        "name": "model-training",
        "description": (
            "XGBoost model training notebook. Reads features from the feature "
            "store, performs train/test split, trains an XGBoost classifier, "
            "and registers the model artifact in MLflow."
        ),
        "parameters": {},
        "environmentConfiguration": {
            "imageVersion": "sagemaker-distribution-v2",
            "packageConfig": {
                "packageManager": "UV",
                "packageSpecification": "xgboost>=2.0\nscikit-learn>=1.3\nmlflow>=2.9",
            },
        },
        "content": _model_training_notebook_content(),
    },
    {
        "name": "exploratory-analysis",
        "description": "Quick EDA scratch notebook — not included in CI/CD sync.",
        "parameters": {},
        "environmentConfiguration": None,
        "content": _exploratory_notebook_content(),
    },
]

# The first 3 are the ones included in the manifest's notebook_ids filter.
# The 4th is intentionally excluded to test filtering.
FILTERED_NOTEBOOK_NAMES = ["data-prep-etl", "feature-engineering", "model-training"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def substitute_env_vars(text):
    """Substitute ${VAR} and ${VAR:default} patterns with environment values."""
    def replace(match):
        expr = match.group(1)
        if ":" in expr:
            var_name, default = expr.split(":", 1)
            return os.environ.get(var_name, default)
        return os.environ.get(expr, match.group(0))
    return re.sub(r"\$\{([^}]+)\}", replace, text)


def load_manifest(path):
    with open(path) as f:
        return yaml.safe_load(substitute_env_vars(f.read()))


# ---------------------------------------------------------------------------
# DataZone operations
# ---------------------------------------------------------------------------

def resolve_domain_id(domain_config, region):
    """Resolve domain ID from config (supports id, name, or tags lookup)."""
    dz = boto3.client("datazone", region_name=region)

    if "id" in domain_config:
        return domain_config["id"]

    if "name" in domain_config:
        domains = dz.list_domains()
        for d in domains.get("items", []):
            if d["name"] == domain_config["name"]:
                return d["id"]
        print(f"  Domain '{domain_config['name']}' not found")
        return None

    if "tags" in domain_config:
        domains = dz.list_domains()
        for d in domains.get("items", []):
            domain_id = d["id"]
            try:
                tags_resp = dz.list_tags_for_resource(resourceArn=d.get("arn", ""))
                domain_tags = tags_resp.get("tags", {})
            except Exception:
                domain_tags = {}
            match = all(
                domain_tags.get(k) == v
                for k, v in domain_config["tags"].items()
            )
            if match:
                return domain_id
        print(f"  Domain with tags {domain_config['tags']} not found")
        return None

    return None


def resolve_project_id(dz_client, domain_id, project_name):
    """Find project ID by name within a domain."""
    projects = dz_client.list_projects(domainIdentifier=domain_id)
    for p in projects.get("items", []):
        if p["name"] == project_name:
            return p["id"]
    return None


def list_existing_notebooks(dz_client, domain_id, project_id):
    """List active notebooks in the project, returns {name: id} map."""
    notebooks = {}
    next_token = None
    while True:
        params = {
            "domainIdentifier": domain_id,
            "owningProjectIdentifier": project_id,
            "status": "ACTIVE",
        }
        if next_token:
            params["nextToken"] = next_token
        resp = dz_client.list_notebooks(**params)
        for item in resp.get("items", []):
            nb_name = item.get("name", "")
            nb_id = item.get("id") or item.get("notebookId") or item.get("identifier")
            if nb_name and nb_id:
                notebooks[nb_name] = nb_id
        next_token = resp.get("nextToken")
        if not next_token:
            break
    return notebooks


def create_or_update_notebook(dz_client, s3_client, domain_id, project_id,
                              notebook_def, existing_notebooks, s3_uri, dry_run=False):
    """
    Create a notebook if it doesn't exist, or update it if it does.

    Steps:
      1. Upload .ipynb content to S3 at {s3_uri}/notebooks/setup/{name}.ipynb
      2. If notebook exists by name → StartNotebookSync (update) + UpdateNotebook
      3. If notebook doesn't exist → StartNotebookSync (create) + UpdateNotebook
    """
    name = notebook_def["name"]
    description = notebook_def["description"]
    parameters = notebook_def.get("parameters") or {}
    env_config = notebook_def.get("environmentConfiguration")
    content = notebook_def["content"]

    existing_id = existing_notebooks.get(name)
    action = "update" if existing_id else "create"

    if dry_run:
        print(f"  [DRY RUN] Would {action}: {name}" +
              (f" ({existing_id})" if existing_id else ""))
        return None

    # Step 1: Upload .ipynb to S3
    bucket, prefix = _parse_s3_uri(s3_uri)
    s3_key = f"{prefix}/notebooks/setup/{name}.ipynb".lstrip("/")
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=content.encode("utf-8"),
        ContentType="application/x-ipynb+json",
    )
    source_location = f"s3://{bucket}/{s3_key}"
    print(f"  Uploaded {name}.ipynb to {source_location}")

    # Step 2: StartNotebookSync
    sync_params = {
        "domainIdentifier": domain_id,
        "owningProjectIdentifier": project_id,
        "sourceLocation": {"s3": source_location},
        "name": name,
        "description": description,
    }
    if existing_id:
        sync_params["notebookId"] = existing_id

    try:
        resp = dz_client.start_notebook_sync(**sync_params)
        notebook_id = resp.get("notebookId") or resp.get("id") or existing_id
    except Exception as exc:
        print(f"  StartNotebookSync failed for '{name}': {exc}")
        return None

    if not notebook_id:
        print(f"  StartNotebookSync returned no ID for '{name}'")
        return None

    # Wait for sync to finish before updating metadata
    import time as _time
    for _ in range(30):
        try:
            detail = dz_client.get_notebook(domainIdentifier=domain_id, identifier=notebook_id)
            if detail.get("status") != "SYNC_IN_PROGRESS":
                break
        except Exception:
            pass
        _time.sleep(2)

    # Step 3: UpdateNotebook with metadata, parameters, environmentConfiguration
    update_params = {
        "domainIdentifier": domain_id,
        "identifier": notebook_id,
        "name": name,
        "description": description,
        "metadata": {"setup-source": "e2e-notebook-sync-setup"},
    }
    if parameters:
        update_params["parameters"] = parameters
    if env_config:
        update_params["environmentConfiguration"] = env_config

    try:
        dz_client.update_notebook(**update_params)
    except Exception as exc:
        print(f"  UpdateNotebook warning for '{name}': {exc}")

    status_icon = "updated" if existing_id else "created"
    print(f"  {name} ({notebook_id}) — {status_icon}")
    return notebook_id


def _parse_s3_uri(s3_uri):
    """Parse s3://bucket/prefix into (bucket, prefix)."""
    without_scheme = s3_uri.replace("s3://", "")
    bucket, _, prefix = without_scheme.partition("/")
    return bucket, prefix.rstrip("/")


def get_s3_shared_uri(dz_client, domain_id, project_id, region):
    """Get the default.s3_shared connection's s3Uri for the project."""
    try:
        resp = dz_client.list_connections(
            domainIdentifier=domain_id,
            projectIdentifier=project_id,
        )
        for conn in resp.get("items", []):
            if conn.get("name") == "default.s3_shared":
                # Get connection details
                detail = dz_client.get_connection(
                    domainIdentifier=domain_id,
                    identifier=conn.get("connectionId") or conn.get("id"),
                )
                # s3Uri lives in props.s3Properties.s3Uri
                props = detail.get("props") or {}
                s3_props = props.get("s3Properties") or {}
                s3_uri = s3_props.get("s3Uri")
                if s3_uri:
                    return s3_uri
                # Fallback: check connectionProperties (older format)
                conn_props = detail.get("connectionProperties") or {}
                s3_uri = conn_props.get("s3Uri") or conn_props.get("S3_URI")
                if s3_uri:
                    return s3_uri
    except Exception as exc:
        print(f"  Warning: Could not resolve s3_shared connection: {exc}")

    # Fallback: construct from account/region convention
    sts = boto3.client("sts")
    account_id = sts.get_caller_identity()["Account"]
    return f"s3://datazone-{account_id}-{region}-shared"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Create test notebooks in the DEV environment for E2E notebook sync testing"
    )
    parser.add_argument(
        "--manifest",
        default="examples/e2e-notebook-sync/manifest.yaml",
        help="Path to manifest YAML",
    )
    parser.add_argument("--stage", default="dev", help="Stage name (default: dev)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    args = parser.parse_args()

    print(f"Loading manifest: {args.manifest}")
    manifest = load_manifest(args.manifest)

    stage_config = manifest.get("stages", {}).get(args.stage)
    if not stage_config:
        print(f"Stage '{args.stage}' not found in manifest")
        sys.exit(1)

    domain_config = stage_config["domain"]
    region = domain_config["region"]
    project_name = stage_config["project"]["name"]

    print(f"Region: {region}")
    print(f"Project: {project_name}")
    print()

    # Resolve domain ID
    domain_id = resolve_domain_id(domain_config, region)
    if not domain_id:
        print("Could not resolve domain ID")
        sys.exit(1)
    print(f"Domain ID: {domain_id}")

    # Resolve project
    dz_client = boto3.client("datazone", region_name=region)
    project_id = resolve_project_id(dz_client, domain_id, project_name)
    if not project_id:
        print(f"Project '{project_name}' not found in domain {domain_id}")
        sys.exit(1)
    print(f"Project ID: {project_id}")

    # Get S3 shared URI
    s3_uri = get_s3_shared_uri(dz_client, domain_id, project_id, region)
    print(f"S3 Shared URI: {s3_uri}")
    print()

    # List existing notebooks
    existing = list_existing_notebooks(dz_client, domain_id, project_id)
    if existing:
        print(f"Existing notebooks in project: {list(existing.keys())}")
    else:
        print("No existing notebooks in project")
    print()

    # Create/update each notebook
    s3_client = boto3.client("s3", region_name=region)
    created_ids = {}

    print("Setting up notebooks:")
    print("-" * 60)
    for nb_def in NOTEBOOKS:
        nb_id = create_or_update_notebook(
            dz_client, s3_client, domain_id, project_id,
            nb_def, existing, s3_uri, dry_run=args.dry_run,
        )
        if nb_id:
            created_ids[nb_def["name"]] = nb_id
        print()

    print("-" * 60)
    print(f"Setup complete: {len(created_ids)} notebook(s) created/updated")
    print()

    # Print the IDs for use in notebook_ids filter
    filtered_ids = [
        created_ids[name] for name in FILTERED_NOTEBOOK_NAMES
        if name in created_ids
    ]
    if filtered_ids:
        print("Notebook IDs for manifest notebook_ids filter:")
        for name in FILTERED_NOTEBOOK_NAMES:
            nb_id = created_ids.get(name)
            if nb_id:
                print(f"  {name}: {nb_id}")
        print()
        print("YAML snippet (paste into manifest content.notebooks.notebook_ids):")
        print("  notebook_ids:")
        for nb_id in filtered_ids:
            print(f"  - {nb_id}")
    else:
        print("(No IDs available — use --dry-run=false to create notebooks)")

    # Verify excluded notebook
    excluded_name = "exploratory-analysis"
    if excluded_name in created_ids:
        print(f"\n  Note: '{excluded_name}' ({created_ids[excluded_name]}) is NOT in the filter")
        print(f"  It should remain in dev only and not be exported/synced.")


if __name__ == "__main__":
    main()
