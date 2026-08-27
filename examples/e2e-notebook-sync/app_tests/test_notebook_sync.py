"""
Post-deploy validation tests for the E2E notebook sync example.

These tests run after deploy completes and verify that notebooks were
synced correctly to the target project.
"""

import os

import boto3
import pytest

SOURCE_NOTEBOOK_METADATA_KEY = "smus-cicd-source-notebook-id"

# Expected notebooks that should be synced (the 3 in the filter)
EXPECTED_NOTEBOOK_NAMES = {"data-prep-etl", "feature-engineering", "model-training"}

# Notebook that must NOT be synced (excluded from filter)
EXCLUDED_NOTEBOOK_NAME = "exploratory-analysis"


@pytest.fixture
def dz_client():
    region = os.environ.get("DOMAIN_REGION") or os.environ.get("TEST_DOMAIN_REGION", "us-east-1")
    return boto3.client("datazone", region_name=region)


@pytest.fixture
def target_project_info():
    """Resolve target project from environment variables set by the deploy step."""
    return {
        "domain_id": os.environ.get("DOMAIN_ID", ""),
        "project_id": os.environ.get("PROJECT_ID", ""),
        "region": os.environ.get("DOMAIN_REGION") or os.environ.get("TEST_DOMAIN_REGION", "us-east-1"),
    }


def _list_active_notebooks(dz_client, domain_id, project_id):
    """List all ACTIVE notebooks in the target project."""
    notebooks = []
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
        notebooks.extend(resp.get("items", []))
        next_token = resp.get("nextToken")
        if not next_token:
            break
    return notebooks


class TestNotebookSyncResults:
    """Verify notebooks were synced to the target project correctly."""

    def test_synced_notebooks_exist_in_target(self, dz_client, target_project_info):
        """All 3 filtered notebooks should exist in the target project."""
        domain_id = target_project_info["domain_id"]
        project_id = target_project_info["project_id"]

        if not domain_id or not project_id:
            pytest.skip("DOMAIN_ID or PROJECT_ID not set — skipping post-deploy test")

        notebooks = _list_active_notebooks(dz_client, domain_id, project_id)
        notebook_names = {nb.get("name", "") for nb in notebooks}

        for expected in EXPECTED_NOTEBOOK_NAMES:
            assert expected in notebook_names, (
                f"Expected notebook '{expected}' not found in target project. "
                f"Found: {sorted(notebook_names)}"
            )

    def test_excluded_notebook_not_synced(self, dz_client, target_project_info):
        """The excluded notebook (exploratory-analysis) must NOT be in the target."""
        domain_id = target_project_info["domain_id"]
        project_id = target_project_info["project_id"]

        if not domain_id or not project_id:
            pytest.skip("DOMAIN_ID or PROJECT_ID not set — skipping post-deploy test")

        notebooks = _list_active_notebooks(dz_client, domain_id, project_id)
        notebook_names = {nb.get("name", "") for nb in notebooks}

        assert EXCLUDED_NOTEBOOK_NAME not in notebook_names, (
            f"Excluded notebook '{EXCLUDED_NOTEBOOK_NAME}' was synced to target "
            f"but should have been filtered out"
        )

    def test_synced_notebooks_have_tracking_metadata(self, dz_client, target_project_info):
        """Each synced notebook must carry the smus-cicd-source-notebook-id metadata key."""
        domain_id = target_project_info["domain_id"]
        project_id = target_project_info["project_id"]

        if not domain_id or not project_id:
            pytest.skip("DOMAIN_ID or PROJECT_ID not set — skipping post-deploy test")

        notebooks = _list_active_notebooks(dz_client, domain_id, project_id)

        for nb in notebooks:
            nb_name = nb.get("name", "")
            if nb_name not in EXPECTED_NOTEBOOK_NAMES:
                continue

            nb_id = nb.get("id") or nb.get("notebookId") or nb.get("identifier")
            detail = dz_client.get_notebook(
                domainIdentifier=domain_id,
                identifier=nb_id,
            )
            metadata = detail.get("metadata") or {}
            assert SOURCE_NOTEBOOK_METADATA_KEY in metadata, (
                f"Notebook '{nb_name}' ({nb_id}) is missing tracking metadata key "
                f"'{SOURCE_NOTEBOOK_METADATA_KEY}'"
            )

    def test_data_prep_etl_has_parameters(self, dz_client, target_project_info):
        """data-prep-etl notebook should have its parameters preserved after sync."""
        domain_id = target_project_info["domain_id"]
        project_id = target_project_info["project_id"]

        if not domain_id or not project_id:
            pytest.skip("DOMAIN_ID or PROJECT_ID not set — skipping post-deploy test")

        notebooks = _list_active_notebooks(dz_client, domain_id, project_id)
        data_prep = next(
            (nb for nb in notebooks if nb.get("name") == "data-prep-etl"), None
        )
        if not data_prep:
            pytest.fail("data-prep-etl notebook not found in target project")

        nb_id = data_prep.get("id") or data_prep.get("notebookId")
        detail = dz_client.get_notebook(
            domainIdentifier=domain_id, identifier=nb_id
        )
        params = detail.get("parameters") or {}
        assert "input_path" in params, "Expected 'input_path' parameter on data-prep-etl"
        assert "refresh_mode" in params, "Expected 'refresh_mode' parameter on data-prep-etl"

    def test_notebook_count_matches_expected(self, dz_client, target_project_info):
        """Target project should have exactly 3 CI/CD-managed notebooks."""
        domain_id = target_project_info["domain_id"]
        project_id = target_project_info["project_id"]

        if not domain_id or not project_id:
            pytest.skip("DOMAIN_ID or PROJECT_ID not set — skipping post-deploy test")

        notebooks = _list_active_notebooks(dz_client, domain_id, project_id)
        cicd_notebooks = []
        for nb in notebooks:
            nb_id = nb.get("id") or nb.get("notebookId") or nb.get("identifier")
            try:
                detail = dz_client.get_notebook(
                    domainIdentifier=domain_id, identifier=nb_id
                )
                metadata = detail.get("metadata") or {}
                if SOURCE_NOTEBOOK_METADATA_KEY in metadata:
                    cicd_notebooks.append(nb)
            except Exception:
                pass

        assert len(cicd_notebooks) == 3, (
            f"Expected 3 CI/CD-managed notebooks, found {len(cicd_notebooks)}: "
            f"{[nb.get('name') for nb in cicd_notebooks]}"
        )
