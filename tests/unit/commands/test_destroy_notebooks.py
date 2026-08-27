"""Unit tests for notebook destroy support.

Covers:
  - _discover_notebooks: pagination, metadata filtering, notebook_ids_filter
  - destroy_executor notebook deletion: deleted / not_found / error / skipped
  - Validate stage notebook discovery integration
"""

import unittest
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from smus_cicd.application.application_manifest import (
    ContentConfig,
    DeploymentConfiguration,
    DomainConfig,
    NotebookConfig,
    ProjectConfig,
    StageConfig,
)
from smus_cicd.helpers.destroy_executor import _destroy_stage
from smus_cicd.helpers.destroy_models import (
    ResourceToDelete,
    ValidationResult,
)
from smus_cicd.helpers.destroy_validator import _discover_notebooks

SOURCE_KEY = "smus-cicd-source-notebook-id"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _client_error(code, message="error"):
    return ClientError({"Error": {"Code": code, "Message": message}}, "Op")


def _make_dz_client(notebook_map):
    """
    notebook_map: {target_nb_id: source_nb_id or None}
    None means no tracking metadata on that notebook.
    """
    client = MagicMock()
    ids = list(notebook_map.keys())
    client.list_notebooks.return_value = {"items": [{"id": nb_id} for nb_id in ids]}

    def get_notebook(domainIdentifier, identifier):
        source_id = notebook_map.get(identifier)
        metadata = {SOURCE_KEY: source_id} if source_id else {}
        return {"id": identifier, "name": f"NB-{identifier}", "metadata": metadata}

    client.get_notebook.side_effect = get_notebook
    return client


def _make_vr(resources=None):
    return ValidationResult(
        errors=[],
        warnings=[],
        resources=resources or [],
        active_workflow_runs={},
    )


def _simple_manifest():
    from smus_cicd.application.application_manifest import ApplicationManifest

    return ApplicationManifest(
        application_name="App",
        content=ContentConfig(),
        stages={},
    )


def _make_stage_config():
    return StageConfig(
        project=ProjectConfig(name="test-project", create=False),
        domain=DomainConfig(region="us-east-1"),
        stage="TEST",
        deployment_configuration=DeploymentConfiguration(),
    )


# ---------------------------------------------------------------------------
# _discover_notebooks
# ---------------------------------------------------------------------------


class TestDiscoverNotebooks(unittest.TestCase):

    def test_notebooks_with_tracking_metadata_included(self):
        dz = _make_dz_client(
            {
                "tgt-1": "src-1",
                "tgt-2": "src-2",
            }
        )
        result = _discover_notebooks(
            "dom-1",
            "proj-1",
            "us-east-1",
            notebook_ids_filter=None,
            _dz_client_override=dz,
        )
        self.assertEqual(len(result), 2)
        source_ids = {r["source_notebook_id"] for r in result}
        self.assertEqual(source_ids, {"src-1", "src-2"})

    def test_notebooks_without_tracking_metadata_excluded(self):
        dz = _make_dz_client(
            {
                "tgt-cicd": "src-1",
                "tgt-manual": None,  # no tracking key
            }
        )
        result = _discover_notebooks(
            "dom-1",
            "proj-1",
            "us-east-1",
            notebook_ids_filter=None,
            _dz_client_override=dz,
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["source_notebook_id"], "src-1")

    def test_notebook_ids_filter_restricts_to_matching_source_ids(self):
        dz = _make_dz_client(
            {
                "tgt-1": "src-1",
                "tgt-2": "src-2",
                "tgt-3": "src-3",
            }
        )
        result = _discover_notebooks(
            "dom-1",
            "proj-1",
            "us-east-1",
            notebook_ids_filter=["src-1", "src-3"],
            _dz_client_override=dz,
        )
        self.assertEqual(len(result), 2)
        source_ids = {r["source_notebook_id"] for r in result}
        self.assertEqual(source_ids, {"src-1", "src-3"})

    def test_notebook_ids_filter_none_includes_all_tracked(self):
        dz = _make_dz_client(
            {
                "tgt-1": "src-1",
                "tgt-2": "src-2",
            }
        )
        result = _discover_notebooks(
            "dom-1",
            "proj-1",
            "us-east-1",
            notebook_ids_filter=None,
            _dz_client_override=dz,
        )
        self.assertEqual(len(result), 2)

    def test_source_environment_no_tracking_returns_empty(self):
        """Notebooks in source environment have no tracking metadata → zero deletions."""
        dz = _make_dz_client(
            {
                "nb-source-1": None,
                "nb-source-2": None,
            }
        )
        result = _discover_notebooks(
            "dom-1",
            "proj-1",
            "us-east-1",
            notebook_ids_filter=None,
            _dz_client_override=dz,
        )
        self.assertEqual(result, [])

    def test_empty_project_returns_empty(self):
        dz = MagicMock()
        dz.list_notebooks.return_value = {"items": []}
        result = _discover_notebooks(
            "dom-1",
            "proj-1",
            "us-east-1",
            notebook_ids_filter=None,
            _dz_client_override=dz,
        )
        self.assertEqual(result, [])

    def test_pagination_followed(self):
        dz = MagicMock()
        dz.list_notebooks.side_effect = [
            {"items": [{"id": "tgt-1"}], "nextToken": "tok"},
            {"items": [{"id": "tgt-2"}]},
        ]
        dz.get_notebook.side_effect = [
            {"id": "tgt-1", "metadata": {SOURCE_KEY: "src-1"}},
            {"id": "tgt-2", "metadata": {SOURCE_KEY: "src-2"}},
        ]
        result = _discover_notebooks(
            "dom-1",
            "proj-1",
            "us-east-1",
            notebook_ids_filter=None,
            _dz_client_override=dz,
        )
        self.assertEqual(len(result), 2)
        self.assertEqual(dz.list_notebooks.call_count, 2)

    def test_result_contains_target_and_source_ids_and_name(self):
        dz = _make_dz_client({"tgt-abc": "src-xyz"})
        result = _discover_notebooks(
            "dom-1",
            "proj-1",
            "us-east-1",
            notebook_ids_filter=None,
            _dz_client_override=dz,
        )
        self.assertEqual(result[0]["target_notebook_id"], "tgt-abc")
        self.assertEqual(result[0]["source_notebook_id"], "src-xyz")
        self.assertIn("name", result[0])


# ---------------------------------------------------------------------------
# Destroy executor: notebook deletion
# ---------------------------------------------------------------------------

PATCH_BOTO3 = "smus_cicd.helpers.destroy_executor.create_client"


class TestDestroyExecutorNotebooks(unittest.TestCase):

    def _sts_dz(self, dz):
        """Return a boto3 factory that yields an STS mock and the given DZ mock."""
        sts = MagicMock()
        sts.get_caller_identity.return_value = {"Account": "111122223333"}

        def factory(service, **kwargs):
            if service == "sts":
                return sts
            return dz

        return factory

    @patch(PATCH_BOTO3)
    def test_notebook_deleted_successfully(self, mock_boto3):
        dz = MagicMock()
        mock_boto3.side_effect = self._sts_dz(dz)

        vr = _make_vr(
            resources=[
                ResourceToDelete(
                    "notebook",
                    "nb-tgt-1",
                    "test",
                    {
                        "name": "My NB",
                        "source_notebook_id": "nb-src-1",
                        "domain_id": "dom-1",
                    },
                )
            ]
        )
        results = _destroy_stage(
            "test", _make_stage_config(), _simple_manifest(), vr, "us-east-1", "TEXT"
        )
        dz.delete_notebook.assert_called_once_with(
            domainIdentifier="dom-1", identifier="nb-tgt-1"
        )
        notebook_results = [r for r in results if r.resource_type == "notebook"]
        self.assertEqual(len(notebook_results), 1)
        self.assertEqual(notebook_results[0].status, "deleted")

    @patch(PATCH_BOTO3)
    def test_notebook_not_found_recorded(self, mock_boto3):
        dz = MagicMock()
        dz.delete_notebook.side_effect = _client_error("ResourceNotFoundException")
        mock_boto3.side_effect = self._sts_dz(dz)

        vr = _make_vr(
            resources=[
                ResourceToDelete(
                    "notebook",
                    "nb-tgt-1",
                    "test",
                    {"name": "NB", "source_notebook_id": "src-1", "domain_id": "dom-1"},
                )
            ]
        )
        results = _destroy_stage(
            "test", _make_stage_config(), _simple_manifest(), vr, "us-east-1", "TEXT"
        )
        nb_result = next(r for r in results if r.resource_type == "notebook")
        self.assertEqual(nb_result.status, "not_found")

    @patch(PATCH_BOTO3)
    def test_notebook_api_error_recorded_and_continues(self, mock_boto3):
        dz = MagicMock()
        call_count = [0]

        def delete_notebook(**kwargs):
            call_count[0] += 1
            if kwargs["identifier"] == "nb-err":
                raise _client_error("AccessDeniedException")

        dz.delete_notebook.side_effect = delete_notebook
        mock_boto3.side_effect = self._sts_dz(dz)

        vr = _make_vr(
            resources=[
                ResourceToDelete(
                    "notebook",
                    "nb-err",
                    "test",
                    {
                        "name": "NB1",
                        "source_notebook_id": "src-1",
                        "domain_id": "dom-1",
                    },
                ),
                ResourceToDelete(
                    "notebook",
                    "nb-ok",
                    "test",
                    {
                        "name": "NB2",
                        "source_notebook_id": "src-2",
                        "domain_id": "dom-1",
                    },
                ),
            ]
        )
        results = _destroy_stage(
            "test", _make_stage_config(), _simple_manifest(), vr, "us-east-1", "TEXT"
        )
        nb_results = {
            r.resource_id: r.status for r in results if r.resource_type == "notebook"
        }
        self.assertEqual(nb_results["nb-err"], "error")
        self.assertEqual(nb_results["nb-ok"], "deleted")
        self.assertEqual(call_count[0], 2)  # both attempted

    @patch(PATCH_BOTO3)
    def test_notebook_missing_domain_id_skipped(self, mock_boto3):
        dz = MagicMock()
        mock_boto3.side_effect = self._sts_dz(dz)

        vr = _make_vr(
            resources=[
                ResourceToDelete(
                    "notebook",
                    "nb-1",
                    "test",
                    {"name": "NB", "source_notebook_id": "src-1", "domain_id": ""},
                ),
            ]
        )
        results = _destroy_stage(
            "test", _make_stage_config(), _simple_manifest(), vr, "us-east-1", "TEXT"
        )
        nb_result = next(r for r in results if r.resource_type == "notebook")
        self.assertEqual(nb_result.status, "skipped")
        dz.delete_notebook.assert_not_called()

    @patch(PATCH_BOTO3)
    def test_multiple_notebooks_all_deleted(self, mock_boto3):
        dz = MagicMock()
        mock_boto3.side_effect = self._sts_dz(dz)

        vr = _make_vr(
            resources=[
                ResourceToDelete(
                    "notebook",
                    f"nb-{i}",
                    "test",
                    {
                        "name": f"NB{i}",
                        "source_notebook_id": f"src-{i}",
                        "domain_id": "dom-1",
                    },
                )
                for i in range(5)
            ]
        )
        results = _destroy_stage(
            "test", _make_stage_config(), _simple_manifest(), vr, "us-east-1", "TEXT"
        )
        nb_results = [r for r in results if r.resource_type == "notebook"]
        self.assertEqual(len(nb_results), 5)
        self.assertTrue(all(r.status == "deleted" for r in nb_results))
        self.assertEqual(dz.delete_notebook.call_count, 5)


# ---------------------------------------------------------------------------
# Validate stage: notebook discovery integration
# ---------------------------------------------------------------------------

PATCH_DOMAIN = "smus_cicd.helpers.destroy_validator.get_domain_from_target_config"
PATCH_PROJECT_ID = "smus_cicd.helpers.destroy_validator.get_project_id_by_name"
PATCH_CONNECTIONS = "smus_cicd.helpers.destroy_validator.get_project_connections"
PATCH_LIST_WF = "smus_cicd.helpers.destroy_validator.list_workflows"
PATCH_LIST_RUNS = "smus_cicd.helpers.destroy_validator.list_workflow_runs"
PATCH_GET_WF_DEF = "smus_cicd.helpers.destroy_validator.get_workflow_definition"


class TestValidateStageNotebooks(unittest.TestCase):

    def _make_manifest_with_notebooks(self, notebook_ids=None):
        from smus_cicd.application.application_manifest import ApplicationManifest

        return ApplicationManifest(
            application_name="App",
            content=ContentConfig(
                notebooks=NotebookConfig(
                    enabled=True,
                    notebook_ids=notebook_ids,
                )
            ),
            stages={},
        )

    def _make_notebook_stage(self):
        return StageConfig(
            project=ProjectConfig(name="target-project", create=False),
            domain=DomainConfig(region="us-east-1"),
            stage="TEST",
            deployment_configuration=DeploymentConfiguration(),
        )

    @patch(PATCH_GET_WF_DEF, return_value="")
    @patch(PATCH_LIST_RUNS, return_value=[])
    @patch(PATCH_LIST_WF, return_value=[])
    @patch(PATCH_CONNECTIONS, return_value={})
    @patch(PATCH_PROJECT_ID, return_value="proj-target")
    @patch(PATCH_DOMAIN, return_value=("dom-1", "test-domain"))
    def test_cicd_notebooks_added_to_destruction_plan(self, *_):
        """Notebooks with tracking metadata are discovered and added to the plan."""
        from smus_cicd.helpers.destroy_validator import _validate_stage

        with patch(
            "smus_cicd.helpers.destroy_validator._discover_notebooks"
        ) as mock_discover:
            mock_discover.return_value = [
                {
                    "target_notebook_id": "tgt-1",
                    "source_notebook_id": "src-1",
                    "name": "NB1",
                },
                {
                    "target_notebook_id": "tgt-2",
                    "source_notebook_id": "src-2",
                    "name": "NB2",
                },
            ]
            result = _validate_stage(
                "test",
                self._make_notebook_stage(),
                self._make_manifest_with_notebooks(),
                "us-east-1",
            )

        notebook_resources = [
            r for r in result.resources if r.resource_type == "notebook"
        ]
        self.assertEqual(len(notebook_resources), 2)
        target_ids = {r.resource_id for r in notebook_resources}
        self.assertEqual(target_ids, {"tgt-1", "tgt-2"})

    @patch(PATCH_GET_WF_DEF, return_value="")
    @patch(PATCH_LIST_RUNS, return_value=[])
    @patch(PATCH_LIST_WF, return_value=[])
    @patch(PATCH_CONNECTIONS, return_value={})
    @patch(PATCH_PROJECT_ID, return_value="proj-target")
    @patch(PATCH_DOMAIN, return_value=("dom-1", "test-domain"))
    def test_discover_notebooks_called_with_notebook_ids_filter(self, *_):
        """When notebook_ids specified in manifest, filter is passed to _discover_notebooks."""
        from smus_cicd.helpers.destroy_validator import _validate_stage

        with patch(
            "smus_cicd.helpers.destroy_validator._discover_notebooks"
        ) as mock_discover:
            mock_discover.return_value = []
            _validate_stage(
                "test",
                self._make_notebook_stage(),
                self._make_manifest_with_notebooks(notebook_ids=["src-1", "src-2"]),
                "us-east-1",
            )

        # _discover_notebooks must have been called with the filter list
        call_kwargs = mock_discover.call_args[1]
        self.assertEqual(call_kwargs.get("notebook_ids_filter"), ["src-1", "src-2"])

    @patch(PATCH_GET_WF_DEF, return_value="")
    @patch(PATCH_LIST_RUNS, return_value=[])
    @patch(PATCH_LIST_WF, return_value=[])
    @patch(PATCH_CONNECTIONS, return_value={})
    @patch(PATCH_PROJECT_ID, return_value="proj-target")
    @patch(PATCH_DOMAIN, return_value=("dom-1", "test-domain"))
    def test_discover_failure_recorded_as_error(self, *_):
        """ListNotebooks API failure → recorded as error in validation result."""
        from smus_cicd.helpers.destroy_validator import _validate_stage

        with patch(
            "smus_cicd.helpers.destroy_validator._discover_notebooks",
            side_effect=Exception("ListNotebooksError"),
        ):
            result = _validate_stage(
                "test",
                self._make_notebook_stage(),
                self._make_manifest_with_notebooks(),
                "us-east-1",
            )

        self.assertTrue(
            any("ListNotebooks" in e or "notebook" in e.lower() for e in result.errors)
        )

    @patch(PATCH_GET_WF_DEF, return_value="")
    @patch(PATCH_LIST_RUNS, return_value=[])
    @patch(PATCH_LIST_WF, return_value=[])
    @patch(PATCH_CONNECTIONS, return_value={})
    @patch(PATCH_PROJECT_ID, return_value="proj-target")
    @patch(PATCH_DOMAIN, return_value=("dom-1", "test-domain"))
    def test_notebooks_disabled_in_manifest_not_discovered(self, *_):
        """When content.notebooks.enabled=False, notebook discovery is skipped."""
        from smus_cicd.application.application_manifest import ApplicationManifest
        from smus_cicd.helpers.destroy_validator import _validate_stage

        manifest = ApplicationManifest(
            application_name="App",
            content=ContentConfig(notebooks=NotebookConfig(enabled=False)),
            stages={},
        )
        with patch(
            "smus_cicd.helpers.destroy_validator._discover_notebooks"
        ) as mock_discover:
            _validate_stage("test", self._make_notebook_stage(), manifest, "us-east-1")
            mock_discover.assert_not_called()


if __name__ == "__main__":
    unittest.main()
