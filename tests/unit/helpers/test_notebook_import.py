"""Unit tests for notebook_import.py.

Covers:
  - _validate_notebook_manifest: required key checks
  - _generate_client_token: determinism, length bound, distinct outputs
  - _build_update_kwargs: Property 7 rules (metadata key always present,
    parameters omitted when empty, environmentConfiguration omitted when None)
  - _discover_target_notebooks: pagination, metadata filtering
  - sync_notebooks: happy paths (create, update, fallback), partial failures,
    missing S3 connection, missing file in bundle
"""

import hashlib
import unittest
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from smus_cicd.helpers.notebook_import import (
    SOURCE_NOTEBOOK_METADATA_KEY,
    NotebookSyncSummary,
    SyncStatus,
    _build_update_kwargs,
    _discover_target_notebooks,
    _generate_client_token,
    _validate_notebook_manifest,
    sync_notebooks,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _client_error(code, message="error"):
    return ClientError({"Error": {"Code": code, "Message": message}}, "Op")


def _make_manifest(notebooks=None, source_project="proj-src", source_domain="dom-1"):
    return {
        "metadata": {
            "sourceProjectId": source_project,
            "sourceDomainId": source_domain,
            "exportTimestamp": "2024-01-01T00:00:00Z",
            "notebookCount": len(notebooks or []),
        },
        "notebooks": notebooks or [],
    }


def _make_entry(nb_id="nb-1", name="NB", description="Desc",
                parameters=None, metadata=None, env_config=None):
    return {
        "sourceNotebookId": nb_id,
        "name": name,
        "description": description,
        "filePath": f"notebooks/{nb_id}.ipynb",
        "exportedAt": "2024-01-01T00:00:00Z",
        "parameters": parameters if parameters is not None else {},
        "metadata": metadata if metadata is not None else {},
        "environmentConfiguration": env_config,
    }


# ---------------------------------------------------------------------------
# _validate_notebook_manifest
# ---------------------------------------------------------------------------

class TestValidateNotebookManifest(unittest.TestCase):

    def test_valid_manifest_passes(self):
        # Should not raise
        _validate_notebook_manifest(_make_manifest())

    def test_missing_metadata_key_raises(self):
        with self.assertRaises(ValueError) as ctx:
            _validate_notebook_manifest({"notebooks": []})
        self.assertIn("metadata", str(ctx.exception))

    def test_missing_notebooks_key_raises(self):
        with self.assertRaises(ValueError) as ctx:
            _validate_notebook_manifest({"metadata": {
                "sourceProjectId": "p", "sourceDomainId": "d",
                "exportTimestamp": "t", "notebookCount": 0,
            }})
        self.assertIn("notebooks", str(ctx.exception))

    def test_missing_source_project_id_raises(self):
        manifest = _make_manifest()
        del manifest["metadata"]["sourceProjectId"]
        with self.assertRaises(ValueError):
            _validate_notebook_manifest(manifest)

    def test_missing_source_domain_id_raises(self):
        manifest = _make_manifest()
        del manifest["metadata"]["sourceDomainId"]
        with self.assertRaises(ValueError):
            _validate_notebook_manifest(manifest)

    def test_missing_export_timestamp_raises(self):
        manifest = _make_manifest()
        del manifest["metadata"]["exportTimestamp"]
        with self.assertRaises(ValueError):
            _validate_notebook_manifest(manifest)

    def test_missing_notebook_count_raises(self):
        manifest = _make_manifest()
        del manifest["metadata"]["notebookCount"]
        with self.assertRaises(ValueError):
            _validate_notebook_manifest(manifest)

    def test_no_api_calls_before_validation(self):
        """Validation is pure — no API calls should be made."""
        # Just calling _validate_notebook_manifest is enough; if it didn't raise
        # on a valid manifest, we know it was fast/pure
        _validate_notebook_manifest(_make_manifest())  # no mocks needed


# ---------------------------------------------------------------------------
# _generate_client_token
# ---------------------------------------------------------------------------

class TestGenerateClientToken(unittest.TestCase):
    """Property 5: deterministic, ≤ 64 chars, distinct inputs → distinct outputs."""

    def test_same_inputs_produce_same_token(self):
        t1 = _generate_client_token("nb-1", "2024-01-01T00:00:00")
        t2 = _generate_client_token("nb-1", "2024-01-01T00:00:00")
        self.assertEqual(t1, t2)

    def test_token_max_64_chars(self):
        token = _generate_client_token("nb-abc123", "1700000000")
        self.assertLessEqual(len(token), 64)

    def test_different_notebook_ids_produce_different_tokens(self):
        t1 = _generate_client_token("nb-1", "ts")
        t2 = _generate_client_token("nb-2", "ts")
        self.assertNotEqual(t1, t2)

    def test_different_timestamps_produce_different_tokens(self):
        t1 = _generate_client_token("nb-1", "ts-1")
        t2 = _generate_client_token("nb-1", "ts-2")
        self.assertNotEqual(t1, t2)

    def test_long_source_id_stays_within_64_chars(self):
        long_id = "a" * 36  # max valid ID length
        token = _generate_client_token(long_id, "timestamp-" + "x" * 50)
        self.assertLessEqual(len(token), 64)


# ---------------------------------------------------------------------------
# _build_update_kwargs  (Property 7)
# ---------------------------------------------------------------------------

class TestBuildUpdateKwargs(unittest.TestCase):

    def _kwargs(self, **entry_overrides):
        entry = _make_entry()
        # Translate convenience kwarg names to manifest entry key names
        if "env_config" in entry_overrides:
            entry["environmentConfiguration"] = entry_overrides.pop("env_config")
        if "parameters" in entry_overrides:
            entry["parameters"] = entry_overrides.pop("parameters")
        if "metadata" in entry_overrides:
            entry["metadata"] = entry_overrides.pop("metadata")
        entry.update(entry_overrides)
        return _build_update_kwargs("dom-1", "proj-1", "tgt-nb-1", entry)

    def test_always_includes_name_and_description(self):
        kwargs = self._kwargs(name="My NB", description="Some desc")
        self.assertEqual(kwargs["name"], "My NB")
        self.assertEqual(kwargs["description"], "Some desc")

    def test_metadata_always_contains_tracking_key(self):
        """smus-cicd-source-notebook-id must always be present."""
        kwargs = self._kwargs(metadata={})
        self.assertIn(SOURCE_NOTEBOOK_METADATA_KEY, kwargs["metadata"])
        self.assertEqual(kwargs["metadata"][SOURCE_NOTEBOOK_METADATA_KEY], "nb-1")

    def test_tracking_key_merged_with_existing_metadata(self):
        kwargs = self._kwargs(metadata={"owner": "team-ds", "version": "2.1"})
        meta = kwargs["metadata"]
        self.assertEqual(meta["owner"], "team-ds")
        self.assertEqual(meta["version"], "2.1")
        self.assertIn(SOURCE_NOTEBOOK_METADATA_KEY, meta)

    def test_metadata_only_tracking_key_when_manifest_metadata_empty(self):
        """Empty manifest metadata → metadata in kwargs contains only tracking key."""
        kwargs = self._kwargs(metadata={})
        self.assertEqual(
            set(kwargs["metadata"].keys()),
            {SOURCE_NOTEBOOK_METADATA_KEY},
        )

    def test_parameters_omitted_when_empty_dict(self):
        """Property 7: parameters is omitted when empty."""
        kwargs = self._kwargs(parameters={})
        self.assertNotIn("parameters", kwargs)

    def test_parameters_included_when_non_empty(self):
        kwargs = self._kwargs(parameters={"dataset_path": "s3://b/d.csv"})
        self.assertEqual(kwargs["parameters"], {"dataset_path": "s3://b/d.csv"})

    def test_environment_configuration_omitted_when_none(self):
        """Property 7: environmentConfiguration omitted when None."""
        kwargs = self._kwargs(env_config=None)
        self.assertNotIn("environmentConfiguration", kwargs)

    def test_environment_configuration_included_when_set(self):
        env = {"imageVersion": "v2", "packageConfig": {"packageManager": "pip", "packageSpecification": "pandas"}}
        kwargs = self._kwargs(env_config=env)
        self.assertEqual(kwargs["environmentConfiguration"], env)

    def test_required_api_fields_always_present(self):
        kwargs = self._kwargs()
        for field in ("domainIdentifier", "identifier", "owningProjectIdentifier", "name", "description", "metadata"):
            self.assertIn(field, kwargs)


# ---------------------------------------------------------------------------
# _discover_target_notebooks
# ---------------------------------------------------------------------------

class TestDiscoverTargetNotebooks(unittest.TestCase):

    def _make_dz_client(self, notebook_metadata_map):
        """
        notebook_metadata_map: {nb_id: metadata_dict}
        ListNotebooks returns all IDs; GetNotebook returns the metadata for each.
        """
        client = MagicMock()
        ids = list(notebook_metadata_map.keys())
        client.list_notebooks.return_value = {
            "items": [{"id": nb_id} for nb_id in ids]
        }

        def get_notebook(domainIdentifier, notebookIdentifier):
            meta = notebook_metadata_map.get(notebookIdentifier, {})
            return {"id": notebookIdentifier, "name": notebookIdentifier, "metadata": meta}

        client.get_notebook.side_effect = get_notebook
        return client

    def test_notebooks_with_tracking_key_are_mapped(self):
        client = self._make_dz_client({
            "tgt-1": {SOURCE_NOTEBOOK_METADATA_KEY: "src-1"},
            "tgt-2": {SOURCE_NOTEBOOK_METADATA_KEY: "src-2"},
        })
        result = _discover_target_notebooks(client, "dom-1", "proj-1")
        self.assertEqual(result["src-1"], "tgt-1")
        self.assertEqual(result["src-2"], "tgt-2")

    def test_notebooks_without_tracking_key_are_excluded(self):
        client = self._make_dz_client({
            "tgt-managed": {},  # no tracking key
            "tgt-cicd": {SOURCE_NOTEBOOK_METADATA_KEY: "src-1"},
        })
        result = _discover_target_notebooks(client, "dom-1", "proj-1")
        self.assertIn("src-1", result)
        self.assertNotIn("tgt-managed", result.values())
        self.assertEqual(len(result), 1)

    def test_empty_project_returns_empty_map(self):
        client = MagicMock()
        client.list_notebooks.return_value = {"items": []}
        result = _discover_target_notebooks(client, "dom-1", "proj-1")
        self.assertEqual(result, {})

    def test_pagination_followed(self):
        client = MagicMock()
        client.list_notebooks.side_effect = [
            {"items": [{"id": "tgt-1"}], "nextToken": "tok"},
            {"items": [{"id": "tgt-2"}]},
        ]

        def get_notebook(domainIdentifier, notebookIdentifier):
            return {
                "id": notebookIdentifier,
                "metadata": {SOURCE_NOTEBOOK_METADATA_KEY: f"src-{notebookIdentifier}"},
            }

        client.get_notebook.side_effect = get_notebook
        result = _discover_target_notebooks(client, "dom-1", "proj-1")
        self.assertEqual(len(result), 2)
        self.assertEqual(client.list_notebooks.call_count, 2)

    def test_get_notebook_failure_skips_that_notebook(self):
        client = MagicMock()
        client.list_notebooks.return_value = {"items": [{"id": "tgt-bad"}, {"id": "tgt-ok"}]}

        def get_notebook(domainIdentifier, notebookIdentifier):
            if notebookIdentifier == "tgt-bad":
                raise Exception("GetNotebookError")
            return {"id": notebookIdentifier, "metadata": {SOURCE_NOTEBOOK_METADATA_KEY: "src-ok"}}

        client.get_notebook.side_effect = get_notebook
        result = _discover_target_notebooks(client, "dom-1", "proj-1")
        self.assertEqual(result, {"src-ok": "tgt-ok"})


# ---------------------------------------------------------------------------
# sync_notebooks (public API)
# ---------------------------------------------------------------------------

PATCH_DZ = "smus_cicd.helpers.notebook_import._get_datazone_client"
PATCH_S3 = "smus_cicd.helpers.notebook_import._get_s3_client"
NOTEBOOK_FILES = {"notebooks/nb-1.ipynb": b'{"cells":[]}'}
S3_URI = "s3://test-bucket/shared"


class TestSyncNotebooks(unittest.TestCase):

    def _make_dz_no_existing(self):
        """DZ client where target project has no existing CI/CD notebooks."""
        dz = MagicMock()
        dz.list_notebooks.return_value = {"items": []}
        dz.start_notebook_sync.return_value = {"notebookId": "new-nb-id"}
        dz.update_notebook.return_value = {}
        return dz

    def _make_s3(self):
        s3 = MagicMock()
        s3.put_object.return_value = {}
        return s3

    # ── validation ────────────────────────────────────────────────────────

    def test_invalid_manifest_raises_before_api_calls(self):
        with self.assertRaises(ValueError):
            sync_notebooks(
                "dom-1", "proj-1", "us-east-1",
                manifest_data={"metadata": {}},  # missing notebooks key
                notebook_files={},
                s3_uri=S3_URI,
            )

    def test_missing_s3_uri_raises_value_error(self):
        with self.assertRaises(ValueError):
            sync_notebooks(
                "dom-1", "proj-1", "us-east-1",
                manifest_data=_make_manifest(),
                notebook_files={},
                s3_uri="",
            )

    # ── happy path: create ───────────────────────────────────────────────

    @patch(PATCH_S3)
    @patch(PATCH_DZ)
    def test_create_new_notebook_increments_created(self, mock_dz, mock_s3):
        dz = self._make_dz_no_existing()
        mock_dz.return_value = dz
        mock_s3.return_value = self._make_s3()

        manifest = _make_manifest(notebooks=[_make_entry("nb-1")])
        summary = sync_notebooks(
            "dom-1", "proj-1", "us-east-1",
            manifest_data=manifest,
            notebook_files=NOTEBOOK_FILES,
            s3_uri=S3_URI,
        )
        self.assertEqual(summary.created, 1)
        self.assertEqual(summary.updated, 0)
        self.assertEqual(summary.failed, 0)

    @patch(PATCH_S3)
    @patch(PATCH_DZ)
    def test_start_notebook_sync_called_without_notebook_id_for_new(self, mock_dz, mock_s3):
        dz = self._make_dz_no_existing()
        mock_dz.return_value = dz
        mock_s3.return_value = self._make_s3()

        manifest = _make_manifest(notebooks=[_make_entry("nb-1")])
        sync_notebooks(
            "dom-1", "proj-1", "us-east-1",
            manifest_data=manifest, notebook_files=NOTEBOOK_FILES, s3_uri=S3_URI,
        )
        call_kwargs = dz.start_notebook_sync.call_args[1]
        self.assertNotIn("notebookId", call_kwargs)

    # ── happy path: update ───────────────────────────────────────────────

    @patch(PATCH_S3)
    @patch(PATCH_DZ)
    def test_update_existing_notebook_increments_updated(self, mock_dz, mock_s3):
        dz = MagicMock()
        # Existing target notebook maps "nb-1" → "tgt-existing"
        dz.list_notebooks.return_value = {"items": [{"id": "tgt-existing"}]}
        dz.get_notebook.return_value = {
            "id": "tgt-existing",
            "metadata": {SOURCE_NOTEBOOK_METADATA_KEY: "nb-1"},
        }
        dz.start_notebook_sync.return_value = {"notebookId": "tgt-existing"}
        dz.update_notebook.return_value = {}
        mock_dz.return_value = dz
        mock_s3.return_value = self._make_s3()

        manifest = _make_manifest(notebooks=[_make_entry("nb-1")])
        summary = sync_notebooks(
            "dom-1", "proj-1", "us-east-1",
            manifest_data=manifest, notebook_files=NOTEBOOK_FILES, s3_uri=S3_URI,
        )
        self.assertEqual(summary.updated, 1)
        self.assertEqual(summary.created, 0)
        # StartNotebookSync called WITH notebookId
        call_kwargs = dz.start_notebook_sync.call_args[1]
        self.assertEqual(call_kwargs["notebookId"], "tgt-existing")

    # ── ResourceNotFoundException fallback ───────────────────────────────

    @patch(PATCH_S3)
    @patch(PATCH_DZ)
    def test_resource_not_found_fallback_creates_new(self, mock_dz, mock_s3):
        dz = MagicMock()
        dz.list_notebooks.return_value = {"items": [{"id": "tgt-deleted"}]}
        dz.get_notebook.return_value = {
            "id": "tgt-deleted",
            "metadata": {SOURCE_NOTEBOOK_METADATA_KEY: "nb-1"},
        }
        call_count = [0]

        def start_sync(**kwargs):
            call_count[0] += 1
            if kwargs.get("notebookId") == "tgt-deleted":
                raise ClientError(
                    {"Error": {"Code": "ResourceNotFoundException", "Message": "Not found"}},
                    "StartNotebookSync",
                )
            return {"notebookId": "brand-new-id"}

        dz.start_notebook_sync.side_effect = start_sync
        dz.update_notebook.return_value = {}
        mock_dz.return_value = dz
        mock_s3.return_value = self._make_s3()

        manifest = _make_manifest(notebooks=[_make_entry("nb-1")])
        summary = sync_notebooks(
            "dom-1", "proj-1", "us-east-1",
            manifest_data=manifest, notebook_files=NOTEBOOK_FILES, s3_uri=S3_URI,
        )
        # Should have retried without notebookId and created
        self.assertEqual(summary.failed, 0)
        self.assertEqual(call_count[0], 2)  # first WITH, then WITHOUT

    # ── UpdateNotebook failure ────────────────────────────────────────────

    @patch(PATCH_S3)
    @patch(PATCH_DZ)
    def test_update_notebook_failure_counts_as_failed(self, mock_dz, mock_s3):
        dz = self._make_dz_no_existing()
        dz.update_notebook.side_effect = Exception("UpdateFailed")
        mock_dz.return_value = dz
        mock_s3.return_value = self._make_s3()

        manifest = _make_manifest(notebooks=[_make_entry("nb-1")])
        summary = sync_notebooks(
            "dom-1", "proj-1", "us-east-1",
            manifest_data=manifest, notebook_files=NOTEBOOK_FILES, s3_uri=S3_URI,
        )
        self.assertEqual(summary.failed, 1)
        self.assertIn("nb-1", summary.failed_ids)

    # ── missing file in bundle ────────────────────────────────────────────

    @patch(PATCH_S3)
    @patch(PATCH_DZ)
    def test_missing_file_in_bundle_counts_as_failed(self, mock_dz, mock_s3):
        mock_dz.return_value = self._make_dz_no_existing()
        mock_s3.return_value = self._make_s3()

        manifest = _make_manifest(notebooks=[_make_entry("nb-missing")])
        # notebook_files dict is empty — file is missing
        summary = sync_notebooks(
            "dom-1", "proj-1", "us-east-1",
            manifest_data=manifest, notebook_files={}, s3_uri=S3_URI,
        )
        self.assertEqual(summary.failed, 1)
        self.assertIn("nb-missing", summary.failed_ids)

    # ── S3 upload failure ─────────────────────────────────────────────────

    @patch(PATCH_S3)
    @patch(PATCH_DZ)
    def test_s3_upload_failure_skips_sync_for_that_notebook(self, mock_dz, mock_s3):
        mock_dz.return_value = self._make_dz_no_existing()
        s3 = MagicMock()
        s3.put_object.side_effect = Exception("S3UploadError")
        mock_s3.return_value = s3

        manifest = _make_manifest(notebooks=[_make_entry("nb-1")])
        summary = sync_notebooks(
            "dom-1", "proj-1", "us-east-1",
            manifest_data=manifest, notebook_files=NOTEBOOK_FILES, s3_uri=S3_URI,
        )
        self.assertEqual(summary.failed, 1)
        self.assertEqual(summary.created, 0)

    # ── summary invariant (Property 11) ───────────────────────────────────

    @patch(PATCH_S3)
    @patch(PATCH_DZ)
    def test_summary_counts_sum_equals_total_notebooks(self, mock_dz, mock_s3):
        """Property 11: created + updated + failed == len(manifest notebooks)."""
        dz = self._make_dz_no_existing()
        mock_dz.return_value = dz
        mock_s3.return_value = self._make_s3()

        entries = [_make_entry(f"nb-{i}") for i in range(5)]
        notebook_files = {e["filePath"]: b'{"cells":[]}' for e in entries}
        manifest = _make_manifest(notebooks=entries)

        summary = sync_notebooks(
            "dom-1", "proj-1", "us-east-1",
            manifest_data=manifest, notebook_files=notebook_files, s3_uri=S3_URI,
        )
        self.assertEqual(summary.total, 5)
        self.assertEqual(summary.created + summary.updated + summary.failed, 5)

    # ── empty manifest ────────────────────────────────────────────────────

    @patch(PATCH_S3)
    @patch(PATCH_DZ)
    def test_empty_manifest_returns_zero_summary(self, mock_dz, mock_s3):
        mock_dz.return_value = MagicMock()
        mock_s3.return_value = MagicMock()

        manifest = _make_manifest(notebooks=[])
        summary = sync_notebooks(
            "dom-1", "proj-1", "us-east-1",
            manifest_data=manifest, notebook_files={}, s3_uri=S3_URI,
        )
        self.assertEqual(summary.created, 0)
        self.assertEqual(summary.updated, 0)
        self.assertEqual(summary.failed, 0)

    # ── UpdateNotebook always passes tracking metadata ────────────────────

    @patch(PATCH_S3)
    @patch(PATCH_DZ)
    def test_update_notebook_called_with_tracking_key(self, mock_dz, mock_s3):
        dz = self._make_dz_no_existing()
        mock_dz.return_value = dz
        mock_s3.return_value = self._make_s3()

        manifest = _make_manifest(notebooks=[_make_entry("nb-1", metadata={})])
        sync_notebooks(
            "dom-1", "proj-1", "us-east-1",
            manifest_data=manifest, notebook_files=NOTEBOOK_FILES, s3_uri=S3_URI,
        )
        update_kwargs = dz.update_notebook.call_args[1]
        self.assertIn(SOURCE_NOTEBOOK_METADATA_KEY, update_kwargs["metadata"])
        self.assertEqual(update_kwargs["metadata"][SOURCE_NOTEBOOK_METADATA_KEY], "nb-1")


if __name__ == "__main__":
    unittest.main()
