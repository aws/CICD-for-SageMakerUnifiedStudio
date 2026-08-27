"""Unit tests for notebook_export.py.

Covers:
  - _validate_notebook_ids: fail-fast, collects all invalid IDs
  - _list_all_notebooks: pagination
  - _compute_backoff_delay: exponential cap
  - _poll_export_status: SUCCEEDED / FAILED / timeout paths
  - _build_export_manifest: schema correctness, empty list
  - export_notebooks: happy path, invalid IDs, partial failure, discovery mode
"""

import json
import unittest
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from smus_cicd.helpers.notebook_export import (
    ExportedNotebook,
    _build_export_manifest,
    _compute_backoff_delay,
    _list_all_notebooks,
    _poll_export_status,
    _validate_notebook_ids,
    export_notebooks,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _client_error(code, message="error"):
    return ClientError({"Error": {"Code": code, "Message": message}}, "Op")


def _make_get_notebook_response(
    nb_id, name="Test NB", params=None, meta=None, env=None
):
    return {
        "id": nb_id,
        "name": name,
        "description": f"Desc of {nb_id}",
        "parameters": params or {},
        "metadata": meta or {},
        "environmentConfiguration": env,
    }


def _make_exported_notebook(nb_id, name="NB"):
    return ExportedNotebook(
        source_notebook_id=nb_id,
        name=name,
        description="desc",
        file_content=b'{"cells":[]}',
        file_path=f"notebooks/{nb_id}.ipynb",
        exported_at="2024-01-01T00:00:00Z",
        parameters={},
        metadata={},
        environment_configuration=None,
    )


# ---------------------------------------------------------------------------
# _validate_notebook_ids
# ---------------------------------------------------------------------------


class TestValidateNotebookIds(unittest.TestCase):

    def _mock_client(self, valid_ids, invalid_ids):
        """Build a mock DZ client that succeeds for valid_ids, raises ResourceNotFoundException for invalid_ids."""
        client = MagicMock()

        def get_notebook(domainIdentifier, identifier):
            if identifier in invalid_ids:
                raise _client_error("ResourceNotFoundException")
            return _make_get_notebook_response(identifier)

        client.get_notebook.side_effect = get_notebook
        return client

    def test_all_valid_returns_details_empty_invalid_list(self):
        client = self._mock_client(["nb-1", "nb-2"], [])
        valid, invalid = _validate_notebook_ids(client, "dom-1", ["nb-1", "nb-2"])
        self.assertEqual(len(valid), 2)
        self.assertEqual(invalid, [])

    def test_all_invalid_returns_empty_valid_all_invalid_ids(self):
        client = self._mock_client([], ["nb-x", "nb-y"])
        valid, invalid = _validate_notebook_ids(client, "dom-1", ["nb-x", "nb-y"])
        self.assertEqual(valid, [])
        self.assertEqual(sorted(invalid), ["nb-x", "nb-y"])

    def test_partial_invalid_collects_all_invalid(self):
        """Fail-fast means we validate ALL IDs, not just until first failure."""
        client = self._mock_client(["nb-1"], ["nb-bad1", "nb-bad2"])
        valid, invalid = _validate_notebook_ids(
            client, "dom-1", ["nb-1", "nb-bad1", "nb-bad2"]
        )
        self.assertEqual(len(valid), 1)
        self.assertEqual(sorted(invalid), ["nb-bad1", "nb-bad2"])
        # GetNotebook must have been called for every ID — not short-circuited
        self.assertEqual(client.get_notebook.call_count, 3)

    def test_valid_notebook_details_contain_parameters_metadata(self):
        client = MagicMock()
        client.get_notebook.return_value = _make_get_notebook_response(
            "nb-1",
            params={"key": "val"},
            meta={"owner": "team"},
            env={
                "imageVersion": "v1",
                "packageConfig": {"packageManager": "pip", "packageSpecification": ""},
            },
        )
        valid, _ = _validate_notebook_ids(client, "dom-1", ["nb-1"])
        self.assertEqual(valid[0]["parameters"], {"key": "val"})
        self.assertEqual(valid[0]["metadata"], {"owner": "team"})
        self.assertIsNotNone(valid[0]["environmentConfiguration"])

    def test_non_resource_not_found_error_propagates(self):
        client = MagicMock()
        client.get_notebook.side_effect = _client_error("ThrottlingException")
        with self.assertRaises(ClientError):
            _validate_notebook_ids(client, "dom-1", ["nb-1"])

    def test_response_metadata_stripped(self):
        """ResponseMetadata must not appear in returned notebook details."""
        client = MagicMock()
        resp = _make_get_notebook_response("nb-1")
        resp["ResponseMetadata"] = {"RequestId": "req-123"}
        client.get_notebook.return_value = resp
        valid, _ = _validate_notebook_ids(client, "dom-1", ["nb-1"])
        self.assertNotIn("ResponseMetadata", valid[0])


# ---------------------------------------------------------------------------
# _list_all_notebooks
# ---------------------------------------------------------------------------


class TestListAllNotebooks(unittest.TestCase):

    def test_single_page_returns_all_items(self):
        client = MagicMock()
        client.list_notebooks.return_value = {"items": [{"id": "nb-1"}, {"id": "nb-2"}]}
        result = _list_all_notebooks(client, "dom-1", "proj-1")
        self.assertEqual(len(result), 2)

    def test_pagination_follows_next_token(self):
        client = MagicMock()
        client.list_notebooks.side_effect = [
            {"items": [{"id": "nb-1"}], "nextToken": "tok-1"},
            {"items": [{"id": "nb-2"}], "nextToken": "tok-2"},
            {"items": [{"id": "nb-3"}]},
        ]
        result = _list_all_notebooks(client, "dom-1", "proj-1")
        self.assertEqual(len(result), 3)
        self.assertEqual(client.list_notebooks.call_count, 3)
        # Second call must include nextToken
        second_call_kwargs = client.list_notebooks.call_args_list[1][1]
        self.assertEqual(second_call_kwargs["nextToken"], "tok-1")

    def test_empty_project_returns_empty_list(self):
        client = MagicMock()
        client.list_notebooks.return_value = {"items": []}
        result = _list_all_notebooks(client, "dom-1", "proj-1")
        self.assertEqual(result, [])

    def test_status_active_filter_sent(self):
        client = MagicMock()
        client.list_notebooks.return_value = {"items": []}
        _list_all_notebooks(client, "dom-1", "proj-1")
        kwargs = client.list_notebooks.call_args[1]
        self.assertEqual(kwargs["status"], "ACTIVE")
        self.assertEqual(kwargs["owningProjectIdentifier"], "proj-1")
        self.assertEqual(kwargs["domainIdentifier"], "dom-1")

    def test_api_error_propagates(self):
        client = MagicMock()
        client.list_notebooks.side_effect = Exception("ServiceError")
        with self.assertRaises(Exception, msg="ServiceError"):
            _list_all_notebooks(client, "dom-1", "proj-1")


# ---------------------------------------------------------------------------
# _compute_backoff_delay
# ---------------------------------------------------------------------------


class TestComputeBackoffDelay(unittest.TestCase):
    """Property 8: delay for attempt i equals min(initial × 2^i, max_interval)."""

    def test_first_attempt_starts_at_initial(self):
        delay = _compute_backoff_delay(0, initial=1.0, cap=30.0)
        # 1.0 * 2^0 = 1.0 (plus up to 0.5 jitter)
        self.assertGreaterEqual(delay, 1.0)
        self.assertLessEqual(delay, 1.5)

    def test_delay_doubles_each_attempt(self):
        # Without jitter randomness affecting assertions, test the base (cap not hit)
        # Use large cap so cap doesn't kick in for early attempts
        d0_base = 1.0  # 1 * 2^0
        d1_base = 2.0  # 1 * 2^1
        d2_base = 4.0  # 1 * 2^2
        # Run many samples — each should be within [base, base+0.5]
        for _ in range(20):
            d0 = _compute_backoff_delay(0, initial=1.0, cap=100.0)
            d1 = _compute_backoff_delay(1, initial=1.0, cap=100.0)
            d2 = _compute_backoff_delay(2, initial=1.0, cap=100.0)
            self.assertGreaterEqual(d0, d0_base)
            self.assertLessEqual(d0, d0_base + 0.5)
            self.assertGreaterEqual(d1, d1_base)
            self.assertLessEqual(d1, d1_base + 0.5)
            self.assertGreaterEqual(d2, d2_base)
            self.assertLessEqual(d2, d2_base + 0.5)

    def test_delay_capped_at_max_interval(self):
        # Attempt 10 with initial=1 would be 1024s without cap
        for _ in range(20):
            delay = _compute_backoff_delay(10, initial=1.0, cap=30.0)
            self.assertLessEqual(delay, 30.5)
            self.assertGreaterEqual(delay, 30.0)

    def test_cap_respected_before_large_attempt(self):
        # At attempt 5, 1 * 2^5 = 32 > cap=30 → should be 30 + jitter
        for _ in range(10):
            delay = _compute_backoff_delay(5, initial=1.0, cap=30.0)
            self.assertGreaterEqual(delay, 30.0)
            self.assertLessEqual(delay, 30.5)


# ---------------------------------------------------------------------------
# _poll_export_status
# ---------------------------------------------------------------------------


class TestPollExportStatus(unittest.TestCase):

    @patch("smus_cicd.helpers.notebook_export.time.sleep")
    @patch("smus_cicd.helpers.notebook_export.time.monotonic")
    def test_succeeded_returns_output_location(self, mock_mono, mock_sleep):
        mock_mono.side_effect = [0.0, 1.0]  # start, then first poll elapsed
        client = MagicMock()
        client.get_notebook_export.return_value = {
            "status": "SUCCEEDED",
            "outputLocation": "s3://bucket/export.ipynb",
        }
        result = _poll_export_status(client, "dom-1", "exp-1", "nb-1", 300)
        self.assertEqual(result, "s3://bucket/export.ipynb")

    @patch("smus_cicd.helpers.notebook_export.time.sleep")
    @patch("smus_cicd.helpers.notebook_export.time.monotonic")
    def test_failed_status_returns_none(self, mock_mono, mock_sleep):
        mock_mono.side_effect = [0.0, 1.0]
        client = MagicMock()
        client.get_notebook_export.return_value = {
            "status": "FAILED",
            "error": {"message": "Export error"},
        }
        result = _poll_export_status(client, "dom-1", "exp-1", "nb-1", 300)
        self.assertIsNone(result)

    @patch("smus_cicd.helpers.notebook_export.time.sleep")
    @patch("smus_cicd.helpers.notebook_export.time.monotonic")
    def test_timeout_returns_none(self, mock_mono, mock_sleep):
        # monotonic always returns elapsed > timeout so we time out immediately
        mock_mono.side_effect = [0.0, 400.0, 400.0]
        client = MagicMock()
        client.get_notebook_export.return_value = {"status": "IN_PROGRESS"}
        result = _poll_export_status(
            client, "dom-1", "exp-1", "nb-1", polling_timeout=300
        )
        self.assertIsNone(result)

    @patch("smus_cicd.helpers.notebook_export.time.sleep")
    @patch("smus_cicd.helpers.notebook_export.time.monotonic")
    def test_polls_multiple_times_before_success(self, mock_mono, mock_sleep):
        # Three IN_PROGRESS polls, then SUCCEEDED
        mock_mono.side_effect = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]
        client = MagicMock()
        client.get_notebook_export.side_effect = [
            {"status": "IN_PROGRESS"},
            {"status": "IN_PROGRESS"},
            {"status": "SUCCEEDED", "outputLocation": "s3://b/f.ipynb"},
        ]
        result = _poll_export_status(client, "dom-1", "exp-1", "nb-1", 300)
        self.assertEqual(result, "s3://b/f.ipynb")
        self.assertEqual(client.get_notebook_export.call_count, 3)
        # sleep called between polls
        self.assertGreaterEqual(mock_sleep.call_count, 2)


# ---------------------------------------------------------------------------
# _build_export_manifest
# ---------------------------------------------------------------------------


class TestBuildExportManifest(unittest.TestCase):

    def test_schema_has_metadata_and_notebooks_keys(self):
        manifest = _build_export_manifest([], "dom-1", "proj-1")
        self.assertIn("metadata", manifest)
        self.assertIn("notebooks", manifest)
        self.assertEqual(set(manifest.keys()), {"metadata", "notebooks"})

    def test_metadata_fields_present(self):
        manifest = _build_export_manifest([], "dom-1", "proj-1")
        meta = manifest["metadata"]
        self.assertEqual(meta["sourceDomainId"], "dom-1")
        self.assertEqual(meta["sourceProjectId"], "proj-1")
        self.assertIn("exportTimestamp", meta)
        self.assertEqual(meta["notebookCount"], 0)

    def test_notebook_count_matches_list_length(self):
        notebooks = [
            _make_exported_notebook("nb-1"),
            _make_exported_notebook("nb-2"),
        ]
        manifest = _build_export_manifest(notebooks, "dom-1", "proj-1")
        self.assertEqual(manifest["metadata"]["notebookCount"], 2)
        self.assertEqual(len(manifest["notebooks"]), 2)

    def test_notebook_entry_has_all_required_fields(self):
        nb = ExportedNotebook(
            source_notebook_id="nb-abc",
            name="My Notebook",
            description="A test",
            file_content=b"{}",
            file_path="notebooks/nb-abc.ipynb",
            exported_at="2024-01-01T00:00:00Z",
            parameters={"k": "v"},
            metadata={"owner": "team"},
            environment_configuration={
                "imageVersion": "v1",
                "packageConfig": {"packageManager": "pip", "packageSpecification": ""},
            },
        )
        manifest = _build_export_manifest([nb], "dom-1", "proj-1")
        entry = manifest["notebooks"][0]
        required = {
            "sourceNotebookId",
            "name",
            "description",
            "filePath",
            "exportedAt",
            "parameters",
            "metadata",
            "environmentConfiguration",
        }
        self.assertEqual(set(entry.keys()), required)
        self.assertEqual(entry["sourceNotebookId"], "nb-abc")
        self.assertEqual(entry["filePath"], "notebooks/nb-abc.ipynb")

    def test_empty_notebooks_produces_count_zero(self):
        manifest = _build_export_manifest([], "dom-1", "proj-1")
        self.assertEqual(manifest["metadata"]["notebookCount"], 0)
        self.assertEqual(manifest["notebooks"], [])

    def test_manifest_is_json_serialisable(self):
        notebooks = [_make_exported_notebook("nb-1"), _make_exported_notebook("nb-2")]
        manifest = _build_export_manifest(notebooks, "dom-1", "proj-1")
        # Must not raise
        json_str = json.dumps(manifest)
        parsed = json.loads(json_str)
        self.assertEqual(parsed["metadata"]["notebookCount"], 2)


# ---------------------------------------------------------------------------
# export_notebooks (integration of all internal pieces)
# ---------------------------------------------------------------------------

PATCH_DZ = "smus_cicd.helpers.notebook_export._get_datazone_client"
PATCH_S3 = "smus_cicd.helpers.notebook_export._get_s3_client"


class TestExportNotebooks(unittest.TestCase):

    def _make_dz_client(self, valid_ids=None, notebook_details=None):
        """Build a DZ mock that succeeds for the given IDs."""
        valid_ids = valid_ids or []
        notebook_details = notebook_details or {}

        def get_notebook(domainIdentifier, identifier):
            if identifier not in valid_ids:
                raise _client_error("ResourceNotFoundException")
            return notebook_details.get(
                identifier,
                _make_get_notebook_response(identifier),
            )

        client = MagicMock()
        client.get_notebook.side_effect = get_notebook
        return client

    def _make_s3_client_with_content(self, content=b'{"cells":[]}'):
        s3 = MagicMock()
        s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=content))
        }
        return s3

    # ── notebook_ids specified path ──────────────────────────────────────

    @patch(PATCH_S3)
    @patch(PATCH_DZ)
    def test_invalid_ids_raises_system_exit(self, mock_dz, mock_s3):
        mock_dz.return_value = self._make_dz_client(
            valid_ids=["nb-1"], notebook_details={}
        )
        mock_s3.return_value = MagicMock()
        with self.assertRaises(SystemExit) as ctx:
            export_notebooks(
                "dom-1", "proj-1", "us-east-1", notebook_ids=["nb-1", "nb-bad"]
            )
        self.assertIn("nb-bad", str(ctx.exception))

    @patch(PATCH_S3)
    @patch(PATCH_DZ)
    def test_all_invalid_ids_listed_in_error(self, mock_dz, mock_s3):
        dz = MagicMock()
        dz.get_notebook.side_effect = _client_error("ResourceNotFoundException")
        mock_dz.return_value = dz
        mock_s3.return_value = MagicMock()
        with self.assertRaises(SystemExit) as ctx:
            export_notebooks(
                "dom-1", "proj-1", "us-east-1", notebook_ids=["nb-a", "nb-b"]
            )
        error_msg = str(ctx.exception)
        self.assertIn("nb-a", error_msg)
        self.assertIn("nb-b", error_msg)

    @patch(PATCH_S3)
    @patch(PATCH_DZ)
    def test_valid_ids_exports_and_returns_manifest(self, mock_dz, mock_s3):
        dz = MagicMock()
        dz.get_notebook.return_value = _make_get_notebook_response("nb-1")
        dz.start_notebook_export.return_value = {"id": "exp-1"}
        dz.get_notebook_export.return_value = {
            "status": "SUCCEEDED",
            "outputLocation": "s3://bucket/nb-1.ipynb",
        }
        mock_dz.return_value = dz
        mock_s3.return_value = self._make_s3_client_with_content()

        exported, manifest = export_notebooks(
            "dom-1", "proj-1", "us-east-1", notebook_ids=["nb-1"]
        )
        self.assertEqual(len(exported), 1)
        self.assertEqual(exported[0].source_notebook_id, "nb-1")
        self.assertEqual(manifest["metadata"]["notebookCount"], 1)

    @patch(PATCH_S3)
    @patch(PATCH_DZ)
    def test_start_export_failure_raises_system_exit(self, mock_dz, mock_s3):
        """When StartNotebookExport fails, the notebook is counted as failed → SystemExit."""
        dz = MagicMock()
        dz.get_notebook.return_value = _make_get_notebook_response("nb-1")
        dz.start_notebook_export.side_effect = Exception("StartExportError")
        mock_dz.return_value = dz
        mock_s3.return_value = MagicMock()

        with self.assertRaises(SystemExit):
            export_notebooks("dom-1", "proj-1", "us-east-1", notebook_ids=["nb-1"])

    # ── discovery mode (notebook_ids omitted) ────────────────────────────

    @patch(PATCH_S3)
    @patch(PATCH_DZ)
    def test_discovery_mode_lists_and_exports_notebooks(self, mock_dz, mock_s3):
        dz = MagicMock()
        # ListNotebooks returns one notebook summary
        dz.list_notebooks.return_value = {"items": [{"id": "nb-2", "name": "NB2"}]}
        # GetNotebook (for full details) returns full response
        dz.get_notebook.return_value = _make_get_notebook_response("nb-2")
        dz.start_notebook_export.return_value = {"id": "exp-2"}
        dz.get_notebook_export.return_value = {
            "status": "SUCCEEDED",
            "outputLocation": "s3://bucket/nb-2.ipynb",
        }
        mock_dz.return_value = dz
        mock_s3.return_value = self._make_s3_client_with_content()

        exported, manifest = export_notebooks("dom-1", "proj-1", "us-east-1")
        self.assertEqual(len(exported), 1)
        self.assertEqual(manifest["metadata"]["notebookCount"], 1)

    @patch(PATCH_S3)
    @patch(PATCH_DZ)
    def test_discovery_mode_empty_project_returns_empty_manifest(
        self, mock_dz, mock_s3
    ):
        dz = MagicMock()
        dz.list_notebooks.return_value = {"items": []}
        mock_dz.return_value = dz
        mock_s3.return_value = MagicMock()

        exported, manifest = export_notebooks("dom-1", "proj-1", "us-east-1")
        self.assertEqual(exported, [])
        self.assertEqual(manifest["metadata"]["notebookCount"], 0)
        self.assertEqual(manifest["notebooks"], [])

    @patch(PATCH_S3)
    @patch(PATCH_DZ)
    def test_list_notebooks_failure_raises_exception(self, mock_dz, mock_s3):
        dz = MagicMock()
        dz.list_notebooks.side_effect = Exception("ListNotebooksError")
        mock_dz.return_value = dz
        mock_s3.return_value = MagicMock()

        with self.assertRaises(Exception, msg="ListNotebooksError"):
            export_notebooks("dom-1", "proj-1", "us-east-1")

    @patch(PATCH_S3)
    @patch(PATCH_DZ)
    def test_partial_export_failure_raises_system_exit(self, mock_dz, mock_s3):
        """One notebook fails export, one succeeds → SystemExit with failed ID listed."""
        dz = MagicMock()
        dz.list_notebooks.return_value = {"items": [{"id": "nb-ok"}, {"id": "nb-fail"}]}

        def get_notebook(domainIdentifier, identifier):
            return _make_get_notebook_response(identifier)

        def start_export(domainIdentifier, notebookIdentifier, **kwargs):
            if notebookIdentifier == "nb-fail":
                raise Exception("ExportError")
            return {"id": "exp-ok"}

        dz.get_notebook.side_effect = get_notebook
        dz.start_notebook_export.side_effect = start_export
        dz.get_notebook_export.return_value = {
            "status": "SUCCEEDED",
            "outputLocation": "s3://bucket/nb-ok.ipynb",
        }
        mock_dz.return_value = dz
        mock_s3.return_value = self._make_s3_client_with_content()

        with self.assertRaises(SystemExit) as ctx:
            export_notebooks("dom-1", "proj-1", "us-east-1")
        self.assertIn("nb-fail", str(ctx.exception))

    @patch(PATCH_S3)
    @patch(PATCH_DZ)
    def test_export_file_content_saved_in_returned_object(self, mock_dz, mock_s3):
        ipynb_content = b'{"cells": [], "metadata": {}, "nbformat": 4}'
        dz = MagicMock()
        dz.get_notebook.return_value = _make_get_notebook_response("nb-1")
        dz.start_notebook_export.return_value = {"id": "exp-1"}
        dz.get_notebook_export.return_value = {
            "status": "SUCCEEDED",
            "outputLocation": "s3://bucket/nb-1.ipynb",
        }
        mock_dz.return_value = dz
        s3 = MagicMock()
        s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=ipynb_content))
        }
        mock_s3.return_value = s3

        exported, _ = export_notebooks(
            "dom-1", "proj-1", "us-east-1", notebook_ids=["nb-1"]
        )
        self.assertEqual(exported[0].file_content, ipynb_content)

    @patch(PATCH_S3)
    @patch(PATCH_DZ)
    def test_file_path_uses_notebook_id(self, mock_dz, mock_s3):
        dz = MagicMock()
        dz.get_notebook.return_value = _make_get_notebook_response("nb-abc123")
        dz.start_notebook_export.return_value = {"id": "exp-1"}
        dz.get_notebook_export.return_value = {
            "status": "SUCCEEDED",
            "outputLocation": "s3://bucket/nb.ipynb",
        }
        mock_dz.return_value = dz
        mock_s3.return_value = self._make_s3_client_with_content()

        exported, _ = export_notebooks(
            "dom-1", "proj-1", "us-east-1", notebook_ids=["nb-abc123"]
        )
        self.assertEqual(exported[0].file_path, "notebooks/nb-abc123.ipynb")


if __name__ == "__main__":
    unittest.main()
