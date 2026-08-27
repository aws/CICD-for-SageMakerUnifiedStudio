"""
DataZone notebook export functionality for SMUS CI/CD CLI.

Exports SageMaker Unified Studio notebooks from a source DataZone project using
the DataZone Notebook APIs (GetNotebook, ListNotebooks, StartNotebookExport,
GetNotebookExport). Produces a ``notebooks/notebook_export_manifest.json`` and
one ``.ipynb`` file per notebook for inclusion in the bundle archive.

Design mirrors ``catalog_export.py`` — a single public ``export_notebooks()``
function backed by private helpers. All individual-notebook failures are
collected and reported; only a total ListNotebooks failure aborts early.
"""

import logging
import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from botocore.exceptions import ClientError

from .boto3_client import create_client

logger = logging.getLogger(__name__)

# ── constants ────────────────────────────────────────────────────────────────

# Metadata key used to track source notebook ID in target notebooks
SOURCE_NOTEBOOK_METADATA_KEY = "smus-cicd-source-notebook-id"

# Backoff settings for export polling (seconds)
_POLL_INITIAL_INTERVAL = 1.0
_POLL_MAX_INTERVAL = 30.0

# Throttle-retry settings for DataZone API calls
_THROTTLE_MAX_RETRIES = 3
_THROTTLE_INITIAL_DELAY = 1.0


# ── data models ──────────────────────────────────────────────────────────────


@dataclass
class ExportedNotebook:
    """Result of a single notebook export operation."""

    source_notebook_id: str
    name: str
    description: str
    file_content: bytes
    file_path: str  # relative path in bundle: notebooks/{sourceNotebookId}.ipynb
    exported_at: str  # ISO 8601
    parameters: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, str] = field(default_factory=dict)
    environment_configuration: Optional[Dict[str, Any]] = None


# ── DataZone client helper ───────────────────────────────────────────────────


def _get_datazone_client(region: str):
    """Create DataZone client with optional custom endpoint."""
    endpoint_url = os.environ.get("DATAZONE_ENDPOINT_URL")
    if endpoint_url:
        return create_client("datazone", region=region, endpoint_url=endpoint_url)
    return create_client("datazone", region=region)


def _get_s3_client(region: str):
    """Create S3 client."""
    return create_client("s3", region=region)


# ── throttle retry decorator ─────────────────────────────────────────────────


def _call_with_throttle_retry(
    func,
    max_retries: int = _THROTTLE_MAX_RETRIES,
    initial_delay: float = _THROTTLE_INITIAL_DELAY,
):
    """
    Call *func* (a zero-argument callable) retrying on ThrottlingException.

    Uses exponential backoff with jitter: delay = initial * 2^attempt + jitter.

    Args:
        func: Zero-argument callable wrapping the API call.
        max_retries: Maximum number of retry attempts after the initial call.
        initial_delay: Delay in seconds before the first retry.

    Returns:
        The return value of *func* on success.

    Raises:
        The last exception raised when all retries are exhausted.
    """
    for attempt in range(max_retries + 1):
        try:
            return func()
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code == "ThrottlingException" and attempt < max_retries:
                delay = initial_delay * (2**attempt) + random.uniform(0, 0.5)
                logger.debug(
                    "ThrottlingException on attempt %d/%d, retrying in %.1fs",
                    attempt + 1,
                    max_retries,
                    delay,
                )
                time.sleep(delay)
            else:
                raise


# ── internal helpers ─────────────────────────────────────────────────────────


def _validate_notebook_ids(
    client,
    domain_id: str,
    notebook_ids: List[str],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Validate ALL notebook IDs upfront via GetNotebook (fail-fast pass).

    Calls GetNotebook for each ID and collects both valid notebook details and
    invalid IDs. Returns after checking *every* ID — does not abort on first
    failure — so the error message can list all invalid entries at once.

    Args:
        client: DataZone boto3 client.
        domain_id: DataZone domain identifier.
        notebook_ids: List of notebook IDs to validate.

    Returns:
        Tuple of (valid_notebooks_with_details, invalid_ids).
        valid_notebooks_with_details contains the full GetNotebook response for
        each valid ID (parameters, metadata, environmentConfiguration included).
    """
    valid: List[Dict[str, Any]] = []
    invalid: List[str] = []

    for nb_id in notebook_ids:
        try:
            resp = _call_with_throttle_retry(
                lambda nb_id=nb_id: client.get_notebook(
                    domainIdentifier=domain_id,
                    identifier=nb_id,
                )
            )
            resp.pop("ResponseMetadata", None)
            valid.append(resp)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ResourceNotFoundException":
                invalid.append(nb_id)
            else:
                # Re-raise unexpected errors
                raise
        except Exception:
            raise

    return valid, invalid


def _list_all_notebooks(
    client,
    domain_id: str,
    project_id: str,
) -> List[Dict[str, Any]]:
    """
    List all ACTIVE notebooks owned by *project_id* via the ListNotebooks API.

    Follows pagination via ``nextToken`` until all pages are retrieved.

    Args:
        client: DataZone boto3 client.
        domain_id: DataZone domain identifier.
        project_id: DataZone project identifier.

    Returns:
        List of notebook summary dicts from ListNotebooks items.

    Raises:
        Exception: If the ListNotebooks API returns an error.
    """
    notebooks: List[Dict[str, Any]] = []
    next_token: Optional[str] = None

    while True:
        params: Dict[str, Any] = {
            "domainIdentifier": domain_id,
            "owningProjectIdentifier": project_id,
            "status": "ACTIVE",
        }
        if next_token:
            params["nextToken"] = next_token

        resp = _call_with_throttle_retry(
            lambda params=params: client.list_notebooks(**params)
        )
        items = resp.get("items", [])
        notebooks.extend(items)

        next_token = resp.get("nextToken")
        if not next_token:
            break

    return notebooks


def _get_notebook_details(
    client,
    domain_id: str,
    notebook_id: str,
) -> Dict[str, Any]:
    """
    Fetch full notebook details via GetNotebook.

    Returns the full response dict (ResponseMetadata stripped).
    """
    resp = _call_with_throttle_retry(
        lambda: client.get_notebook(
            domainIdentifier=domain_id,
            identifier=notebook_id,
        )
    )
    resp.pop("ResponseMetadata", None)
    return resp


def _compute_backoff_delay(
    attempt: int,
    initial: float = _POLL_INITIAL_INTERVAL,
    cap: float = _POLL_MAX_INTERVAL,
) -> float:
    """
    Compute exponential backoff delay with jitter for poll attempt *attempt*
    (0-indexed from the first wait).

    Formula: min(initial × 2^attempt, cap) + uniform jitter [0, 0.5s].

    Property 8: delay for attempt i = min(initial × 2^(i-1), max_interval)
    (i is 1-indexed; here we pass 0-indexed attempt so initial × 2^attempt
    equals initial × 2^(i-1) when i = attempt+1).
    """
    base = min(initial * (2**attempt), cap)
    return base + random.uniform(0, 0.5)


def _poll_export_status(
    client,
    domain_id: str,
    export_id: str,
    notebook_id: str,
    polling_timeout: int,
) -> Optional[str]:
    """
    Poll GetNotebookExport with exponential backoff until the export succeeds
    or fails.

    Args:
        client: DataZone boto3 client.
        domain_id: DataZone domain identifier.
        export_id: The export identifier returned by StartNotebookExport.
        notebook_id: Source notebook identifier (used in log messages).
        polling_timeout: Maximum seconds to wait before giving up.

    Returns:
        The S3 ``outputLocation`` URI on success, or ``None`` if the export
        failed or timed out.
    """
    start = time.monotonic()
    attempt = 0

    while True:
        resp = _call_with_throttle_retry(
            lambda: client.get_notebook_export(
                domainIdentifier=domain_id,
                identifier=export_id,
            )
        )
        resp.pop("ResponseMetadata", None)

        status = resp.get("status", "")

        if status == "SUCCEEDED":
            output_loc = resp.get("outputLocation")
            # outputLocation can be a dict like {"s3": {"uri": "s3://..."}} or a plain string
            if isinstance(output_loc, dict):
                s3_val = output_loc.get("s3", {})
                if isinstance(s3_val, dict):
                    return s3_val.get("uri") or s3_val.get("s3Uri")
                return s3_val  # s3_val is the URI string itself
            return output_loc

        if status == "FAILED":
            error_msg = resp.get("error", {}).get("message", "unknown error")
            logger.error(
                "Export FAILED for notebook %s (export %s): %s",
                notebook_id,
                export_id,
                error_msg,
            )
            return None

        elapsed = time.monotonic() - start
        if elapsed >= polling_timeout:
            logger.warning(
                "Export polling timed out for notebook %s after %.0fs (export %s)",
                notebook_id,
                elapsed,
                export_id,
            )
            return None

        delay = _compute_backoff_delay(attempt)
        remaining = polling_timeout - elapsed
        actual_delay = min(delay, remaining)
        time.sleep(actual_delay)
        attempt += 1


def _download_from_s3(s3_client, s3_uri: str) -> bytes:
    """
    Download an object from S3 and return its content as bytes.

    Args:
        s3_client: Boto3 S3 client.
        s3_uri: Full S3 URI of the form ``s3://bucket/key``.

    Returns:
        Raw bytes of the downloaded object.

    Raises:
        ValueError: If *s3_uri* is not a valid S3 URI.
        Exception: If the S3 download fails.
    """
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    without_scheme = s3_uri[5:]
    bucket, _, key = without_scheme.partition("/")
    if not bucket or not key:
        raise ValueError(f"Could not parse bucket/key from S3 URI: {s3_uri}")

    resp = s3_client.get_object(Bucket=bucket, Key=key)
    return resp["Body"].read()


def _export_single_notebook(
    dz_client,
    s3_client,
    domain_id: str,
    project_id: str,
    notebook_details: Dict[str, Any],
    polling_timeout: int,
) -> Optional[ExportedNotebook]:
    """
    Export a single notebook: StartNotebookExport → poll → download .ipynb.

    Args:
        dz_client: DataZone boto3 client.
        s3_client: S3 boto3 client.
        domain_id: DataZone domain identifier.
        project_id: DataZone project identifier (owningProjectIdentifier).
        notebook_details: Full GetNotebook response dict for this notebook.
        polling_timeout: Max seconds to wait for export to complete.

    Returns:
        ``ExportedNotebook`` on success, or ``None`` if the export failed.
    """
    notebook_id = (
        notebook_details.get("id")
        or notebook_details.get("notebookId")
        or notebook_details.get("identifier")
    )
    name = notebook_details.get("name", "")
    description = notebook_details.get("description", "") or ""
    parameters = notebook_details.get("parameters") or {}
    metadata = notebook_details.get("metadata") or {}
    env_config = notebook_details.get("environmentConfiguration")

    if not notebook_id:
        logger.error(
            "Cannot export notebook — missing identifier in details: %s",
            notebook_details,
        )
        return None

    # Start the export
    try:
        start_resp = _call_with_throttle_retry(
            lambda: dz_client.start_notebook_export(
                domainIdentifier=domain_id,
                notebookIdentifier=notebook_id,
                owningProjectIdentifier=project_id,
                fileFormat="IPYNB",
            )
        )
        export_id = start_resp.get("id")
    except Exception as exc:
        logger.error("StartNotebookExport failed for notebook %s: %s", notebook_id, exc)
        return None

    if not export_id:
        logger.error(
            "StartNotebookExport returned no export identifier for notebook %s",
            notebook_id,
        )
        return None

    logger.info("Started export %s for notebook %s", export_id, notebook_id)

    # Poll until done
    output_location = _poll_export_status(
        dz_client, domain_id, export_id, notebook_id, polling_timeout
    )
    if not output_location:
        return None

    # Download .ipynb from S3
    try:
        file_content = _download_from_s3(s3_client, output_location)
    except Exception as exc:
        logger.error(
            "Failed to download export for notebook %s from %s: %s",
            notebook_id,
            output_location,
            exc,
        )
        return None

    exported_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    file_path = f"notebooks/{notebook_id}.ipynb"

    return ExportedNotebook(
        source_notebook_id=notebook_id,
        name=name,
        description=description,
        file_content=file_content,
        file_path=file_path,
        exported_at=exported_at,
        parameters=parameters,
        metadata=metadata,
        environment_configuration=env_config,
    )


def _build_export_manifest(
    exported: List[ExportedNotebook],
    domain_id: str,
    project_id: str,
) -> Dict[str, Any]:
    """
    Build the ``notebook_export_manifest.json`` structure.

    Args:
        exported: List of successfully exported notebooks.
        domain_id: Source DataZone domain identifier.
        project_id: Source DataZone project identifier.

    Returns:
        Dict matching the NotebookExportManifest JSON schema.
    """
    export_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    notebooks_list = []
    for nb in exported:
        entry: Dict[str, Any] = {
            "sourceNotebookId": nb.source_notebook_id,
            "name": nb.name,
            "description": nb.description,
            "filePath": nb.file_path,
            "exportedAt": nb.exported_at,
            "parameters": nb.parameters,
            "metadata": nb.metadata,
            "environmentConfiguration": nb.environment_configuration,
        }
        notebooks_list.append(entry)

    return {
        "metadata": {
            "sourceProjectId": project_id,
            "sourceDomainId": domain_id,
            "exportTimestamp": export_timestamp,
            "notebookCount": len(notebooks_list),
        },
        "notebooks": notebooks_list,
    }


# ── public API ───────────────────────────────────────────────────────────────


def export_notebooks(
    domain_id: str,
    project_id: str,
    region: str,
    notebook_ids: Optional[List[str]] = None,
    polling_timeout: int = 300,
) -> Tuple[List[ExportedNotebook], Dict[str, Any]]:
    """
    Export notebooks from a DataZone project.

    Two modes:

    **notebook_ids specified** — fail-fast validation pass:
      1. Call GetNotebook for every ID in the list.
      2. If *any* ID is not found (ResourceNotFoundException), raise
         ``SystemExit`` listing all invalid IDs.  No exports are started.
      3. If all IDs are valid, export only those notebooks.

    **notebook_ids omitted** — discover and export all:
      1. Call ListNotebooks with ``status=ACTIVE`` (paginated) to discover all
         active notebooks owned by *project_id*.
      2. Export all discovered notebooks.

    In both modes individual export failures (StartNotebookExport failure,
    polling timeout, S3 download failure) are collected and reported; they do
    not abort remaining exports.

    Args:
        domain_id: DataZone domain identifier.
        project_id: DataZone project identifier (source project).
        region: AWS region string.
        notebook_ids: Optional list of specific notebook IDs to export.
        polling_timeout: Max seconds to wait per notebook export (default 300).

    Returns:
        Tuple of ``(exported_notebooks, manifest_dict)``.
        *exported_notebooks* contains only successfully exported notebooks.
        *manifest_dict* is the full ``notebook_export_manifest.json`` structure.

    Raises:
        SystemExit: If *notebook_ids* contains any invalid (not-found) IDs.
        Exception: If the ListNotebooks API fails entirely (discovery mode).
    """
    dz_client = _get_datazone_client(region)
    s3_client = _get_s3_client(region)

    # ── Step 1: resolve the list of notebooks to export ──────────────────────
    notebooks_to_export: List[Dict[str, Any]] = []

    if notebook_ids is not None:
        # Fail-fast validation pass — validate ALL before starting any export
        valid_notebooks, invalid_ids = _validate_notebook_ids(
            dz_client, domain_id, notebook_ids
        )

        if invalid_ids:
            # Report all invalid IDs and abort immediately
            ids_str = ", ".join(invalid_ids)
            raise SystemExit(
                f"Error: The following notebook_ids were not found in domain "
                f"'{domain_id}': {ids_str}\n"
                f"No notebooks were exported. Fix the notebook_ids list and retry."
            )

        logger.info(
            "Validated %d notebook ID(s) for project %s",
            len(valid_notebooks),
            project_id,
        )
        notebooks_to_export = valid_notebooks
    else:
        # Discovery mode — list all active notebooks
        try:
            notebook_summaries = _list_all_notebooks(dz_client, domain_id, project_id)
        except Exception as exc:
            raise Exception(
                f"ListNotebooks API failed for project '{project_id}' in domain "
                f"'{domain_id}': {exc}"
            ) from exc

        if not notebook_summaries:
            logger.info(
                "No active notebooks found for project %s in domain %s",
                project_id,
                domain_id,
            )
            manifest = _build_export_manifest([], domain_id, project_id)
            return [], manifest

        # Fetch full details for each summary (need parameters/metadata/envConfig)
        for summary in notebook_summaries:
            nb_id = (
                summary.get("id")
                or summary.get("notebookId")
                or summary.get("identifier")
            )
            if not nb_id:
                logger.warning("Skipping notebook summary with no ID: %s", summary)
                continue
            try:
                details = _get_notebook_details(dz_client, domain_id, nb_id)
                notebooks_to_export.append(details)
            except Exception as exc:
                logger.error(
                    "GetNotebook failed for notebook %s during discovery: %s",
                    nb_id,
                    exc,
                )
                # Count it as a failure below by not adding to the list —
                # we still proceed with the rest

    # ── Step 2: export each notebook ────────────────────────────────────────
    exported: List[ExportedNotebook] = []
    failed_ids: List[str] = []

    for notebook_details in notebooks_to_export:
        nb_id = (
            notebook_details.get("id")
            or notebook_details.get("notebookId")
            or notebook_details.get("identifier")
        )
        result = _export_single_notebook(
            dz_client,
            s3_client,
            domain_id,
            project_id,
            notebook_details,
            polling_timeout,
        )
        if result:
            exported.append(result)
            logger.info("Successfully exported notebook %s (%s)", nb_id, result.name)
        else:
            failed_ids.append(nb_id or "<unknown>")
            logger.warning("Failed to export notebook %s", nb_id)

    # ── Step 3: build manifest ───────────────────────────────────────────────
    manifest = _build_export_manifest(exported, domain_id, project_id)

    # ── Step 4: report failures ──────────────────────────────────────────────
    if failed_ids:
        failed_str = ", ".join(failed_ids)
        raise SystemExit(
            f"Error: {len(failed_ids)} notebook(s) failed to export: {failed_str}\n"
            f"Successfully exported: {len(exported)} notebook(s)."
        )

    return exported, manifest
