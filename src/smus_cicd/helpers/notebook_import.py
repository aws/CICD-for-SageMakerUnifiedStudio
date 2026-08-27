"""
DataZone notebook sync (import) functionality for SMUS CI/CD CLI.

Syncs SageMaker Unified Studio notebooks into a target DataZone project using
the StartNotebookSync API. Follows the six-step deployment sequence:

  1. Upload all .ipynb files to S3 at {s3Uri}/notebooks/imports/{sourceNotebookId}.ipynb
  2. ListNotebooks + GetNotebook for each target notebook → read metadata
  3. Build {sourceNotebookId → targetNotebookId} map from smus-cicd-source-notebook-id
  4. For each manifest entry: StartNotebookSync WITH notebookId (update) or
     WITHOUT (create), with ResourceNotFoundException fallback to create
  5. UpdateNotebook to apply name, description, metadata (with tracking key),
     parameters, and environmentConfiguration
  6. Report created / updated / failed counts

Design mirrors ``catalog_import.py``.
"""

import enum
import hashlib
import logging
import os
import random
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from botocore.exceptions import ClientError

from .boto3_client import create_client

logger = logging.getLogger(__name__)

# ── constants ────────────────────────────────────────────────────────────────

SOURCE_NOTEBOOK_METADATA_KEY = "smus-cicd-source-notebook-id"

# Required top-level keys in notebook_export_manifest.json
_REQUIRED_MANIFEST_KEYS = {"metadata", "notebooks"}
_REQUIRED_METADATA_KEYS = {
    "sourceProjectId",
    "sourceDomainId",
    "exportTimestamp",
    "notebookCount",
}

# S3 import prefix inside the project's s3_shared bucket
_S3_IMPORT_PREFIX = "notebooks/imports"

# Throttle-retry settings
_THROTTLE_MAX_RETRIES = 3
_THROTTLE_INITIAL_DELAY = 1.0

# Max client token length enforced by the DataZone API
_CLIENT_TOKEN_MAX_LEN = 64


# ── data models ──────────────────────────────────────────────────────────────


class SyncStatus(enum.Enum):
    CREATED = "created"
    UPDATED = "updated"
    FAILED = "failed"


@dataclass
class SyncResult:
    """Outcome of syncing a single notebook."""

    status: SyncStatus
    source_notebook_id: str
    target_notebook_id: Optional[str] = None
    message: str = ""


@dataclass
class NotebookSyncSummary:
    """Aggregate summary of a notebook sync operation."""

    created: int = 0
    updated: int = 0
    failed: int = 0
    failed_ids: List[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.created + self.updated + self.failed

    @property
    def has_failures(self) -> bool:
        return self.failed > 0


# ── client helpers ───────────────────────────────────────────────────────────


def _get_datazone_client(region: str):
    endpoint_url = os.environ.get("DATAZONE_ENDPOINT_URL")
    if endpoint_url:
        return create_client("datazone", region=region, endpoint_url=endpoint_url)
    return create_client("datazone", region=region)


def _get_s3_client(region: str):
    return create_client("s3", region=region)


# ── throttle retry ───────────────────────────────────────────────────────────


def _call_with_throttle_retry(
    func,
    max_retries: int = _THROTTLE_MAX_RETRIES,
    initial_delay: float = _THROTTLE_INITIAL_DELAY,
):
    """Retry *func* on ThrottlingException with exponential backoff + jitter."""
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


# ── validation ───────────────────────────────────────────────────────────────


def _validate_notebook_manifest(manifest_data: Dict[str, Any]) -> None:
    """
    Validate that the manifest has required structure.

    Raises ValueError before any API calls if required keys are missing.

    Required top-level keys: ``metadata``, ``notebooks``
    Required metadata keys: ``sourceProjectId``, ``sourceDomainId``,
                            ``exportTimestamp``, ``notebookCount``
    """
    missing_top = _REQUIRED_MANIFEST_KEYS - set(manifest_data.keys())
    if missing_top:
        raise ValueError(
            f"notebook_export_manifest.json is missing required top-level "
            f"key(s): {sorted(missing_top)}"
        )

    metadata = manifest_data.get("metadata", {})
    missing_meta = _REQUIRED_METADATA_KEYS - set(metadata.keys())
    if missing_meta:
        raise ValueError(
            f"notebook_export_manifest.json metadata is missing required "
            f"key(s): {sorted(missing_meta)}"
        )


# ── S3 helpers ───────────────────────────────────────────────────────────────


def _parse_s3_uri(s3_uri: str):
    """Parse ``s3://bucket/prefix`` into ``(bucket, prefix)``."""
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {s3_uri!r}")
    without_scheme = s3_uri[5:]
    bucket, _, prefix = without_scheme.partition("/")
    return bucket, prefix.rstrip("/")


def _upload_notebook_to_s3(
    s3_client,
    s3_uri: str,
    source_notebook_id: str,
    content: bytes,
) -> str:
    """
    Upload a .ipynb file to S3.

    Key: ``{s3_uri}/notebooks/imports/{sourceNotebookId}.ipynb``

    Returns:
        Full S3 URI of the uploaded object.

    Raises:
        Exception: If the upload fails.
    """
    bucket, base_prefix = _parse_s3_uri(s3_uri)
    key = f"{base_prefix}/{_S3_IMPORT_PREFIX}/{source_notebook_id}.ipynb"
    key = key.lstrip("/")

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=content,
        ContentType="application/x-ipynb+json",
    )
    return f"s3://{bucket}/{key}"


# ── target notebook discovery ─────────────────────────────────────────────────


def _discover_target_notebooks(
    dz_client,
    domain_id: str,
    project_id: str,
) -> Dict[str, str]:
    """
    Build a ``{sourceNotebookId → targetNotebookId}`` mapping by inspecting
    the ``smus-cicd-source-notebook-id`` metadata key on every active notebook
    in the target project.

    Steps:
      1. ListNotebooks with owningProjectIdentifier + status=ACTIVE (paginated).
      2. GetNotebook for each discovered notebook.
      3. Read metadata[SOURCE_NOTEBOOK_METADATA_KEY] → target notebook ID mapping.

    Returns:
        Dict mapping source notebook ID → target notebook ID.
    """
    source_to_target: Dict[str, str] = {}

    # Paginated list
    next_token: Optional[str] = None
    target_ids: List[str] = []

    while True:
        params: Dict[str, Any] = {
            "domainIdentifier": domain_id,
            "owningProjectIdentifier": project_id,
            "status": "ACTIVE",
        }
        if next_token:
            params["nextToken"] = next_token

        resp = _call_with_throttle_retry(
            lambda params=params: dz_client.list_notebooks(**params)
        )
        for item in resp.get("items", []):
            nb_id = item.get("id") or item.get("notebookId") or item.get("identifier")
            if nb_id:
                target_ids.append(nb_id)

        next_token = resp.get("nextToken")
        if not next_token:
            break

    # GetNotebook for each to read metadata
    for target_id in target_ids:
        try:
            detail = _call_with_throttle_retry(
                lambda tid=target_id: dz_client.get_notebook(
                    domainIdentifier=domain_id,
                    identifier=tid,
                )
            )
            nb_metadata = detail.get("metadata") or {}
            source_id = nb_metadata.get(SOURCE_NOTEBOOK_METADATA_KEY)
            if source_id:
                source_to_target[source_id] = target_id
        except Exception as exc:
            logger.warning(
                "Could not GetNotebook for target notebook %s during discovery: %s",
                target_id,
                exc,
            )

    return source_to_target


# ── client token ─────────────────────────────────────────────────────────────


def _generate_client_token(source_notebook_id: str, timestamp: str) -> str:
    """
    Generate a deterministic, idempotent client token for StartNotebookSync.

    Derived from source notebook ID + deployment timestamp, truncated to
    _CLIENT_TOKEN_MAX_LEN characters.

    Property 5: same inputs → same output; distinct pairs → distinct tokens;
    len ≤ 64.
    """
    raw = f"{source_notebook_id}:{timestamp}"
    # Use SHA-256 hex digest (64 chars) which is exactly the limit
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return digest[:_CLIENT_TOKEN_MAX_LEN]


# ── UpdateNotebook kwargs builder ─────────────────────────────────────────────


def _build_update_kwargs(
    domain_id: str,
    target_notebook_id: str,
    notebook_entry: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Build the UpdateNotebook API kwargs from a manifest notebook entry.

    Rules (Property 7):
    - Always include ``name`` and ``description``.
    - Always include ``metadata`` with ``smus-cicd-source-notebook-id`` set,
      merged with any existing metadata from the manifest (even if empty dict).
    - Omit ``parameters`` when the entry's parameters is an empty dict.
    - Omit ``environmentConfiguration`` when the entry's value is None.
    """
    source_id = notebook_entry["sourceNotebookId"]

    # Build metadata: merge manifest metadata + tracking key
    manifest_metadata = dict(notebook_entry.get("metadata") or {})
    manifest_metadata[SOURCE_NOTEBOOK_METADATA_KEY] = source_id

    kwargs: Dict[str, Any] = {
        "domainIdentifier": domain_id,
        "identifier": target_notebook_id,
        "name": notebook_entry.get("name", ""),
        "description": notebook_entry.get("description", ""),
        "metadata": manifest_metadata,
    }

    # Omit parameters when empty
    parameters = notebook_entry.get("parameters") or {}
    if parameters:
        kwargs["parameters"] = parameters

    # Omit environmentConfiguration when None
    env_config = notebook_entry.get("environmentConfiguration")
    if env_config is not None:
        kwargs["environmentConfiguration"] = env_config

    return kwargs


# ── per-notebook sync ─────────────────────────────────────────────────────────


def _apply_notebook_metadata(
    dz_client,
    domain_id: str,
    target_notebook_id: str,
    notebook_entry: Dict[str, Any],
) -> bool:
    """
    Call UpdateNotebook to apply name, description, metadata (with tracking
    key), parameters, and environmentConfiguration from the manifest entry.

    Returns True on success, False on any error (notebook counts as FAILED).
    """
    kwargs = _build_update_kwargs(domain_id, target_notebook_id, notebook_entry)
    try:
        _call_with_throttle_retry(lambda: dz_client.update_notebook(**kwargs))
        return True
    except Exception as exc:
        logger.error(
            "UpdateNotebook failed for notebook %s (target %s): %s",
            notebook_entry.get("sourceNotebookId"),
            target_notebook_id,
            exc,
        )
        return False


def _wait_for_sync_complete(
    dz_client,
    domain_id: str,
    notebook_id: str,
    source_id: str,
    timeout: int = 120,
    poll_interval: float = 2.0,
) -> bool:
    """
    Poll GetNotebook until the notebook leaves SYNC_IN_PROGRESS status.

    Returns True if sync completed (status is ACTIVE or SYNC_FAILED),
    False if timed out.
    """
    import time as _time

    start = _time.monotonic()
    while True:
        try:
            resp = _call_with_throttle_retry(
                lambda: dz_client.get_notebook(
                    domainIdentifier=domain_id,
                    identifier=notebook_id,
                )
            )
            status = resp.get("status", "")
            if status != "SYNC_IN_PROGRESS":
                return True
        except Exception as exc:
            logger.debug(
                "GetNotebook failed while waiting for sync on %s: %s",
                notebook_id,
                exc,
            )

        elapsed = _time.monotonic() - start
        if elapsed >= timeout:
            logger.warning(
                "Timed out waiting for notebook %s (source %s) to leave SYNC_IN_PROGRESS after %.0fs",
                notebook_id,
                source_id,
                elapsed,
            )
            return False

        _time.sleep(poll_interval)


def _sync_single_notebook(
    dz_client,
    domain_id: str,
    project_id: str,
    notebook_entry: Dict[str, Any],
    s3_location: str,
    target_notebook_id: Optional[str],
    deployment_timestamp: str,
) -> SyncResult:
    """
    Sync a single notebook via StartNotebookSync then UpdateNotebook.

    Logic:
    - If target_notebook_id is set → call StartNotebookSync WITH notebookId (update).
      If ResourceNotFoundException → log warning, retry WITHOUT notebookId (create).
    - If target_notebook_id is None → call StartNotebookSync WITHOUT notebookId (create).
    - On success → call UpdateNotebook to apply metadata/config.
    - Any non-ResourceNotFoundException error from StartNotebookSync → FAILED.
    - Any UpdateNotebook error → FAILED.

    Returns a SyncResult with the appropriate status.
    """
    source_id = notebook_entry.get("sourceNotebookId", "")
    client_token = _generate_client_token(source_id, deployment_timestamp)

    def _do_sync(with_nb_id: Optional[str]) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "domainIdentifier": domain_id,
            "owningProjectIdentifier": project_id,
            "sourceLocation": {"s3": s3_location},
            "clientToken": client_token,
        }
        # Optional fields supported by StartNotebookSync
        if notebook_entry.get("name"):
            params["name"] = notebook_entry["name"]
        if notebook_entry.get("description"):
            params["description"] = notebook_entry["description"]
        if with_nb_id:
            params["notebookId"] = with_nb_id
        return _call_with_throttle_retry(
            lambda: dz_client.start_notebook_sync(**params)
        )

    intended_update = target_notebook_id is not None
    synced_as_update = False

    # Attempt sync
    try:
        if target_notebook_id:
            try:
                sync_resp = _do_sync(target_notebook_id)
                synced_as_update = True
            except ClientError as exc:
                if exc.response["Error"]["Code"] == "ResourceNotFoundException":
                    logger.warning(
                        "Target notebook %s not found (may have been manually deleted) "
                        "for source %s — creating new notebook instead.",
                        target_notebook_id,
                        source_id,
                    )
                    sync_resp = _do_sync(None)
                    synced_as_update = False
                else:
                    raise
        else:
            sync_resp = _do_sync(None)
            synced_as_update = False

    except Exception as exc:
        logger.error(
            "StartNotebookSync failed for source notebook %s: %s", source_id, exc
        )
        return SyncResult(
            status=SyncStatus.FAILED,
            source_notebook_id=source_id,
            message=str(exc),
        )

    # Extract the resulting target notebook ID
    new_target_id = (
        sync_resp.get("notebookId")
        or sync_resp.get("id")
        or sync_resp.get("identifier")
        or target_notebook_id  # fallback for in-place update
    )

    if not new_target_id:
        logger.error(
            "StartNotebookSync returned no notebook identifier for source %s", source_id
        )
        return SyncResult(
            status=SyncStatus.FAILED,
            source_notebook_id=source_id,
            message="StartNotebookSync returned no notebook identifier",
        )

    # Wait for the notebook to leave SYNC_IN_PROGRESS before updating metadata
    _wait_for_sync_complete(dz_client, domain_id, new_target_id, source_id)

    # Apply metadata via UpdateNotebook
    metadata_ok = _apply_notebook_metadata(
        dz_client, domain_id, new_target_id, notebook_entry
    )
    if not metadata_ok:
        return SyncResult(
            status=SyncStatus.FAILED,
            source_notebook_id=source_id,
            target_notebook_id=new_target_id,
            message="UpdateNotebook failed — see logs for details",
        )

    final_status = (
        SyncStatus.UPDATED
        if (intended_update or synced_as_update)
        else SyncStatus.CREATED
    )
    return SyncResult(
        status=final_status,
        source_notebook_id=source_id,
        target_notebook_id=new_target_id,
    )


# ── public API ────────────────────────────────────────────────────────────────


def sync_notebooks(
    domain_id: str,
    project_id: str,
    region: str,
    manifest_data: Dict[str, Any],
    notebook_files: Dict[str, bytes],
    s3_uri: str,
) -> NotebookSyncSummary:
    """
    Sync notebooks into a target DataZone project using StartNotebookSync.

    Six-step deployment sequence (strict order):

      Step 1: Upload all .ipynb files to S3 at
              ``{s3Uri}/notebooks/imports/{sourceNotebookId}.ipynb``
      Step 2: ListNotebooks + GetNotebook for each target notebook → read
              metadata
      Step 3: Build ``{sourceNotebookId → targetNotebookId}`` map from
              ``smus-cicd-source-notebook-id`` metadata key
      Step 4: For each manifest entry: StartNotebookSync WITH notebookId
              (update) or WITHOUT (create); ResourceNotFoundException on
              update → retry as create
      Step 5: UpdateNotebook — name, description, metadata (with tracking
              key), parameters, environmentConfiguration
      Step 6: Report created / updated / failed counts

    Args:
        domain_id: Target domain identifier.
        project_id: Target project identifier.
        region: AWS region string.
        manifest_data: Parsed ``notebook_export_manifest.json`` dict.
        notebook_files: Mapping of ``filePath`` (as in the manifest) to raw
            ``.ipynb`` bytes.
        s3_uri: S3 URI from the target project's ``default.s3_shared``
            connection (e.g. ``s3://bucket/prefix``).

    Returns:
        ``NotebookSyncSummary`` with counts of created, updated, and failed.

    Raises:
        ValueError: If the manifest is structurally invalid (before any API
            calls).
        ValueError: If *s3_uri* is missing or empty.
    """
    # ── pre-flight validation ────────────────────────────────────────────────
    _validate_notebook_manifest(manifest_data)

    if not s3_uri:
        raise ValueError(
            "Target project 'default.s3_shared' connection is missing or has "
            "no s3Uri. Cannot upload notebook files for sync."
        )

    dz_client = _get_datazone_client(region)
    s3_client = _get_s3_client(region)
    summary = NotebookSyncSummary()

    notebook_entries: List[Dict[str, Any]] = manifest_data.get("notebooks", [])
    if not notebook_entries:
        logger.info("Notebook export manifest contains no notebooks — nothing to sync.")
        return summary

    deployment_timestamp = str(int(time.time()))

    # ── Step 1: upload all .ipynb files to S3 ───────────────────────────────
    # Map sourceNotebookId → uploaded S3 URI (only successfully uploaded ones)
    uploaded_locations: Dict[str, str] = {}

    for entry in notebook_entries:
        source_id = entry.get("sourceNotebookId", "")
        file_path = entry.get("filePath", "")

        if file_path not in notebook_files:
            logger.error(
                "Notebook file missing from bundle for source %s (filePath=%s) — skipping",
                source_id,
                file_path,
            )
            summary.failed += 1
            summary.failed_ids.append(source_id)
            continue

        content = notebook_files[file_path]
        try:
            uploaded_uri = _upload_notebook_to_s3(s3_client, s3_uri, source_id, content)
            uploaded_locations[source_id] = uploaded_uri
            logger.info("Uploaded notebook %s to %s", source_id, uploaded_uri)
        except Exception as exc:
            logger.error(
                "S3 upload failed for notebook %s to %s: %s",
                source_id,
                f"{s3_uri}/{_S3_IMPORT_PREFIX}/{source_id}.ipynb",
                exc,
            )
            summary.failed += 1
            summary.failed_ids.append(source_id)

    # ── Steps 2–3: discover existing target notebooks and build source→target map ──
    source_to_target = _discover_target_notebooks(dz_client, domain_id, project_id)
    logger.info(
        "Found %d existing target notebook(s) with source tracking metadata",
        len(source_to_target),
    )

    # ── Steps 4–5: sync each notebook ────────────────────────────────────────
    for entry in notebook_entries:
        source_id = entry.get("sourceNotebookId", "")

        # Skip notebooks that failed upload
        if source_id not in uploaded_locations:
            continue

        s3_location = uploaded_locations[source_id]
        target_notebook_id = source_to_target.get(source_id)

        result = _sync_single_notebook(
            dz_client=dz_client,
            domain_id=domain_id,
            project_id=project_id,
            notebook_entry=entry,
            s3_location=s3_location,
            target_notebook_id=target_notebook_id,
            deployment_timestamp=deployment_timestamp,
        )

        if result.status == SyncStatus.CREATED:
            summary.created += 1
            logger.info(
                "Created target notebook %s from source %s",
                result.target_notebook_id,
                source_id,
            )
        elif result.status == SyncStatus.UPDATED:
            summary.updated += 1
            logger.info(
                "Updated target notebook %s from source %s",
                result.target_notebook_id,
                source_id,
            )
        else:
            summary.failed += 1
            summary.failed_ids.append(source_id)
            logger.error(
                "Failed to sync source notebook %s: %s",
                source_id,
                result.message,
            )

    # ── Step 6: report ───────────────────────────────────────────────────────
    logger.info(
        "Notebook sync complete: %d created, %d updated, %d failed",
        summary.created,
        summary.updated,
        summary.failed,
    )
    return summary
