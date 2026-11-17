"""Storage deployment simulation checker (Phase 7).

Simulates storage deployment for each storage item in
``deployment_configuration``. Reports target S3 bucket, prefix,
and file count per item.

- For items sourced from a bundle, counts files matching each storage item
  under ``context.bundle_files``.
- For items with no ``connectionName`` on their ``content.storage`` entry
  (local-filesystem items — no bundle is produced for these, see
  ``manifest_requires_bundle()``), counts files on disk by resolving the
  item's ``include`` glob patterns relative to the manifest's directory,
  mirroring ``deploy.py::_deploy_local_storage_item()``. Without this,
  ``context.bundle_files`` is empty for a local-only item and the checker
  always reports ``0 file(s) to deploy`` even though the real deploy finds
  and syncs real files.
- Storage items have: name, connectionName (derives bucket),
  targetDirectory (prefix).

Requirements: 5.2
"""

from __future__ import annotations

import glob
import logging
import os
from typing import List

from smus_cicd.commands.dry_run.checkers import get_project_connections
from smus_cicd.commands.dry_run.models import DryRunContext, Finding, Severity

logger = logging.getLogger(__name__)


class StorageChecker:
    """Simulates storage deployment and reports planned actions per item."""

    def check(self, context: DryRunContext) -> List[Finding]:
        findings: List[Finding] = []

        dep_cfg = getattr(context.target_config, "deployment_configuration", None)
        storage_items = getattr(dep_cfg, "storage", None) if dep_cfg else None

        if not storage_items:
            findings.append(
                Finding(
                    severity=Severity.OK,
                    message="No storage items configured; skipping.",
                    service="s3",
                )
            )
            return findings

        config = context.config or {}
        region = config.get("region")

        # Resolve real bucket names via DataZone project connections
        project_connections = get_project_connections(context, region)

        # Map deployment item name -> source content.storage entry, so items
        # with no connectionName (local filesystem, no bundle produced) can
        # have their file count resolved from disk instead of the bundle.
        content_map = {}
        manifest = getattr(context, "manifest", None)
        if manifest is not None:
            for content_item in getattr(manifest.content, "storage", None) or []:
                content_map[content_item.name] = content_item

        manifest_dir = None
        if context.manifest_file:
            manifest_dir = os.path.dirname(os.path.abspath(context.manifest_file))

        for item in storage_items:
            name = getattr(item, "name", str(item))
            connection = getattr(item, "connectionName", None) or ""
            target_dir = getattr(item, "targetDirectory", None) or ""

            # Resolve bucket name from DataZone connection
            bucket = None
            if connection and project_connections:
                conn_info = project_connections.get(connection)
                if conn_info:
                    s3_uri = conn_info.get("s3Uri", "")
                    if s3_uri:
                        bucket = s3_uri.replace("s3://", "").rstrip("/").split("/")[0]

            if not bucket:
                # Fallback: show connection name as-is
                bucket = f"<unresolved:{connection}>" if connection else "<unknown>"

            content_item = content_map.get(name)
            is_local = content_item is not None and not getattr(
                content_item, "connectionName", None
            )

            if is_local and manifest_dir:
                file_count = self._count_local_files(manifest_dir, content_item)
            else:
                # Count matching files in the bundle
                matching_files = [
                    f
                    for f in context.bundle_files
                    if f.startswith(f"{name}/") or f == name
                ]
                file_count = len(matching_files)

            findings.append(
                Finding(
                    severity=Severity.OK,
                    message=(
                        f"Storage item '{name}': "
                        f"target bucket '{bucket}', "
                        f"prefix '{target_dir}', "
                        f"{file_count} file(s) to deploy"
                    ),
                    resource=name,
                    service="s3",
                    details={
                        "bucket": bucket,
                        "prefix": target_dir,
                        "file_count": file_count,
                    },
                )
            )

        return findings

    @staticmethod
    def _count_local_files(manifest_dir: str, content_item) -> int:
        """Count files a local-filesystem storage item will deploy.

        Mirrors the collection logic in
        ``deploy.py::_deploy_local_storage_item()``: each ``include``
        pattern is resolved relative to ``manifest_dir`` and either walked
        (directory pattern) or globbed (file pattern). Deliberately does not
        deduplicate across overlapping patterns, so the preview matches the
        count ``deploy.py`` itself reports for the same deploy.
        """
        count = 0
        for pattern in getattr(content_item, "include", None) or []:
            full_pattern = os.path.join(manifest_dir, pattern)
            if os.path.isdir(full_pattern):
                for _root, _dirs, files in os.walk(full_pattern):
                    count += len(files)
            else:
                count += sum(
                    1
                    for f in glob.glob(full_pattern, recursive=True)
                    if os.path.isfile(f)
                )
        return count
