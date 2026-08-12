"""DryRunEngine orchestrator for deployment validation.

Instantiates all 12 phase-specific checkers in deployment order, runs each
checker against a shared DryRunContext, and collects findings into a
DryRunReport.  Implements fail-fast on manifest errors (Phase 1 ERROR →
return immediately) while continuing through all other phases even when
errors are found.
"""

from __future__ import annotations

import logging
from typing import List, Optional, Tuple

from smus_cicd.commands.dry_run.checkers import Checker
from smus_cicd.commands.dry_run.checkers.bootstrap_checker import BootstrapChecker
from smus_cicd.commands.dry_run.checkers.bundle_checker import BundleChecker
from smus_cicd.commands.dry_run.checkers.catalog_checker import CatalogChecker
from smus_cicd.commands.dry_run.checkers.connectivity_checker import ConnectivityChecker
from smus_cicd.commands.dry_run.checkers.dependency_checker import DependencyChecker
from smus_cicd.commands.dry_run.checkers.git_checker import GitChecker
from smus_cicd.commands.dry_run.checkers.manifest_checker import ManifestChecker
from smus_cicd.commands.dry_run.checkers.notebook_checker import NotebookChecker
from smus_cicd.commands.dry_run.checkers.permission_checker import PermissionChecker
from smus_cicd.commands.dry_run.checkers.preflight_checker import PreflightChecker
from smus_cicd.commands.dry_run.checkers.project_checker import ProjectChecker
from smus_cicd.commands.dry_run.checkers.quicksight_checker import QuickSightChecker
from smus_cicd.commands.dry_run.checkers.storage_checker import StorageChecker
from smus_cicd.commands.dry_run.checkers.workflow_checker import WorkflowChecker
from smus_cicd.commands.dry_run.models import (
    DryRunContext,
    DryRunReport,
    Finding,
    Phase,
    Severity,
)

logger = logging.getLogger(__name__)


class DryRunEngine:
    """Orchestrates all dry-run validation phases in deployment order."""

    def __init__(
        self,
        manifest_file: str,
        stage_name: str,
        bundle_path: Optional[str] = None,
        output_format: str = "text",
    ) -> None:
        self.manifest_file = manifest_file
        self.stage_name = stage_name
        self.bundle_path = bundle_path
        self.output_format = output_format

        # Checkers in phase execution order
        self._checkers: List[Tuple[Phase, Checker]] = [
            (Phase.MANIFEST_VALIDATION, ManifestChecker()),
            (Phase.BUNDLE_EXPLORATION, BundleChecker()),
            (Phase.PREFLIGHT, PreflightChecker()),
            (Phase.PERMISSION_VERIFICATION, PermissionChecker()),
            (Phase.CONNECTIVITY, ConnectivityChecker()),
            (Phase.PROJECT_INIT, ProjectChecker()),
            (Phase.QUICKSIGHT, QuickSightChecker()),
            (Phase.STORAGE_DEPLOYMENT, StorageChecker()),
            (Phase.GIT_DEPLOYMENT, GitChecker()),
            (Phase.CATALOG_IMPORT, CatalogChecker()),
            (Phase.DEPENDENCY_VALIDATION, DependencyChecker()),
            (Phase.WORKFLOW_VALIDATION, WorkflowChecker()),
            (Phase.NOTEBOOK_SYNC, NotebookChecker()),
            (Phase.BOOTSTRAP_ACTIONS, BootstrapChecker()),
        ]

    def run(self) -> DryRunReport:
        """Execute all dry-run phases and return the completed report."""
        context = DryRunContext(
            manifest_file=self.manifest_file,
            stage_name=self.stage_name,
            bundle_path=self.bundle_path,
        )
        report = DryRunReport()

        for phase, checker in self._checkers:
            try:
                findings = checker.check(context)
            except Exception as exc:
                logger.error(
                    "Checker for %s raised an unexpected error: %s",
                    phase.value,
                    exc,
                )
                findings = [
                    Finding(
                        severity=Severity.ERROR,
                        message=f"{phase.value} failed: {exc}",
                    )
                ]
            report.add_findings(phase, findings)

            # Fail-fast: if manifest validation has blocking errors, stop
            if phase == Phase.MANIFEST_VALIDATION and report.has_blocking_errors(
                Phase.MANIFEST_VALIDATION
            ):
                logger.warning(
                    "Manifest validation failed with errors — skipping remaining phases"
                )
                return report

            # Fail-fast: if preflight has blocking errors, stop
            if phase == Phase.PREFLIGHT and report.has_blocking_errors(Phase.PREFLIGHT):
                logger.warning("Preflight checks failed — skipping remaining phases")
                return report

            # After manifest validation succeeds, resolve target domain/region
            # so downstream checkers can query the target environment.
            if phase == Phase.MANIFEST_VALIDATION and context.config:
                self._resolve_target_identifiers(context)

        return report

    def _resolve_target_identifiers(self, context: DryRunContext) -> None:
        """Resolve target_domain_id and target_region from the manifest config."""
        config = context.config or {}
        region = config.get("region")
        if not region:
            raise ValueError(
                "Region not found in manifest config. "
                "Ensure domain.region is set in the target stage."
            )
        context.target_region = region

        domain_id = config.get("domain_id")
        if not domain_id:
            try:
                from smus_cicd.helpers.utils import _resolve_domain_id

                domain_id = _resolve_domain_id(config, region)
            except Exception:
                pass
        context.target_domain_id = domain_id
