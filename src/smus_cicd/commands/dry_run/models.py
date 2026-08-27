"""Data models for the deploy dry-run feature."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set


class Severity(enum.Enum):
    """Classification of a dry-run finding."""

    OK = "OK"
    WARNING = "WARNING"
    ERROR = "ERROR"


class Phase(enum.Enum):
    """Deployment phases in execution order."""

    MANIFEST_VALIDATION = "Manifest Validation"
    BUNDLE_EXPLORATION = "Bundle Exploration"
    PREFLIGHT = "Preflight Checks"
    PERMISSION_VERIFICATION = "Permission Verification"
    CONNECTIVITY = "Connectivity & Reachability"
    PROJECT_INIT = "Project Initialization"
    QUICKSIGHT = "QuickSight Deployment"
    STORAGE_DEPLOYMENT = "Storage Deployment"
    GIT_DEPLOYMENT = "Git Deployment"
    CATALOG_IMPORT = "Catalog Import"
    DEPENDENCY_VALIDATION = "Dependency Validation"
    WORKFLOW_VALIDATION = "Workflow Validation"
    NOTEBOOK_SYNC = "Notebook Sync"
    BOOTSTRAP_ACTIONS = "Bootstrap Actions"


@dataclass
class Finding:
    """A single validation finding produced by a checker."""

    severity: Severity
    message: str
    phase: Optional[Phase] = None
    resource: Optional[str] = None
    service: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


@dataclass
class DryRunContext:
    """Shared state passed between checkers during a dry-run execution."""

    manifest_file: str
    manifest: Optional[Any] = None
    stage_name: Optional[str] = None
    target_config: Optional[Any] = None
    config: Optional[Dict[str, Any]] = None
    bundle_path: Optional[str] = None
    bundle_files: Set[str] = field(default_factory=set)
    catalog_data: Optional[Dict[str, Any]] = None
    target_domain_id: Optional[str] = None
    target_region: Optional[str] = None


@dataclass
class DryRunReport:
    """Structured report collecting findings across all deployment phases."""

    findings_by_phase: Dict[Phase, List[Finding]] = field(default_factory=dict)

    def add_findings(self, phase: Phase, findings: List[Finding]) -> None:
        """Add findings for a phase, setting each finding's phase attribute."""
        for f in findings:
            f.phase = phase
        self.findings_by_phase.setdefault(phase, []).extend(findings)

    @property
    def ok_count(self) -> int:
        """Count of OK findings across all phases."""
        return sum(
            1
            for findings in self.findings_by_phase.values()
            for f in findings
            if f.severity == Severity.OK
        )

    @property
    def warning_count(self) -> int:
        """Count of WARNING findings across all phases."""
        return sum(
            1
            for findings in self.findings_by_phase.values()
            for f in findings
            if f.severity == Severity.WARNING
        )

    @property
    def error_count(self) -> int:
        """Count of ERROR findings across all phases."""
        return sum(
            1
            for findings in self.findings_by_phase.values()
            for f in findings
            if f.severity == Severity.ERROR
        )

    def has_blocking_errors(self, phase: Phase) -> bool:
        """Return True if the given phase has any ERROR findings."""
        return any(
            f.severity == Severity.ERROR for f in self.findings_by_phase.get(phase, [])
        )

    def render(self, output_format: str = "text") -> str:
        """Render the report in the requested format.

        Delegates to ReportFormatter (defined in report.py).
        """
        from smus_cicd.commands.dry_run.report import ReportFormatter

        if output_format == "json":
            return ReportFormatter.to_json(self)
        return ReportFormatter.to_text(self)
