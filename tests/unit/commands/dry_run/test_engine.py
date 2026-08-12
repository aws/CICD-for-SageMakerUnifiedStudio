"""Unit tests for the DryRunEngine orchestrator."""

from __future__ import annotations

from typing import List
from unittest.mock import MagicMock

from smus_cicd.commands.dry_run.engine import DryRunEngine
from smus_cicd.commands.dry_run.models import (
    DryRunContext,
    Finding,
    Phase,
    Severity,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_checker_mock(findings: List[Finding]) -> MagicMock:
    """Return a mock checker whose check() returns the given findings."""
    mock = MagicMock()
    mock.check.return_value = findings
    return mock


PHASE_ORDER = list(Phase)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPhaseOrdering:
    """Verify all phases run in the correct order."""

    def test_all_phases_run_in_order(self):
        """When manifest validation passes, all phases should execute in order."""
        engine = DryRunEngine(
            manifest_file="manifest.yaml",
            stage_name="dev",
        )

        call_order: list[Phase] = []

        def _make_tracking_checker(phase: Phase) -> MagicMock:
            mock = MagicMock()

            def track_check(context):
                call_order.append(phase)
                return [Finding(severity=Severity.OK, message=f"{phase.value} OK")]

            mock.check.side_effect = track_check
            return mock

        # Replace all checkers with tracking mocks
        engine._checkers = [
            (phase, _make_tracking_checker(phase)) for phase in PHASE_ORDER
        ]

        report = engine.run()

        assert call_order == PHASE_ORDER
        assert len(report.findings_by_phase) == len(PHASE_ORDER)
        for phase in PHASE_ORDER:
            assert phase in report.findings_by_phase


class TestEarlyTermination:
    """Verify fail-fast on manifest validation errors."""

    def test_manifest_error_stops_execution(self):
        """When ManifestChecker returns ERROR, engine stops after Phase 1."""
        engine = DryRunEngine(
            manifest_file="manifest.yaml",
            stage_name="dev",
        )

        manifest_checker = _make_checker_mock(
            [Finding(severity=Severity.ERROR, message="Manifest parse error")]
        )
        bundle_checker = _make_checker_mock(
            [Finding(severity=Severity.OK, message="Bundle OK")]
        )

        engine._checkers = [
            (Phase.MANIFEST_VALIDATION, manifest_checker),
            (Phase.BUNDLE_EXPLORATION, bundle_checker),
        ]

        report = engine.run()

        # Only manifest phase should have findings
        assert Phase.MANIFEST_VALIDATION in report.findings_by_phase
        assert Phase.BUNDLE_EXPLORATION not in report.findings_by_phase

        # Bundle checker should never have been called
        bundle_checker.check.assert_not_called()

        # Report should have the error
        assert report.error_count == 1
        assert report.has_blocking_errors(Phase.MANIFEST_VALIDATION)

    def test_manifest_warning_continues(self):
        """When ManifestChecker returns only WARNING, engine continues."""
        engine = DryRunEngine(
            manifest_file="manifest.yaml",
            stage_name="dev",
        )

        manifest_checker = _make_checker_mock(
            [Finding(severity=Severity.WARNING, message="Unresolved env var")]
        )
        bundle_checker = _make_checker_mock(
            [Finding(severity=Severity.OK, message="Bundle OK")]
        )

        engine._checkers = [
            (Phase.MANIFEST_VALIDATION, manifest_checker),
            (Phase.BUNDLE_EXPLORATION, bundle_checker),
        ]

        report = engine.run()

        assert Phase.MANIFEST_VALIDATION in report.findings_by_phase
        assert Phase.BUNDLE_EXPLORATION in report.findings_by_phase
        bundle_checker.check.assert_called_once()


class TestFullFlow:
    """Verify full flow with mixed findings."""

    def test_mixed_findings_all_collected(self):
        """Engine collects all findings from all phases when no manifest error."""
        engine = DryRunEngine(
            manifest_file="manifest.yaml",
            stage_name="dev",
        )

        checkers_and_findings = [
            (
                Phase.MANIFEST_VALIDATION,
                [
                    Finding(severity=Severity.OK, message="Manifest OK"),
                ],
            ),
            (
                Phase.BUNDLE_EXPLORATION,
                [
                    Finding(severity=Severity.OK, message="Bundle OK"),
                    Finding(severity=Severity.WARNING, message="Extra file in bundle"),
                ],
            ),
            (
                Phase.PERMISSION_VERIFICATION,
                [
                    Finding(severity=Severity.ERROR, message="Missing s3:PutObject"),
                ],
            ),
            (
                Phase.CONNECTIVITY,
                [
                    Finding(severity=Severity.OK, message="Domain reachable"),
                ],
            ),
            (
                Phase.PROJECT_INIT,
                [
                    Finding(severity=Severity.OK, message="Project exists"),
                ],
            ),
            (
                Phase.QUICKSIGHT,
                [
                    Finding(severity=Severity.OK, message="No QuickSight configured"),
                ],
            ),
            (
                Phase.STORAGE_DEPLOYMENT,
                [
                    Finding(severity=Severity.OK, message="Storage OK"),
                ],
            ),
            (
                Phase.GIT_DEPLOYMENT,
                [
                    Finding(severity=Severity.OK, message="Git OK"),
                ],
            ),
            (
                Phase.CATALOG_IMPORT,
                [
                    Finding(
                        severity=Severity.WARNING, message="Missing optional field"
                    ),
                ],
            ),
            (
                Phase.DEPENDENCY_VALIDATION,
                [
                    Finding(severity=Severity.ERROR, message="Missing Glue table"),
                ],
            ),
            (
                Phase.WORKFLOW_VALIDATION,
                [
                    Finding(severity=Severity.OK, message="Workflows OK"),
                ],
            ),
            (
                Phase.BOOTSTRAP_ACTIONS,
                [
                    Finding(severity=Severity.OK, message="Bootstrap OK"),
                ],
            ),
        ]

        engine._checkers = [
            (phase, _make_checker_mock(findings))
            for phase, findings in checkers_and_findings
        ]

        report = engine.run()

        # All 12 phases should be present
        assert len(report.findings_by_phase) == 12

        # Verify counts
        assert report.ok_count == 9
        assert report.warning_count == 2
        assert report.error_count == 2

    def test_errors_in_later_phases_do_not_stop_execution(self):
        """Errors in phases after manifest do not cause early termination."""
        engine = DryRunEngine(
            manifest_file="manifest.yaml",
            stage_name="dev",
        )

        engine._checkers = [
            (
                Phase.MANIFEST_VALIDATION,
                _make_checker_mock([Finding(severity=Severity.OK, message="OK")]),
            ),
            (
                Phase.BUNDLE_EXPLORATION,
                _make_checker_mock(
                    [Finding(severity=Severity.ERROR, message="Bad bundle")]
                ),
            ),
            (
                Phase.PERMISSION_VERIFICATION,
                _make_checker_mock([Finding(severity=Severity.OK, message="Perms OK")]),
            ),
        ]

        report = engine.run()

        # All three phases should have run
        assert len(report.findings_by_phase) == 3
        assert report.error_count == 1


class TestReportCounts:
    """Verify report counts are correct after full flow."""

    def test_empty_findings(self):
        """Engine handles checkers that return empty findings lists."""
        engine = DryRunEngine(
            manifest_file="manifest.yaml",
            stage_name="dev",
        )

        engine._checkers = [
            (Phase.MANIFEST_VALIDATION, _make_checker_mock([])),
            (Phase.BUNDLE_EXPLORATION, _make_checker_mock([])),
        ]

        report = engine.run()

        # Manifest phase returned no findings (no errors), so engine continues
        assert report.ok_count == 0
        assert report.warning_count == 0
        assert report.error_count == 0

    def test_context_is_shared_across_checkers(self):
        """All checkers receive the same DryRunContext instance."""
        engine = DryRunEngine(
            manifest_file="manifest.yaml",
            stage_name="dev",
            bundle_path="/tmp/bundle.zip",
        )

        contexts_seen: list[DryRunContext] = []

        def _capture_context_checker():
            mock = MagicMock()

            def capture(context):
                contexts_seen.append(context)
                return [Finding(severity=Severity.OK, message="OK")]

            mock.check.side_effect = capture
            return mock

        engine._checkers = [
            (Phase.MANIFEST_VALIDATION, _capture_context_checker()),
            (Phase.BUNDLE_EXPLORATION, _capture_context_checker()),
        ]

        engine.run()

        assert len(contexts_seen) == 2
        assert contexts_seen[0] is contexts_seen[1]
        assert contexts_seen[0].manifest_file == "manifest.yaml"
        assert contexts_seen[0].stage_name == "dev"
        assert contexts_seen[0].bundle_path == "/tmp/bundle.zip"
