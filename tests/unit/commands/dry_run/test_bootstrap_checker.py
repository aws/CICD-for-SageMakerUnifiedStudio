"""Unit tests for BootstrapChecker (Phase 11 — Bootstrap Action Simulation).

Tests cover:
- No target config → WARNING skip
- No bootstrap section → OK "no bootstrap actions configured"
- Empty actions list → OK "no bootstrap actions configured"
- Single known action type → OK with type and parameters
- Multiple actions → one OK finding per action
- Unknown action type → ERROR "no registered handler found"
- Action with no parameters → OK message without parameter summary
- Action with multiple parameters → OK message with sorted param keys
- Finding metadata (resource, service, details)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pytest

from smus_cicd.commands.dry_run.checkers.bootstrap_checker import BootstrapChecker
from smus_cicd.commands.dry_run.models import DryRunContext, Severity

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass
class _FakeAction:
    type: str
    parameters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class _FakeBootstrap:
    actions: List[_FakeAction] = field(default_factory=list)


@dataclass
class _FakeTarget:
    bootstrap: Optional[_FakeBootstrap] = None


def _make_context(
    target: Optional[_FakeTarget] = None,
) -> DryRunContext:
    return DryRunContext(
        manifest_file="manifest.yaml",
        target_config=target,
    )


@pytest.fixture
def checker() -> BootstrapChecker:
    return BootstrapChecker()


# ---------------------------------------------------------------------------
# No target config
# ---------------------------------------------------------------------------


# TestNoTargetConfig tests moved to test_preflight_checker.py

# ---------------------------------------------------------------------------
# No bootstrap actions configured
# ---------------------------------------------------------------------------


class TestNoBootstrapActions:
    def test_no_bootstrap_section(self, checker):
        ctx = _make_context(target=_FakeTarget(bootstrap=None))
        findings = checker.check(ctx)

        assert len(findings) == 1
        assert findings[0].severity == Severity.OK
        assert "No bootstrap actions configured" in findings[0].message

    def test_empty_actions_list(self, checker):
        ctx = _make_context(target=_FakeTarget(bootstrap=_FakeBootstrap(actions=[])))
        findings = checker.check(ctx)

        assert len(findings) == 1
        assert findings[0].severity == Severity.OK
        assert "No bootstrap actions configured" in findings[0].message


# ---------------------------------------------------------------------------
# Known action types (registered handlers)
# ---------------------------------------------------------------------------


class TestKnownActionTypes:
    def test_workflow_run_action(self, checker):
        action = _FakeAction(
            type="workflow.run",
            parameters={"workflowName": "etl_pipeline", "wait": True},
        )
        ctx = _make_context(
            target=_FakeTarget(bootstrap=_FakeBootstrap(actions=[action]))
        )
        findings = checker.check(ctx)

        assert len(findings) == 1
        assert findings[0].severity == Severity.OK
        assert "workflow.run" in findings[0].message
        assert "workflowName" in findings[0].message

    def test_workflow_create_action(self, checker):
        action = _FakeAction(
            type="workflow.create",
            parameters={"workflowName": "my_dag"},
        )
        ctx = _make_context(
            target=_FakeTarget(bootstrap=_FakeBootstrap(actions=[action]))
        )
        findings = checker.check(ctx)

        assert len(findings) == 1
        assert findings[0].severity == Severity.OK
        assert "workflow.create" in findings[0].message

    def test_quicksight_refresh_action(self, checker):
        action = _FakeAction(
            type="quicksight.refresh_dataset",
            parameters={"refreshScope": "IMPORTED"},
        )
        ctx = _make_context(
            target=_FakeTarget(bootstrap=_FakeBootstrap(actions=[action]))
        )
        findings = checker.check(ctx)

        assert len(findings) == 1
        assert findings[0].severity == Severity.OK
        assert "quicksight.refresh_dataset" in findings[0].message

    def test_project_create_environment(self, checker):
        action = _FakeAction(
            type="project.create_environment",
            parameters={"environmentName": "dev-env"},
        )
        ctx = _make_context(
            target=_FakeTarget(bootstrap=_FakeBootstrap(actions=[action]))
        )
        findings = checker.check(ctx)

        assert len(findings) == 1
        assert findings[0].severity == Severity.OK
        assert "project.create_environment" in findings[0].message

    def test_cli_print_action(self, checker):
        action = _FakeAction(
            type="cli.print",
            parameters={"message": "hello"},
        )
        ctx = _make_context(
            target=_FakeTarget(bootstrap=_FakeBootstrap(actions=[action]))
        )
        findings = checker.check(ctx)

        assert len(findings) == 1
        assert findings[0].severity == Severity.OK
        assert "cli.print" in findings[0].message


# ---------------------------------------------------------------------------
# Multiple actions
# ---------------------------------------------------------------------------


class TestMultipleActions:
    def test_multiple_actions_produce_one_finding_each(self, checker):
        actions = [
            _FakeAction(type="workflow.run", parameters={"workflowName": "dag1"}),
            _FakeAction(type="workflow.logs", parameters={"workflowName": "dag1"}),
            _FakeAction(type="quicksight.refresh_dataset", parameters={}),
        ]
        ctx = _make_context(
            target=_FakeTarget(bootstrap=_FakeBootstrap(actions=actions))
        )
        findings = checker.check(ctx)

        assert len(findings) == 3
        types_in_messages = [f.message for f in findings]
        assert any("workflow.run" in m for m in types_in_messages)
        assert any("workflow.logs" in m for m in types_in_messages)
        assert any("quicksight.refresh_dataset" in m for m in types_in_messages)


# ---------------------------------------------------------------------------
# Unknown action type
# ---------------------------------------------------------------------------


class TestUnknownActionType:
    def test_unknown_type_produces_warning(self, checker):
        action = _FakeAction(
            type="unknown.action",
            parameters={"key": "value"},
        )
        ctx = _make_context(
            target=_FakeTarget(bootstrap=_FakeBootstrap(actions=[action]))
        )
        findings = checker.check(ctx)

        assert len(findings) == 1
        assert findings[0].severity == Severity.ERROR
        assert "no registered handler found" in findings[0].message
        assert "unknown.action" in findings[0].message


# ---------------------------------------------------------------------------
# Action with no parameters
# ---------------------------------------------------------------------------


class TestActionNoParameters:
    def test_action_without_params(self, checker):
        action = _FakeAction(type="workflow.create", parameters={})
        ctx = _make_context(
            target=_FakeTarget(bootstrap=_FakeBootstrap(actions=[action]))
        )
        findings = checker.check(ctx)

        assert len(findings) == 1
        assert findings[0].severity == Severity.OK
        assert "workflow.create" in findings[0].message
        # No colon separator when there are no params
        assert findings[0].message == "Bootstrap action 'workflow.create'"


# ---------------------------------------------------------------------------
# Action with multiple parameters (sorted keys)
# ---------------------------------------------------------------------------


class TestActionMultipleParameters:
    def test_params_sorted_in_message(self, checker):
        action = _FakeAction(
            type="workflow.run",
            parameters={"wait": True, "trailLogs": False, "workflowName": "dag"},
        )
        ctx = _make_context(
            target=_FakeTarget(bootstrap=_FakeBootstrap(actions=[action]))
        )
        findings = checker.check(ctx)

        assert len(findings) == 1
        msg = findings[0].message
        # Keys should appear in sorted order: trailLogs, wait, workflowName
        trail_pos = msg.index("trailLogs")
        wait_pos = msg.index("wait")
        wf_pos = msg.index("workflowName")
        assert trail_pos < wait_pos < wf_pos


# ---------------------------------------------------------------------------
# Finding metadata
# ---------------------------------------------------------------------------


class TestFindingMetadata:
    def test_ok_finding_has_service(self, checker):
        action = _FakeAction(type="workflow.run", parameters={})
        ctx = _make_context(
            target=_FakeTarget(bootstrap=_FakeBootstrap(actions=[action]))
        )
        findings = checker.check(ctx)

        assert findings[0].service == "bootstrap"

    def test_ok_finding_has_resource(self, checker):
        action = _FakeAction(type="workflow.run", parameters={})
        ctx = _make_context(
            target=_FakeTarget(bootstrap=_FakeBootstrap(actions=[action]))
        )
        findings = checker.check(ctx)

        assert findings[0].resource == "workflow.run"

    def test_ok_finding_has_details(self, checker):
        action = _FakeAction(
            type="workflow.run",
            parameters={"workflowName": "dag1"},
        )
        ctx = _make_context(
            target=_FakeTarget(bootstrap=_FakeBootstrap(actions=[action]))
        )
        findings = checker.check(ctx)

        assert findings[0].details is not None
        assert findings[0].details["type"] == "workflow.run"
        assert findings[0].details["parameters"] == {"workflowName": "dag1"}

    def test_skip_finding_has_service(self, checker):
        ctx = _make_context(target=_FakeTarget(bootstrap=_FakeBootstrap(actions=[])))
        findings = checker.check(ctx)

        assert findings[0].service == "bootstrap"

    def test_warning_finding_has_details(self, checker):
        action = _FakeAction(
            type="unknown.action",
            parameters={"foo": "bar"},
        )
        ctx = _make_context(
            target=_FakeTarget(bootstrap=_FakeBootstrap(actions=[action]))
        )
        findings = checker.check(ctx)

        assert findings[0].details is not None
        assert findings[0].details["type"] == "unknown.action"
        assert findings[0].details["parameters"] == {"foo": "bar"}


# ---------------------------------------------------------------------------
# workflow.create custom tag validation
# ---------------------------------------------------------------------------


class TestWorkflowCreateTagValidation:
    def test_reserved_tag_collision_produces_error(self, checker):
        action = _FakeAction(
            type="workflow.create",
            parameters={"tags": {"CostCenter": "1234", "STAGE": "override"}},
        )
        ctx = _make_context(
            target=_FakeTarget(bootstrap=_FakeBootstrap(actions=[action]))
        )
        findings = checker.check(ctx)

        errors = [f for f in findings if f.severity == Severity.ERROR]
        assert len(errors) == 1
        assert "reserved" in errors[0].message
        assert "STAGE" in errors[0].message

    def test_non_dict_tags_produces_error(self, checker):
        action = _FakeAction(
            type="workflow.create",
            parameters={"tags": ["not", "a", "dict"]},
        )
        ctx = _make_context(
            target=_FakeTarget(bootstrap=_FakeBootstrap(actions=[action]))
        )
        findings = checker.check(ctx)

        errors = [f for f in findings if f.severity == Severity.ERROR]
        assert len(errors) == 1
        assert "must be a mapping" in errors[0].message

    def test_valid_custom_tags_produce_no_error(self, checker):
        action = _FakeAction(
            type="workflow.create",
            parameters={"tags": {"CostCenter": "1234", "Team": "analytics"}},
        )
        ctx = _make_context(
            target=_FakeTarget(bootstrap=_FakeBootstrap(actions=[action]))
        )
        findings = checker.check(ctx)

        # Only the normal OK "action would run" finding, no ERROR findings.
        assert not [f for f in findings if f.severity == Severity.ERROR]
        assert any(f.severity == Severity.OK for f in findings)

    def test_tag_validation_only_applies_to_workflow_create(self, checker):
        # A reserved-looking key on a different action type is not our concern.
        action = _FakeAction(
            type="workflow.run",
            parameters={"tags": {"STAGE": "whatever"}},
        )
        ctx = _make_context(
            target=_FakeTarget(bootstrap=_FakeBootstrap(actions=[action]))
        )
        findings = checker.check(ctx)

        assert not [f for f in findings if f.severity == Severity.ERROR]
