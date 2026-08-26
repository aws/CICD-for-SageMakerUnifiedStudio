"""Bootstrap action simulation checker (Phase 11).

Lists each bootstrap action that would execute, including type and
parameters.  References bootstrap action types from
``docs/bootstrap-actions.md`` and uses the action registry from
``bootstrap/action_registry.py`` to verify that each action type has a
registered handler.

- If no bootstrap actions are configured, returns OK with
  "no bootstrap actions configured".
- For each action, reports OK with the action type and key parameters.
- If an action type has no registered handler, reports WARNING.

Requirements: 5.6
"""

from __future__ import annotations

import logging
from typing import List

from smus_cicd.bootstrap.action_registry import registry
from smus_cicd.bootstrap.handlers.workflow_create_handler import (
    RESERVED_WORKFLOW_TAG_KEYS,
)
from smus_cicd.commands.dry_run.models import DryRunContext, Finding, Severity

logger = logging.getLogger(__name__)


class BootstrapChecker:
    """Lists bootstrap actions that would execute during deployment."""

    def check(self, context: DryRunContext) -> List[Finding]:
        findings: List[Finding] = []

        bootstrap = getattr(context.target_config, "bootstrap", None)
        actions = getattr(bootstrap, "actions", None) or [] if bootstrap else []

        if not actions:
            findings.append(
                Finding(
                    severity=Severity.OK,
                    message="No bootstrap actions configured.",
                    service="bootstrap",
                )
            )
            return findings

        for action in actions:
            action_type = getattr(action, "type", str(action))
            parameters = getattr(action, "parameters", {}) or {}

            # Validate custom workflow tags up front so a bad manifest fails the
            # dry-run (and pre-deployment validation) rather than at deploy time.
            findings.extend(self._check_workflow_create_tags(action_type, parameters))

            # Check whether the action type has a registered handler
            has_handler = self._has_handler(action_type)

            param_keys = sorted(parameters.keys()) if parameters else []
            param_summary = ", ".join(f"{k}={parameters[k]!r}" for k in param_keys)

            if has_handler:
                message = f"Bootstrap action '{action_type}'"
                if param_summary:
                    message += f": {param_summary}"
                findings.append(
                    Finding(
                        severity=Severity.OK,
                        message=message,
                        resource=action_type,
                        service="bootstrap",
                        details={
                            "type": action_type,
                            "parameters": parameters,
                        },
                    )
                )
            else:
                findings.append(
                    Finding(
                        severity=Severity.ERROR,
                        message=(
                            f"Bootstrap action '{action_type}': "
                            f"no registered handler found"
                        ),
                        resource=action_type,
                        service="bootstrap",
                        details={
                            "type": action_type,
                            "parameters": parameters,
                        },
                    )
                )

        return findings

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _check_workflow_create_tags(
        action_type: str, parameters: dict
    ) -> List[Finding]:
        """Validate custom tags on a workflow.create action.

        Mirrors the deploy-time enforcement in handle_workflow_create: custom
        tag keys must not collide with the reserved SMUS-managed tags, and the
        tags value must be a mapping. Returns ERROR findings for violations, or
        an empty list when the action is not workflow.create or its tags are OK.
        """
        if action_type != "workflow.create":
            return []

        tags = parameters.get("tags")
        if not tags:
            return []

        if not isinstance(tags, dict):
            return [
                Finding(
                    severity=Severity.ERROR,
                    message=(
                        "workflow.create 'tags' must be a mapping of string keys "
                        "to string values"
                    ),
                    resource=action_type,
                    service="bootstrap",
                    details={"type": action_type, "tags": tags},
                )
            ]

        reserved_collisions = sorted(set(tags) & RESERVED_WORKFLOW_TAG_KEYS)
        if reserved_collisions:
            return [
                Finding(
                    severity=Severity.ERROR,
                    message=(
                        f"Custom workflow tag key(s) {reserved_collisions} are "
                        "reserved by SMUS CI/CD and cannot be overridden. Rename "
                        "or remove them in the workflow.create action's 'tags'."
                    ),
                    resource=action_type,
                    service="bootstrap",
                    details={
                        "type": action_type,
                        "reserved_collisions": reserved_collisions,
                    },
                )
            ]

        return []

    @staticmethod
    def _has_handler(action_type: str) -> bool:
        """Return True if the action registry has a handler for *action_type*."""
        try:
            registry.get_handler(action_type)
            return True
        except (ValueError, KeyError):
            return False
