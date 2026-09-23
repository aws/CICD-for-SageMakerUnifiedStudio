"""Unit tests for Tooling environment resolution in datazone helpers.

Focus: blueprint discovery must fall back to the IAM connection path when it
fails (e.g. a deploy role lacks datazone:ListEnvironmentBlueprints), rather than
aborting the whole resolution.
"""

from unittest.mock import MagicMock, patch

from smus_cicd.helpers.datazone import (
    _resolve_tooling_environment_id,
    get_default_tooling_environment,
)

DOMAIN_ID = "dzd-6p08e3x5jd44sr"
PROJECT_ID = "b4ybveso46wxaj"
ENV_ID = "env-abc123"


def _access_denied():
    return Exception(
        "An error occurred (AccessDeniedException) when calling the "
        "ListEnvironmentBlueprints operation: not authorized to perform "
        "datazone:ListEnvironmentBlueprints"
    )


class TestResolveToolingEnvironmentId:
    """Direct tests for _resolve_tooling_environment_id."""

    def test_resolves_via_blueprint(self):
        """Happy path: a managed Tooling blueprint yields the default environment."""
        client = MagicMock()
        client.list_environment_blueprints.return_value = {
            "items": [{"id": "bp-1", "name": "Tooling.GA", "provider": "Amazon"}]
        }
        client.list_environments.return_value = {
            "items": [{"id": ENV_ID, "status": "ACTIVE"}]
        }
        logger = MagicMock()

        result = _resolve_tooling_environment_id(client, DOMAIN_ID, PROJECT_ID, logger)

        assert result == ENV_ID

    def test_returns_none_when_no_blueprints(self):
        """No managed Tooling blueprint -> None (so caller tries IAM fallback)."""
        client = MagicMock()
        client.list_environment_blueprints.return_value = {"items": []}
        logger = MagicMock()

        result = _resolve_tooling_environment_id(client, DOMAIN_ID, PROJECT_ID, logger)

        assert result is None

    def test_returns_none_on_access_denied_instead_of_raising(self):
        """AccessDenied on ListEnvironmentBlueprints must be swallowed -> None."""
        client = MagicMock()
        client.list_environment_blueprints.side_effect = _access_denied()
        logger = MagicMock()

        # Must not raise.
        result = _resolve_tooling_environment_id(client, DOMAIN_ID, PROJECT_ID, logger)

        assert result is None

    def test_returns_none_on_list_environments_error(self):
        """An error while listing environments is also swallowed -> None."""
        client = MagicMock()
        client.list_environment_blueprints.return_value = {
            "items": [{"id": "bp-1", "name": "Tooling.GA"}]
        }
        client.list_environments.side_effect = Exception("throttled")
        logger = MagicMock()

        result = _resolve_tooling_environment_id(client, DOMAIN_ID, PROJECT_ID, logger)

        assert result is None


class TestGetDefaultToolingEnvironmentFallback:
    """End-to-end control flow: blueprint failure -> IAM connection fallback."""

    def test_falls_back_to_iam_connection_when_blueprints_denied(self):
        """When blueprint discovery is denied, the IAM connection path resolves the env."""
        client = MagicMock()
        client.list_environment_blueprints.side_effect = _access_denied()
        # IAM connection fallback returns a connection pointing at the tooling env.
        client.list_connections.return_value = {
            "items": [{"props": {"iamProperties": {"environmentId": ENV_ID}}}]
        }
        client.get_environment.return_value = {"id": ENV_ID, "provisionedResources": []}

        with patch(
            "smus_cicd.helpers.datazone.get_project_id_by_name",
            return_value=PROJECT_ID,
        ), patch(
            "smus_cicd.helpers.datazone._get_datazone_client",
            return_value=client,
        ):
            result = get_default_tooling_environment(
                "Compass-Dev", DOMAIN_ID, "eu-central-1"
            )

        assert result == {"id": ENV_ID, "provisionedResources": []}
        client.list_connections.assert_called_once()
        client.get_environment.assert_called_once_with(
            domainIdentifier=DOMAIN_ID, identifier=ENV_ID
        )

    def test_returns_none_when_both_paths_fail(self):
        """Blueprint denied and no IAM connection -> None (caller uses defaults)."""
        client = MagicMock()
        client.list_environment_blueprints.side_effect = _access_denied()
        client.list_connections.return_value = {"items": []}

        with patch(
            "smus_cicd.helpers.datazone.get_project_id_by_name",
            return_value=PROJECT_ID,
        ), patch(
            "smus_cicd.helpers.datazone._get_datazone_client",
            return_value=client,
        ):
            result = get_default_tooling_environment(
                "Compass-Dev", DOMAIN_ID, "eu-central-1"
            )

        assert result is None
        client.list_connections.assert_called_once()
        client.get_environment.assert_not_called()
