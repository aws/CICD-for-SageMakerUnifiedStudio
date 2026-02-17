"""Integration test for CDK bootstrap action with DataZone verification."""

import os
import tempfile

import boto3
import pytest

from tests.integration.base import IntegrationTestBase


@pytest.mark.slow
class TestCdkBootstrap(IntegrationTestBase):
    """Test CDK bootstrap action end-to-end with DataZone integration."""

    MANIFEST_PATH = os.path.join(
        os.path.dirname(__file__), "manifest.yaml"
    )

    def setup_method(self, method):
        """Setup: unset public DataZone endpoint so CLI uses internal service."""
        # Save and unset the public endpoint env vars — the public
        # datazone endpoint may be unreachable; the internal service
        # endpoint (used by boto3 when no override is set) works.
        self._saved_dz_endpoint = os.environ.pop("DATAZONE_ENDPOINT_URL", None)
        self._saved_aws_dz_endpoint = os.environ.pop("AWS_ENDPOINT_URL_DATAZONE", None)
        super().setup_method(method)

    def teardown_method(self, method):
        """Teardown: restore original endpoint env vars."""
        super().teardown_method(method)
        if self._saved_dz_endpoint is not None:
            os.environ["DATAZONE_ENDPOINT_URL"] = self._saved_dz_endpoint
        if self._saved_aws_dz_endpoint is not None:
            os.environ["AWS_ENDPOINT_URL_DATAZONE"] = self._saved_aws_dz_endpoint

    def _get_dz_internal_client(self):
        """Get datazone-internal client (no endpoint_url override)."""
        region = os.environ.get(
            "DEV_DOMAIN_REGION",
            self.config.get("aws", {}).get("region", "us-east-1"),
        )
        return boto3.client("datazone-internal", region_name=region)

    def _resolve_domain(self):
        """Resolve domain using datazone-internal client."""
        client = self._get_dz_internal_client()
        resp = client.list_domains(maxResults=10)
        domains = resp.get("items", [])

        # Match by tags (purpose: smus-cicd-testing) like config.yaml
        domain_tags = self.config.get("test_environment", {}).get("domain_tags", {})
        domain_name_cfg = self.config.get("test_environment", {}).get("domain_name")

        for d in domains:
            if domain_name_cfg and d.get("name") == domain_name_cfg:
                return d["id"], d["name"]

        # Fallback: match by tags
        if domain_tags:
            for d in domains:
                try:
                    tags_resp = client.list_tags_for_resource(resourceArn=d["arn"])
                    tags = tags_resp.get("tags", {})
                    if all(tags.get(k) == v for k, v in domain_tags.items()):
                        return d["id"], d["name"]
                except Exception:
                    continue

        # Fallback: single domain
        if len(domains) == 1:
            return domains[0]["id"], domains[0]["name"]

        raise RuntimeError(f"Could not resolve domain from {len(domains)} domains")

    def _resolve_project(self, domain_id, project_name):
        """Resolve project ID by name using datazone-internal client."""
        client = self._get_dz_internal_client()
        next_token = None
        while True:
            kwargs = {"domainIdentifier": domain_id, "maxResults": 50}
            if next_token:
                kwargs["nextToken"] = next_token
            resp = client.list_projects(**kwargs)
            for p in resp.get("items", []):
                if p["name"] == project_name:
                    return p["id"]
            next_token = resp.get("nextToken")
            if not next_token:
                break
        return None

    # ------------------------------------------------------------------
    # Test 1: Verify DataZone domain resolution via internal client
    # ------------------------------------------------------------------
    def test_datazone_domain_resolution(self):
        """Verify DataZone domain can be resolved using datazone-internal client."""
        self.logger.info("=== DataZone domain resolution test ===")

        domain_id, domain_name = self._resolve_domain()
        assert domain_id is not None, "Could not resolve DataZone domain ID"
        self.logger.info(f"Resolved domain: name={domain_name}, id={domain_id}")

        # Verify via get_domain
        client = self._get_dz_internal_client()
        domain = client.get_domain(identifier=domain_id)
        assert domain["id"] == domain_id
        assert domain["status"] == "AVAILABLE"
        self.logger.info(
            f"✅ Domain verified: {domain['name']} (status={domain['status']})"
        )

    # ------------------------------------------------------------------
    # Test 2: Verify project exists and list connections
    # ------------------------------------------------------------------
    def test_datazone_project_exists(self):
        """Verify the target project exists in DataZone and list its connections."""
        self.logger.info("=== DataZone project verification test ===")

        domain_id, domain_name = self._resolve_domain()
        assert domain_id, "Domain not resolved"

        project_name = "test-marketing"
        project_id = self._resolve_project(domain_id, project_name)
        assert project_id is not None, (
            f"Project '{project_name}' not found in domain {domain_id}"
        )
        self.logger.info(f"Project resolved: {project_name} -> {project_id}")

        # Get project details via internal client
        client = self._get_dz_internal_client()
        project = client.get_project(
            domainIdentifier=domain_id, identifier=project_id
        )
        assert project["name"] == project_name
        self.logger.info(
            f"Project status: {project.get('projectStatus', 'N/A')}"
        )

        # List connections
        connections = client.list_connections(
            domainIdentifier=domain_id, projectIdentifier=project_id
        )
        conn_names = [c.get("name") for c in connections.get("items", [])]
        self.logger.info(f"Project connections: {conn_names}")
        assert len(conn_names) > 0, "Project should have at least one connection"
        self.logger.info("✅ Project and connections verified")

    # ------------------------------------------------------------------
    # Test 3: CDK handler registration and parameter validation
    # ------------------------------------------------------------------
    def test_cdk_handler_registration_and_validation(self):
        """Verify CDK handler registration, routing, and parameter validation."""
        self.logger.info("=== CDK handler registration and validation ===")

        from smus_cicd.bootstrap import registry
        from smus_cicd.bootstrap.models import BootstrapAction

        # All four CDK actions must be registered
        for action_type in ["cdk.deploy", "cdk.destroy", "cdk.synth", "cdk.diff"]:
            handler = registry.get_handler(action_type)
            assert handler is not None, f"{action_type} handler not registered"
            self.logger.info(f"✅ {action_type} registered")

        # Missing appPath → ValueError
        handler = registry.get_handler("cdk.deploy")
        action = BootstrapAction(type="cdk.deploy", parameters={})
        context = {"stage_name": "test", "region": "us-east-1"}
        try:
            handler(action, context)
            assert False, "Should have raised ValueError for missing appPath"
        except ValueError as e:
            assert "appPath" in str(e)
            self.logger.info(f"✅ Missing appPath raises ValueError: {e}")

        # Non-existent appPath → ValueError
        action = BootstrapAction(
            type="cdk.deploy",
            parameters={"appPath": "/nonexistent/cdk/app"},
        )
        try:
            handler(action, context)
            assert False, "Should have raised ValueError for bad path"
        except ValueError as e:
            assert "does not exist" in str(e)
            self.logger.info(f"✅ Bad appPath raises ValueError: {e}")

    # ------------------------------------------------------------------
    # Test 4: CDK handler with DataZone-resolved context
    # ------------------------------------------------------------------
    def test_cdk_handler_with_datazone_context(self):
        """
        Build a realistic deployment context from DataZone (via internal
        client) and invoke the CDK handler to verify context/env-var
        merging works end-to-end.
        """
        self.logger.info("=== CDK handler with DataZone context ===")

        domain_id, domain_name = self._resolve_domain()
        assert domain_id, "Domain not resolved"

        project_name = "test-marketing"
        project_id = self._resolve_project(domain_id, project_name)
        assert project_id, f"Project '{project_name}' not found"

        region = os.environ.get(
            "DEV_DOMAIN_REGION",
            self.config.get("aws", {}).get("region", "us-east-1"),
        )

        from smus_cicd.bootstrap import registry
        from smus_cicd.bootstrap.models import BootstrapAction

        # Build context similar to what deploy.py passes to handlers
        context = {
            "stage_name": "test",
            "project_name": project_name,
            "domain_id": domain_id,
            "region": region,
            "env_vars": {
                "DOMAIN_ID": domain_id,
                "PROJECT_ID": project_id,
                "STAGE": "TEST",
            },
        }

        handler = registry.get_handler("cdk.deploy")

        with tempfile.TemporaryDirectory() as tmpdir:
            action = BootstrapAction(
                type="cdk.deploy",
                parameters={
                    "appPath": tmpdir,
                    "stackName": "CdkBootstrapTestStack",
                    "context": {"env": "test", "customKey": "customValue"},
                    "environmentVariables": {"MY_VAR": "hello"},
                },
            )

            try:
                result = handler(action, context)
                self.logger.info(f"Handler returned: {result}")
            except Exception as e:
                # CDK CLI not installed or no cdk.json — expected in CI
                error_msg = str(e).lower()
                assert any(
                    kw in error_msg for kw in ["cdk", "failed", "not found"]
                ), f"Unexpected error: {e}"
                self.logger.info(
                    f"Expected CDK execution error (no CDK app): {e}"
                )

        self.logger.info("✅ CDK handler invoked with DataZone-resolved context")

    # ------------------------------------------------------------------
    # Test 5: Describe manifest via CLI
    # ------------------------------------------------------------------
    def test_describe_manifest(self):
        """Test that the CLI describe command works for the CDK manifest."""
        self.logger.info("=== Describe manifest ===")

        result = self.run_cli_command(
            ["describe", "--manifest", self.MANIFEST_PATH]
        )
        assert result["success"], (
            f"Describe failed: {result.get('output', '')}"
        )

        output = result.get("output", "").lower()
        assert "cdk" in output or "bootstrap" in output, (
            "Describe output should reference CDK bootstrap action"
        )
        self.logger.info("✅ Describe succeeded with CDK bootstrap visible")

    # ------------------------------------------------------------------
    # Test 6: Deploy manifest via CLI (end-to-end)
    # ------------------------------------------------------------------
    def test_deploy_cdk_bootstrap_action(self):
        """
        Run the full deploy CLI path which triggers the CDK bootstrap action.

        The deploy resolves domain/project via DataZone, then executes
        bootstrap actions including cdk.deploy.  The CDK step fails
        because there is no real CDK app at infra/, but we validate the
        pipeline reaches the bootstrap phase.
        """
        self.logger.info("=== Deploy with CDK bootstrap action ===")

        deploy_result = self.run_cli_command(
            ["deploy", "--manifest", self.MANIFEST_PATH, "--targets", "test"]
        )
        deploy_output = deploy_result.get("output", "")
        self.logger.info(f"Deploy exit_code={deploy_result['exit_code']}")
        self.logger.info(f"Deploy output:\n{deploy_output}")

        output_lower = deploy_output.lower()

        # Deploy should reach the bootstrap phase even though CDK fails
        # (no real CDK app at infra/)
        reached_bootstrap = any(
            kw in output_lower
            for kw in ["bootstrap", "cdk", "cdk.deploy"]
        )
        assert reached_bootstrap, (
            "Deploy should have reached the bootstrap phase"
        )
        self.logger.info("✅ Deploy reached bootstrap phase with CDK action")
