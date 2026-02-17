"""Integration tests for test command."""

import os
import pytest
import subprocess
import sys
from pathlib import Path
from tests.integration.base import IntegrationTestBase


class TestTestCommandIntegration(IntegrationTestBase):
    """Integration tests for test command."""

    @pytest.fixture
    def manifest_path(self):
        """Get path to test manifest."""
        return "tests/integration/glue-mwaa-catalog-app/manifest.yaml"

    def test_test_command_basic(self, manifest_path):
        """Test basic test command functionality.

        Note: This test handles two scenarios:
        1. Project exists (deployed by other tests) - shows test folder
        2. Project doesn't exist (deployment tests skipped) - shows graceful skip message
        Both are valid behaviors.
        """
        # Run test command
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "smus_cicd.cli",
                "test",
                "--manifest",
                manifest_path,
                "--targets",
                "test",
            ],
            capture_output=True,
            text=True,
            cwd=os.getcwd(),
        )

        # Should show test configuration
        assert "Target: test" in result.stdout

        # Should either show test folder or graceful skip message if project doesn't exist
        # Both are valid: project may not exist if deployment tests are skipped
        assert ("Test folder:" in result.stdout
                or "not found - skipping tests" in result.stdout), \
            "Should show test folder or graceful skip message"

        # Should show pipeline name
        assert "Pipeline: GlueMwaaCatalogApp" in result.stdout

    def test_test_command_json_output(self, manifest_path):
        """Test test command with JSON output."""
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "smus_cicd.cli",
                "test",
                "--manifest",
                manifest_path,
                "--targets",
                "test",
                "--output",
                "JSON",
            ],
            capture_output=True,
            text=True,
            cwd=os.getcwd(),
        )

        # Should produce valid JSON
        assert '"bundle": "GlueMwaaCatalogApp"' in result.stdout
        assert '"domain":' in result.stdout  # Check domain field exists (value varies by environment)

    def test_test_command_verbose(self, manifest_path):
        """Test test command with verbose output."""
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "smus_cicd.cli",
                "test",
                "--manifest",
                manifest_path,
                "--targets",
                "test",
                "--verbose",
            ],
            capture_output=True,
            text=True,
            cwd=os.getcwd(),
        )

        # Should show verbose information
        assert "Target: test" in result.stdout

    def test_test_command_all_targets(self, manifest_path):
        """Test test command with all targets."""
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "smus_cicd.cli",
                "test",
                "--manifest",
                manifest_path,
            ],
            capture_output=True,
            text=True,
            cwd=os.getcwd(),
        )

        # Should process all targets
        assert "Pipeline: GlueMwaaCatalogApp" in result.stdout
        # All targets should be processed (dev, test, prod)
        assert "Target: dev" in result.stdout
        assert "Target: test" in result.stdout
        assert "Target: prod" in result.stdout
        # Should show test execution attempts
        assert "Running tests..." in result.stdout

    def test_test_files_exist(self):
        """Test that test files exist in the expected location."""
        test_folder = Path("tests/integration/glue-mwaa-catalog-app/app_tests")
        assert test_folder.exists(), "Test folder should exist"

        test_file = test_folder / "test_project_validation.py"
        assert test_file.exists(), "Test file should exist"

        # Check test file content
        content = test_file.read_text()
        assert "test_environment_variables_available" in content
        assert "SMUS_DOMAIN_ID" in content
        assert "SMUS_PROJECT_ID" in content

    @pytest.mark.slow
    def test_actual_test_execution(self, manifest_path):
        """Test actual test execution with mocked environment."""
        # Set up mock environment variables
        env = os.environ.copy()
        env.update(
            {
                "SMUS_DOMAIN_ID": "test-domain-id",
                "SMUS_PROJECT_ID": "test-project-id",
                "SMUS_PROJECT_NAME": "integration-test-test",
                "SMUS_TARGET_NAME": "test",
                "SMUS_REGION": "us-east-1",
                "SMUS_DOMAIN_NAME": "cicd-test-domain",
            }
        )

        # Run pytest directly on test folder
        test_folder = "tests/integration/glue-mwaa-catalog-app/app_tests"
        result = subprocess.run(
            [sys.executable, "-m", "pytest", test_folder, "-v"],
            capture_output=True,
            text=True,
            env=env,
            cwd=os.getcwd(),
        )

        # Should run tests successfully (some may be skipped due to AWS connectivity)
        assert "test_environment_variables_available PASSED" in result.stdout
        assert "test_project_context PASSED" in result.stdout
        assert "test_domain_and_project_ids PASSED" in result.stdout
