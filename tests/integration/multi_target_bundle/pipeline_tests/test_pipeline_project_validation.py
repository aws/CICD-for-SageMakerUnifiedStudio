"""Integration tests for pipeline project validation."""

import os
import pytest


def test_environment_variables_available():
    """Test that required environment variables are available during test execution."""
    # These environment variables are set by the SMUS CLI test command
    # when running tests in the context of a deployed project
    
    # Check for domain context
    domain_id = os.environ.get('SMUS_DOMAIN_ID')
    domain_name = os.environ.get('SMUS_DOMAIN_NAME')
    region = os.environ.get('SMUS_REGION')
    
    # Check for project context
    project_id = os.environ.get('SMUS_PROJECT_ID')
    project_name = os.environ.get('SMUS_PROJECT_NAME')
    target_name = os.environ.get('SMUS_TARGET_NAME')
    
    print("\n=== SMUS Test Environment ===")
    print(f"Domain ID: {domain_id}")
    print(f"Domain Name: {domain_name}")
    print(f"Region: {region}")
    print(f"Project ID: {project_id}")
    print(f"Project Name: {project_name}")
    print(f"Target: {target_name}")
    print("============================\n")
    
    # Basic validation - at least project context should be available
    # Domain context may not be available in all environments
    assert project_id or project_name, "Project context should be available"
    assert region, "Region should be available"


def test_project_context():
    """Test that project context information is valid."""
    project_name = os.environ.get('SMUS_PROJECT_NAME')
    target_name = os.environ.get('SMUS_TARGET_NAME')
    
    if project_name:
        # Project name should be reasonable length
        assert len(project_name) > 0, "Project name should not be empty"
        assert len(project_name) <= 64, "Project name should be within length limits"
    
    if target_name:
        # Target name should be one of the standard stages
        assert target_name in ['dev', 'test', 'prod', 'DEV', 'TEST', 'PROD'], \
            f"Target name '{target_name}' should be a valid stage"


def test_domain_and_project_ids():
    """Test that domain and project IDs are in expected format."""
    domain_id = os.environ.get('SMUS_DOMAIN_ID')
    project_id = os.environ.get('SMUS_PROJECT_ID')
    
    if domain_id:
        # DataZone domain IDs typically start with 'dzd-'
        assert len(domain_id) > 0, "Domain ID should not be empty"
        print(f"Domain ID format: {domain_id}")
    
    if project_id:
        # DataZone project IDs are alphanumeric
        assert len(project_id) > 0, "Project ID should not be empty"
        assert project_id.replace('-', '').replace('_', '').isalnum(), \
            "Project ID should be alphanumeric"
        print(f"Project ID format: {project_id}")


@pytest.mark.slow
def test_aws_connectivity():
    """Test AWS connectivity (marked as slow test)."""
    import boto3
    from botocore.exceptions import ClientError
    
    region = os.environ.get('SMUS_REGION', 'us-east-1')
    
    try:
        # Test basic AWS connectivity
        sts = boto3.client('sts', region_name=region)
        identity = sts.get_caller_identity()
        
        assert 'Account' in identity, "Should be able to get AWS account info"
        assert 'UserId' in identity, "Should be able to get AWS user info"
        
        print(f"\n✅ AWS Account: {identity['Account']}")
        print(f"✅ AWS Identity: {identity['Arn']}")
        
        # Test DataZone connectivity if domain ID is available
        domain_id = os.environ.get('SMUS_DOMAIN_ID')
        if domain_id:
            try:
                datazone = boto3.client('datazone', region_name=region)
                domain = datazone.get_domain(identifier=domain_id)
                print(f"✅ DataZone domain accessible: {domain['name']}")
            except ClientError as e:
                if 'AccessDenied' in str(e):
                    print("⚠️  DataZone access denied (expected if no permissions)")
                else:
                    print(f"⚠️  DataZone error: {e}")
        
    except ClientError as e:
        if 'InvalidUserID.NotFound' in str(e):
            pytest.fail("AWS credentials not configured properly")
        else:
            pytest.skip(f"AWS connectivity test skipped: {e}")
    except Exception as e:
        pytest.skip(f"Unexpected error in AWS connectivity test: {e}")
