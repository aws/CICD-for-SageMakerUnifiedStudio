#!/usr/bin/env python3

import boto3
import json
import os
import pytest

def test_permissions():
    """Test basic DataZone permissions before attempting connection creation"""
    
    # Get configuration from environment variables
    region = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
    domain_id = os.environ.get('DATAZONE_DOMAIN_ID')
    project_id = os.environ.get('DATAZONE_PROJECT_ID_DEV')
    
    if not domain_id or not project_id:
        pytest.skip("DATAZONE_DOMAIN_ID and DATAZONE_PROJECT_ID_DEV environment variables required")
    
    client = boto3.client('datazone', region_name=region)
    
    # Get first environment from the project
    try:
        env_response = client.list_environments(
            domainIdentifier=domain_id,
            projectIdentifier=project_id,
            maxResults=1
        )
        environments = env_response.get('items', [])
        if not environments:
            pytest.skip(f"No environments found for project {project_id}")
        env_id = environments[0]['id']
    except Exception as e:
        pytest.skip(f"Could not list environments: {e}")
    
    print("Testing DataZone permissions...")
    print(f"Region: {region}")
    print(f"Domain: {domain_id}")
    print(f"Project: {project_id}")
    print(f"Environment: {env_id}")
    print("=" * 50)
    
    # Test 1: List connections (read permission)
    try:
        response = client.list_connections(
            domainIdentifier=domain_id,
            projectIdentifier=project_id
        )
        print(f"✅ List connections: SUCCESS - Found {len(response.get('items', []))} connections")
        
        if response.get('items'):
            print("   Existing connections:")
            for conn in response['items'][:3]:  # Show first 3
                print(f"   - {conn['name']} ({conn['type']})")
        
    except Exception as e:
        print(f"❌ List connections: FAILED - {str(e)}")
        pytest.fail(f"List connections failed: {str(e)}")
    
    # Test 2: Try simple S3 connection creation
    print(f"\nTesting S3 connection creation...")
    
    # First, clean up any existing test connections
    try:
        response = client.list_connections(
            domainIdentifier=domain_id,
            projectIdentifier=project_id
        )
        for conn in response.get('items', []):
            if conn['name'].startswith('test-s3-permission-check'):
                try:
                    client.delete_connection(
                        domainIdentifier=domain_id,
                        identifier=conn['connectionId']
                    )
                    print(f"🧹 Cleaned up old test connection: {conn['name']}")
                except Exception:
                    pass  # Ignore cleanup errors
    except Exception:
        pass  # Ignore cleanup errors
    
    connection_id = None
    try:
        response = client.create_connection(
            domainIdentifier=domain_id,
            environmentIdentifier=env_id,
            name="test-s3-permission-check",
            description="Permission test",
            props={
                "s3Properties": {
                    "s3Uri": "s3://test-bucket/data/"
                }
            }
        )
        
        connection_id = response['connectionId']
        print(f"✅ S3 connection creation: SUCCESS - {connection_id}")
        
    except Exception as e:
        error_msg = str(e)
        print(f"❌ S3 connection creation: FAILED - {error_msg}")
        
        if "AccessDeniedException" in error_msg:
            print("   → Permission issue: User may not be project owner or lack CreateConnection permission")
        elif "ValidationException" in error_msg:
            print("   → Schema issue: Parameters may be incorrect")
        
        pytest.fail(f"S3 connection creation failed: {error_msg}")
    
    finally:
        # Clean up the connection we just created
        if connection_id:
            try:
                client.delete_connection(
                    domainIdentifier=domain_id,
                    identifier=connection_id
                )
                print(f"   Cleaned up successfully")
            except Exception as e:
                print(f"   ⚠️ Cleanup warning: {e}")

if __name__ == "__main__":
    success = test_permissions()
    if success:
        print(f"\n🎉 Permissions verified - ready to test all connection types")
    else:
        print(f"\n💥 Permission issues detected - need to resolve before proceeding")
