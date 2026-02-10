#!/usr/bin/env python3

import boto3
import time
import os
import pytest

def test_s3_connection():
    """Test S3 connection creation with real IDs"""
    
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
    
    print("Testing S3 connection creation...")
    print(f"Region: {region}")
    print(f"Domain: {domain_id}")
    print(f"Project: {project_id}")
    print(f"Environment: {env_id}")
    print("=" * 50)
    
    # First, clean up any existing test connections
    try:
        response = client.list_connections(
            domainIdentifier=domain_id,
            projectIdentifier=project_id
        )
        for conn in response.get('items', []):
            if conn['name'].startswith('test-s3-'):
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
            name=f"test-s3-{int(time.time())}",
            description="Test S3 connection",
            props={
                "s3Properties": {
                    "s3Uri": "s3://test-bucket/data/"
                }
            }
        )
        
        connection_id = response['connectionId']
        print(f"✅ S3 connection created: {connection_id}")
        
    except Exception as e:
        print(f"❌ Failed: {str(e)}")
        pytest.fail(f"S3 connection test failed: {str(e)}")
    
    finally:
        # Clean up the connection we just created
        if connection_id:
            try:
                client.delete_connection(
                    domainIdentifier=domain_id,
                    identifier=connection_id
                )
                print(f"✅ Cleaned up: {connection_id}")
            except Exception as e:
                print(f"⚠️ Cleanup warning: {e}")

if __name__ == "__main__":
    test_s3_connection()
    print("\n🎉 Ready to test all connection types!")
