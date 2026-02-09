#!/usr/bin/env python3
"""
Cleanup script to remove test artifacts from previous runs.
Run this BEFORE starting a new test to ensure clean state.
"""

import os
import sys
import boto3
from botocore.exceptions import ClientError


def cleanup_analyses(prefix, account_id, region):
    """Delete analyses matching prefix (prevents 5-entity limit errors)."""
    client = boto3.client("quicksight", region_name=region)
    deleted_count = 0
    
    try:
        response = client.list_analyses(AwsAccountId=account_id)
        analyses = response.get('AnalysisSummaryList', [])
        
        for analysis in analyses:
            if analysis['AnalysisId'].startswith(prefix):
                try:
                    print(f"  Deleting analysis: {analysis['AnalysisId']}")
                    client.delete_analysis(
                        AwsAccountId=account_id,
                        AnalysisId=analysis['AnalysisId']
                    )
                    print(f"    ✓ Deleted")
                    deleted_count += 1
                except ClientError as e:
                    if e.response['Error']['Code'] != 'ResourceNotFoundException':
                        print(f"    ✗ Error: {e}")
        
        if deleted_count == 0:
            print(f"  No analyses found with prefix '{prefix}' (already clean)")
        else:
            print(f"  ✓ Deleted {deleted_count} analyses")
        
        return deleted_count
        
    except Exception as e:
        print(f"  Error listing analyses: {e}")
        return 0


def cleanup_dashboard(dashboard_id, account_id, region):
    """Delete a dashboard if it exists."""
    client = boto3.client("quicksight", region_name=region)
    
    try:
        # Check if dashboard exists
        client.describe_dashboard(
            AwsAccountId=account_id,
            DashboardId=dashboard_id
        )
        
        # Dashboard exists, delete it
        print(f"  Deleting dashboard: {dashboard_id}")
        client.delete_dashboard(
            AwsAccountId=account_id,
            DashboardId=dashboard_id
        )
        print(f"    ✓ Deleted")
        return True
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"  Dashboard {dashboard_id} not found (already clean)")
            return False
        else:
            print(f"  Error checking dashboard: {e}")
            return False


def main():
    """Clean up test artifacts from previous run."""
    # Get from environment or use defaults
    account_id = os.environ.get("AWS_ACCOUNT_ID")
    region = os.environ.get("DEV_DOMAIN_REGION", "us-east-2")
    
    if not account_id:
        # Try to get from STS
        sts = boto3.client("sts")
        account_id = sts.get_caller_identity()["Account"]
    
    print("Cleaning up test artifacts from previous run...")
    print(f"Region: {region}")
    print(f"Account: {account_id}\n")
    
    # Clean up analyses first (critical to prevent 5-entity limit errors)
    print("1. Checking analyses with 'deployed-test' prefix...")
    cleanup_analyses("deployed-test", account_id, region)
    
    # Clean up deployed dashboard (from previous deploy)
    print("\n2. Checking deployed dashboard...")
    cleanup_dashboard("deployed-test-covid-dashboard", account_id, region)
    
    # Note: We keep test-covid-dashboard as it's the source for export
    print("\n3. Keeping source dashboard: test-covid-dashboard")
    print("   (This is the source for bundle export)\n")
    
    print("✅ Cleanup complete! Ready for new test run.")
    print("\nNext steps:")
    print("  1. Run: smus-cli bundle --targets dev")
    print("  2. Run: smus-cli deploy --targets test")


if __name__ == "__main__":
    main()
