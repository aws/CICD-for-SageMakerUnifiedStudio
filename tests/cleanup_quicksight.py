import os
import boto3
from botocore.exceptions import ClientError

region = os.environ.get('AWS_REGION', os.environ.get('DOMAIN_REGION', 'us-east-2'))
# Get account ID from STS
sts = boto3.client('sts')
account_id = sts.get_caller_identity()['Account']

qs = boto3.client('quicksight', region_name=region)

print(f"Cleaning up QuickSight resources in account {account_id}, region {region}")

# Delete analyses starting with "deployed-test" (prevents 5-entity limit errors)
try:
    analyses = qs.list_analyses(AwsAccountId=account_id)['AnalysisSummaryList']
    for analysis in analyses:
        if analysis['AnalysisId'].startswith('deployed-test'):
            try:
                qs.delete_analysis(AwsAccountId=account_id, AnalysisId=analysis['AnalysisId'])
                print(f"✓ Deleted analysis: {analysis['AnalysisId']}")
            except ClientError as e:
                if e.response['Error']['Code'] != 'ResourceNotFoundException':
                    print(f"✗ Error deleting analysis {analysis['AnalysisId']}: {e}")
except Exception as e:
    print(f"✗ Error listing analyses: {e}")

# Delete dashboards starting with "deployed-test"
dashboards = qs.list_dashboards(AwsAccountId=account_id)['DashboardSummaryList']
for dash in dashboards:
    if dash['DashboardId'].startswith('deployed-test'):
        try:
            qs.delete_dashboard(AwsAccountId=account_id, DashboardId=dash['DashboardId'])
            print(f"✓ Deleted dashboard: {dash['DashboardId']}")
        except Exception as e:
            print(f"✗ Error deleting dashboard {dash['DashboardId']}: {e}")

# Delete datasets starting with "deployed-test"
datasets = qs.list_data_sets(AwsAccountId=account_id)['DataSetSummaries']
for ds in datasets:
    if ds['DataSetId'].startswith('deployed-test'):
        try:
            qs.delete_data_set(AwsAccountId=account_id, DataSetId=ds['DataSetId'])
            print(f"✓ Deleted dataset: {ds['DataSetId']}")
        except Exception as e:
            print(f"✗ Error deleting dataset {ds['DataSetId']}: {e}")

# Delete data sources starting with "deployed-test"
sources = qs.list_data_sources(AwsAccountId=account_id)['DataSources']
for src in sources:
    if src['DataSourceId'].startswith('deployed-test'):
        try:
            qs.delete_data_source(AwsAccountId=account_id, DataSourceId=src['DataSourceId'])
            print(f"✓ Deleted data source: {src['DataSourceId']}")
        except Exception as e:
            print(f"✗ Error deleting data source {src['DataSourceId']}: {e}")

print("✓ Cleanup complete")
