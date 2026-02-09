# QuickSight Dashboard Deployment Workflow

This example demonstrates the complete workflow for deploying a QuickSight dashboard with Glue ETL pipeline across environments.

## Files

- **`TotalDeathByCountry.qs`** - Sample QuickSight dashboard bundle (reference only)
- **`setup_test_dashboard.py`** - Script to setup the QuickSight Dashboard from scratch
- **`cleanup_test.py`** - Script to clean up QuickSight resources

## Workflow Overview

The deployment workflow follows these steps:

1. **Deploy to Dev**: Creates Glue jobs, runs ETL workflow, creates QuickSight dashboard with data
2. **Customize Dashboard**: Make visual changes to the dashboard in dev QuickSight UI
3. **Bundle from Dev**: Exports the customized dashboard from dev QuickSight
4. **Deploy to Test**: Imports the customized dashboard to test environment

This ensures:
- Dashboard customizations are preserved across environments
- Data pipeline and dashboard are deployed together
- Each environment has properly prefixed resource IDs

## Usage

### Prerequisites

1. **AWS Credentials**: Configure credentials for both dev and test accounts
2. **QuickSight Setup**: QuickSight must be set up in both regions with the Admin user
3. **QuickSight S3 Permissions**: Grant S3 access to QuickSight service role (see below)
4. **DataZone Domain**: Tagged domain must exist in both accounts (use `aws datazone tag-resource` with domain ARN)
5. **SMUS Project**: SageMaker Unified Studio Project inside the domain with name listed in the manifest file

#### Grant QuickSight S3 Access

QuickSight needs S3 permissions to read data from the Glue Data Catalog. Add this policy to the QuickSight service role in **both dev and test accounts**:

```bash
# Get your account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Grant S3 access to QuickSight service role
aws iam put-role-policy \
  --role-name aws-quicksight-service-role-v0 \
  --policy-name QuickSightDataBucketAccess \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [{
      \"Effect\": \"Allow\",
      \"Action\": [\"s3:GetObject\", \"s3:GetObjectVersion\", \"s3:ListBucket\"],
      \"Resource\": [
        \"arn:aws:s3:::amazon-sagemaker-*-${ACCOUNT_ID}-*\",
        \"arn:aws:s3:::amazon-sagemaker-*-${ACCOUNT_ID}-*/*\"
      ]
    }]
  }"

echo "✓ Granted S3 access to QuickSight service role"
```

**Note**: The S3 bucket pattern `amazon-sagemaker-*-ACCOUNT_ID-*` matches the buckets created by SageMaker/DataZone for data storage.

### 1. Deploy to Dev Environment

Deploy the complete application (Glue jobs, workflows, and QuickSight dashboard) to dev:

```bash
smus-cli deploy --targets dev --manifest examples/analytic-workflow/dashboard-glue-quick/manifest.yaml
```

This will:
- Upload Glue job scripts and workflow definitions to S3
- Create and run the Glue workflow (`covid_dashboard_glue_quick_pipeline`)
- Create QuickSight dashboard with prefix `deployed-dev-covid-`
- Refresh the QuickSight dataset with ETL results

### 2. Customize Dashboard in Dev

Open the QuickSight dashboard in the dev account (us-east-2) and make your customizations:
- Add/modify visuals
- Change colors, layouts, filters
- Update titles and descriptions

The dashboard will be named `TotalDeathByCountry` with ID prefix `deployed-dev-covid-`.

### 3. Bundle from Dev

Export the customized dashboard from dev QuickSight:

```bash
smus-cli bundle --targets dev --manifest examples/analytic-workflow/dashboard-glue-quick/manifest.yaml
```

This exports the live dashboard from dev QuickSight and packages it into `./artifacts/IntegrationTestETLWorkflow.zip`.

**Note**: The manifest does NOT specify `assetBundle` for the QuickSight dashboard, which means it defaults to "export" mode (exports from live QuickSight).

### 4. Deploy to Test Environment

**First, switch to your test account credentials**
```bash
export AWS_ACCOUNT_ID=<test-account-id>
export TEST_DOMAIN_REGION=us-east-1
```

**List domains to find the domain ID**
```bash
aws datazone list-domains --region us-east-1 --query 'items[*].[id,name]' --output table
```

Use the SageMaker Console to create an IAM-based domain if you do not have it.

**Get the domain ID (replace with actual ID from above)**
```bash
DOMAIN_ID=$(aws datazone list-domains --region us-east-1 --query 'items[0].id' --output text)
```

**Tag the domain**
```bash
aws datazone tag-resource \
  --region us-east-1 \
  --resource-arn "arn:aws:datazone:us-east-1:${AWS_ACCOUNT_ID}:domain/$DOMAIN_ID" \
  --tags purpose=smus-cicd-testing

echo "Tagged domain: $DOMAIN_ID"
```

**Verify the tag was applied**
```bash
aws datazone list-tags-for-resource \
  --region us-east-1 \
  --resource-arn "arn:aws:datazone:us-east-1:${AWS_ACCOUNT_ID}:domain/$DOMAIN_ID"
```

**Create SMUS Project**
Create a project with the name matching the one set in the manifest file under the test stage.

**Deploy the bundled application to test:**
```bash
smus-cli deploy --targets test --manifest examples/analytic-workflow/dashboard-glue-quick/manifest.yaml
```

This will:
- Upload Glue job scripts and workflow definitions to S3
- Create and run the Glue workflow in test
- Import the customized dashboard from the bundle with prefix `deployed-test-covid-`
- Refresh the QuickSight dataset with test ETL results

Your dashboard customizations from dev will be preserved in test.

## Cleanup

To clean up resources in both environments:

```bash
python cleanup_test.py
```

## Troubleshooting

### QuickSight Dataset Refresh Fails

If the dataset refresh fails with S3 permission errors, ensure you've granted S3 access to the QuickSight service role (see Prerequisites section above).

If you haven't done this yet, run:

```bash
# Get your account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Grant S3 access to QuickSight service role
aws iam put-role-policy \
  --role-name aws-quicksight-service-role-v0 \
  --policy-name QuickSightDataBucketAccess \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [{
      \"Effect\": \"Allow\",
      \"Action\": [\"s3:GetObject\", \"s3:GetObjectVersion\", \"s3:ListBucket\"],
      \"Resource\": [
        \"arn:aws:s3:::amazon-sagemaker-*-${ACCOUNT_ID}-*\",
        \"arn:aws:s3:::amazon-sagemaker-*-${ACCOUNT_ID}-*/*\"
      ]
    }]
  }"
```

### Customizations Not Preserved

If your dashboard customizations don't appear in test:
1. Verify the manifest does NOT have `assetBundle: quicksight/TotalDeathByCountry.qs` line
2. Re-run bundle command to export from live QuickSight
3. Check bundle contents: `unzip -l ./artifacts/IntegrationTestETLWorkflow.zip | grep quicksight`

### Troubleshooting: 5-Entity Limit Error

Sometimes the QuickSight dashboard deployment can fail because it tries to import more than 5 analysis resources from the bundle. If that happens you will see:

```
✗ Error deploying dashboard: Import failed with status FAILED_ROLLBACK_IN_PROGRESS: 
[{'Arn': 'arn:aws:quicksight:us-east-1:123456789012:dashboard/deployed-test-covid-...', 
'Message': "... at 'linkEntities' failed to satisfy constraint: Member must have length less than or equal to 5"}]
```

**Root Cause**: The source dashboard in DEV has links to deleted analyses from previous failed deployments. These stale references get included in the bundle export.

**Solution**: Recreate the source dashboard cleanly in DEV:

```bash
# 1. Switch to DEV account credentials
export AWS_ACCOUNT_ID=<your-dev-account-id>
export DEV_DOMAIN_REGION=us-east-2
export QUICKSIGHT_USER=Admin  # or your QuickSight username

# 2. Clean up the stale dashboard and analyses in DEV
cd examples/analytic-workflow/dashboard-glue-quick/quicksight
python cleanup_test.py

# 3. Recreate clean dashboard from the reference bundle
python setup_test_dashboard.py

# 4. Bundle from the clean dashboard
cd ../../../..
smus-cli bundle \
  --manifest examples/analytic-workflow/dashboard-glue-quick/manifest.yaml \
  --targets dev \
  --output-dir ./my-bundles

# 5. Deploy to test
smus-cli deploy \
  --manifest examples/analytic-workflow/dashboard-glue-quick/manifest.yaml \
  --targets test \
  --bundle-archive-path ./my-bundles/*.zip
```

The `setup_test_dashboard.py` script imports the clean reference bundle (`TotalDeathByCountry.qs`) which has no linked analyses, ensuring a clean export.
