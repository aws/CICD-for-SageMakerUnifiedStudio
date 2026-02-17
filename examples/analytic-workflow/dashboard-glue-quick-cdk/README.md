# QuickSight Dashboard with CDK-Managed Glue Infrastructure

This example is a CDK-based variant of `dashboard-glue-quick`. Instead of using MWAA Serverless
workflows to create Glue resources at runtime, all infrastructure is provisioned via AWS CDK
during the `cdk.deploy` bootstrap action.

## What CDK Provisions

The `infra/` CDK app deploys a single stack (`CovidEtlInfra-{stage}`) containing:

- **Glue databases**: `covid19_db`, `covid19_summary_db`
- **Glue tables**: `us_simplified` (CSV), `us_state_summary` (Parquet)
- **Glue jobs**: `setup-covid-db-job`, `summary-glue-job`, `set-permission-check-job`
- **Glue workflow**: DAG with conditional triggers (setup → summary → permissions)
- **IAM role**: Glue job execution role with S3, Catalog, LakeFormation, Athena access
- **Lake Formation permissions**: DESCRIBE/SELECT grants for configured roles

## Differences from `dashboard-glue-quick`

| Aspect | dashboard-glue-quick | dashboard-glue-quick-cdk |
|--------|---------------------|--------------------------|
| Infrastructure | Created by Glue jobs at runtime | Provisioned by CDK at deploy time |
| Bootstrap actions | `workflow.create` → `workflow.run` | `cdk.deploy` |
| Workflow dependency | Requires MWAA Serverless connection | No workflow connection needed |
| Rollback | Manual cleanup | `cdk.destroy` via CloudFormation |

## CDK Context Parameters

The stack accepts these context values (passed via `-c key=value` or manifest `context:`):

| Parameter | Description | Default |
|-----------|-------------|---------|
| `stage` | Deployment stage name | `dev` |
| `projectName` | DataZone project name | `marketing` |
| `s3Bucket` | DataZone project S3 bucket | (required) |
| `s3Prefix` | S3 prefix for shared data | `shared` |
| `iamRoleName` | Project IAM role for Glue jobs | (uses CDK-created role) |
| `grantRoles` | Comma-separated role names for LF permissions | (none) |

## Usage

```bash
# Deploy to dev
smus-cli deploy --targets dev --manifest examples/analytic-workflow/dashboard-glue-quick-cdk/manifest.yaml

# Deploy to test
smus-cli deploy --targets test --manifest examples/analytic-workflow/dashboard-glue-quick-cdk/manifest.yaml
```

## Prerequisites

Same as `dashboard-glue-quick` — see its README for QuickSight setup, S3 permissions, and
DataZone domain/project configuration.

Additionally, the CDK CLI must be installed: `npm install -g aws-cdk`
