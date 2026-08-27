# CLI Commands Reference

← [Back to Main README](../README.md)

The SMUS CI/CD CLI provides 10 commands for managing CI/CD pipelines in SageMaker Unified Studio.

## Global Options

All commands support these global options:

| Option | Description | Values | Default |
|--------|-------------|--------|---------|
| `--log-level` | Control logging verbosity | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` | `INFO` |
| `--output` | Output format | `TEXT`, `JSON` | `TEXT` |
| `--manifest` / `-m` | Path to manifest file | File path | `manifest.yaml` |
| `--targets` / `-t` | Target environment | Target name(s) | Varies by command |

**Examples:**
```bash
# Debug mode for troubleshooting
aws-smus-cicd-cli describe --manifest manifest.yaml --log-level DEBUG

# Quiet mode - only errors
aws-smus-cicd-cli deploy --targets prod --log-level ERROR

# JSON output for automation
aws-smus-cicd-cli describe --manifest manifest.yaml --output JSON --log-level WARNING
```

**Environment Variable:**
```bash
# Set default log level
export SMUS_LOG_LEVEL=DEBUG
aws-smus-cicd-cli describe --manifest manifest.yaml
```

---

## Command Overview

| Command | Purpose | Example |
|---------|---------|---------|
| `create` | Create new bundle manifest | `aws-smus-cicd-cli create --output manifest.yaml` |
| `describe` | Validate and show bundle configuration | `aws-smus-cicd-cli describe --manifest manifest.yaml --connect` |
| `bundle` | Package files from source environment | `aws-smus-cicd-cli bundle --targets dev` |
| `deploy` | Deploy bundle to target environment | `aws-smus-cicd-cli deploy --targets test` |
| `run` | Execute workflow commands or trigger workflows | `aws-smus-cicd-cli run --workflow my_dag` |
| `logs` | Fetch workflow logs from CloudWatch | `aws-smus-cicd-cli logs --workflow arn:aws:airflow-serverless:region:account:workflow/name` |
| `monitor` | Monitor workflow status | `aws-smus-cicd-cli monitor --manifest manifest.yaml` |
| `test` | Run tests for pipeline targets | `aws-smus-cicd-cli test --targets marketing-test-stage` |
| `integrate` | Integrate with external tools (Q CLI) | `aws-smus-cicd-cli integrate qcli` |
| `destroy` | Delete all resources deployed by the manifest | `aws-smus-cicd-cli destroy --targets test --force` |

---

## Command Details

### 1. create - Create New Bundle Manifest

Creates a new bundle manifest file with basic structure.

```bash
aws-smus-cicd-cli create [OPTIONS]
```

#### Options
- **`-o, --output`**: Output file path for the bundle manifest (default: `manifest.yaml`)
- **`-n, --name`**: Pipeline name (optional, defaults to 'YourPipelineName')
- **`--domain-id`**: SageMaker Unified Studio domain ID (optional)
- **`--dev-project-id`**: Development project ID to base other targets on (optional)
- **`--targets`**: Comma-separated list of target stages to create in the manifest (default: `dev,test,prod`)
- **`--region`**: AWS region (default: `us-east-1`)

#### Examples

```bash
# Create basic bundle manifest
aws-smus-cicd-cli create

# Create with custom output file and name
aws-smus-cicd-cli create --output my-manifest.yaml --name MyPipeline

# Create with specific stages and region
aws-smus-cicd-cli create --output manifest.yaml --targets dev,test,prod --region us-west-2
```

---

### 2. describe - Describe Pipeline Configuration

Validates and displays information about your bundle manifest.

```bash
aws-smus-cicd-cli describe [OPTIONS]
```

#### Options
- **`-m, --manifest`**: Path to bundle manifest file (default: `manifest.yaml`)
- **`-t, --targets`**: Target name(s) - single target or comma-separated list (optional, defaults to all targets)
- **`-o, --output`**: Output format: TEXT (default) or JSON
- **`-w, --workflows`**: Show workflow information
- **`-c, --connections`**: Show connection information
- **`--connect`**: Connect to AWS account and pull additional information

#### Examples

```bash
# Basic describe
aws-smus-cicd-cli describe

# Describe specific targets with workflows
aws-smus-cicd-cli describe -t dev,test -w

# Describe with AWS connection info in JSON format
aws-smus-cicd-cli describe --connect -o JSON

# Describe specific pipeline file
aws-smus-cicd-cli describe -m my-manifest.yaml
```

#### Example Output
```
Pipeline: IntegrationTestMultiTarget
Domain: cicd-test-domain (us-east-1)

Stages:
  - dev: dev-marketing
    Project Name: dev-marketing
    Project ID: <dev-project-id>
    Status: ACTIVE
    Owners: Admin, eng1
    Connections:
      project.workflow_mwaa:
        connectionId: 6f58emph2gtciv
        type: WORKFLOWS_MWAA
        region: us-east-1
        awsAccountId: <aws-account-id>
        description: Connection for MWAA environment
        environmentName: DataZoneMWAAEnv-<domain-id>-<project-id>-dev
      project.workflow_serverless:
        connectionId: 7g69fnqi3hukjw
        type: WORKFLOWS_SERVERLESS
        region: us-east-1
        awsAccountId: <aws-account-id>
        description: Serverless workflows connection
      default.s3_shared:
        connectionId: dqbxjn28zehzjb
        type: S3
        region: us-east-1
        awsAccountId: <aws-account-id>
        description: This is the connection to interact with s3 shared storage location if enabled in the project.
        s3Uri: s3://sagemaker-unified-studio-<aws-account-id>-us-east-1-your-domain-name/<domain-id>/<dev-project-id>/shared/
        status: READY

Manifest Workflows:
  - test_dag (Connection: project.workflow_mwaa, Engine: MWAA)
  - runGettingStartedNotebook (Connection: project.workflow_mwaa, Engine: MWAA)
```

The describe command validates your bundle configuration and displays the structure of your bundle — each target environment with their associated projects, connections, and workflows.

---

### 3. bundle - Create Deployment Packages

Creates bundle zip files by downloading from S3.

```bash
aws-smus-cicd-cli bundle [OPTIONS] [TARGET_POSITIONAL]
```

#### Options
- **`-m, --manifest`**: Path to bundle manifest file (default: `manifest.yaml`)
- **`-t, --targets`**: Target name(s) - single target or comma-separated list (uses default target if not specified)
- **`-d, --output-dir`**: Output directory for bundle files (default: `./bundles`)
- **`-o, --output`**: Output format: TEXT (default) or JSON
- **`TARGET_POSITIONAL`**: Target name (positional argument for backward compatibility)

#### Bundle Storage Locations

The bundle command supports both local and S3 storage locations via the `bundlesDirectory` configuration in your bundle manifest:

**Local Storage:**
```yaml
bundlesDirectory: ./bundles
```
- Bundles are created directly in the specified local directory
- Suitable for development and single-user workflows

**S3 Storage:**
```yaml
bundlesDirectory: s3://my-datazone-bucket/bundles
```
- Bundles are created locally then uploaded to S3
- Enables team collaboration and CI/CD integration
- Works with DataZone domain S3 buckets
- Requires appropriate S3 permissions

#### Examples

```bash
# Bundle default target
aws-smus-cicd-cli bundle

# Bundle specific targets
aws-smus-cicd-cli bundle --targets dev,test

# Bundle to custom directory
aws-smus-cicd-cli bundle --output-dir /path/to/bundles

# Bundle with JSON output
aws-smus-cicd-cli bundle --output JSON

# Bundle using positional argument (backward compatibility)
aws-smus-cicd-cli bundle dev
```

#### Example Output
```
Creating bundle for target: dev
Project: dev-marketing
Downloading workflows from S3: default.s3_shared (append: True)
  Downloaded: workflows/dags/test_dag.py
  Downloaded: workflows/.visual/runGettingStartedNotebook.wf
  Downloaded 17 workflow files from S3
Downloading storage from S3: default.s3_shared (append: False)
  Downloaded: src/test-notebook1.ipynb
  Downloaded 1 storage files from S3
Creating archive: IntegrationTestMultiTarget.zip
✅ Bundle created: s3://my-datazone-bucket/bundles/IntegrationTestMultiTarget.zip (279462 bytes)

📦 Bundle Contents:
==================================================
├── storage/
│   └── src/
│       └── test-notebook1.ipynb
└── workflows/
    ├── .visual/
    │   └── runGettingStartedNotebook.wf
    ├── dags/
    │   ├── test_dag.py
    │   └── visual/
    │       └── runGettingStartedNotebook.py
    └── config/
        ├── requirements.txt
        └── startup.sh
==================================================
📊 Total files: 18
Bundle creation complete for target: dev
```

---

### 4. deploy - Deploy to Targets

Deploys bundle files to target environments (auto-initializes if needed). The deploy command performs the following operations:

0. **Pre-Deployment Validation (automatic)**: Runs a best-effort dry-run validation before deployment to catch errors early. If any blocking errors are found, the deployment is aborted before any resources are created or modified. This prevents partial deployments that leave resources in an inconsistent state. Note that a passing validation does not guarantee deployment success — transient errors or state changes may still occur. Skip with `--skip-validation`.
1. **Bundle Deployment**: Uploads workflow and storage files to target project connections
2. **Catalog Asset Access**: Processes catalog assets defined in the bundle manifest:
   - Searches for assets in the DataZone catalog
   - Creates subscription requests for required access
   - Waits for subscription approval (up to 5 minutes)
   - Verifies subscription grants are completed
   - Fails deployment if catalog access cannot be obtained
3. **Notebook Sync**: Syncs notebooks from the bundle into the target project:
   - Uploads .ipynb files to the target project's S3 shared bucket
   - Calls StartNotebookSync to create or update notebooks
   - Applies metadata, parameters, and environment configuration via UpdateNotebook
   - Tracks source-to-target mapping for idempotent updates
4. **Workflow Validation**: Ensures deployed workflows are accessible by the target environment
5. **Bootstrap Actions**: Executes post-deployment actions defined in the manifest (if configured) — see [Bootstrap Actions](bootstrap-actions.md)
6. **Deployment Metrics**: Optionally emits deployment lifecycle events to EventBridge — see [Bundle Deployment Metrics](pipeline-deployment-metrics.md)

```bash
aws-smus-cicd-cli deploy [OPTIONS] [TARGET_POSITIONAL]
```

#### Options
- **`-m, --manifest`**: Path to bundle manifest file (default: `manifest.yaml`)
- **`-t, --targets`**: Target name(s) - single target or comma-separated list (uses default target if not specified)
- **`-b, --bundle-archive-path`**: Path to pre-created bundle file (optional)
- **`--dry-run`**: Preview the deployment without making any changes. Validates the manifest, bundle, IAM permissions, resource reachability, catalog dependencies, and workflow definitions, then produces a structured report of what would happen and any issues detected. No resources are created, modified, or deleted. This is a best-effort check — a passing dry run does not guarantee deployment success.
- **`--output`**: Output format for the dry-run report: `text` (default, human-readable) or `json` (machine-readable). Only applies when `--dry-run` is used.
- **`--skip-validation`**: Skip the automatic pre-deployment dry-run validation step and proceed directly to deployment. Useful when you have already validated with `--dry-run` or need to bypass validation for speed.
- **`--emit-events`**: Enable EventBridge event emission for deployment tracking
- **`--no-events`**: Disable EventBridge event emission
- **`--event-bus-name`**: Custom EventBridge event bus name
- **`TARGET_POSITIONAL`**: Target name (positional argument for backward compatibility)

#### Examples

```bash
# Deploy to default target
aws-smus-cicd-cli deploy

# Deploy to specific targets
aws-smus-cicd-cli deploy --targets test,prod

# Deploy with pre-created bundle
aws-smus-cicd-cli deploy --targets test --manifest /path/to/bundle.zip

# Deploy with EventBridge monitoring enabled
aws-smus-cicd-cli deploy --targets prod --emit-events

# Deploy using positional argument (backward compatibility)
aws-smus-cicd-cli deploy test

# Preview deployment without making changes (dry run)
aws-smus-cicd-cli deploy --dry-run --targets test

# Dry run with JSON output for automation
aws-smus-cicd-cli deploy --dry-run --targets test --output json

# Skip pre-deployment validation for faster deployment
aws-smus-cicd-cli deploy --targets test --skip-validation
```

#### Dry Run Mode

Use `--dry-run` to preview a deployment without creating, modifying, or deleting any resources. The dry run walks through every deployment phase in read-only mode:

> **Note:** The dry run is a best-effort validation. A passing dry run significantly reduces the risk of deployment failure but does not guarantee success. Conditions such as transient AWS service errors, IAM policy changes between validation and deployment, concurrent resource modifications, eventual consistency delays, and service quota limits may cause a deployment to fail even after a clean dry-run report.

1. **Manifest Validation** — Loads and validates the manifest YAML, resolves the target stage, builds domain configuration, checks environment variable references
2. **Bundle Exploration** — Opens the bundle archive, enumerates files, validates catalog export data and notebook manifest if present
3. **Permission Verification** — Uses `iam:SimulatePrincipalPolicy` to check that the current IAM identity has all required permissions (S3, DataZone, Glue, IAM, QuickSight, Airflow, etc.). Also checks DataZone policy grants on the project's domain unit when catalog resources are present.
4. **Connectivity & Reachability** — Verifies that the DataZone domain and project are reachable, S3 buckets are accessible, and Airflow environments respond
5. **Project Initialization** — Checks whether the target project exists or would need to be created
6. **Deployment Simulation** — Simulates each deployment phase (QuickSight, storage, git, catalog import, workflows, bootstrap actions) and reports what would happen
7. **Dependency Validation** — Checks that pre-existing AWS resources referenced by catalog export data exist in the target environment
8. **Workflow Validation** — Validates workflow YAML files for correct syntax, required Airflow DAG keys, and environment variable references

The report ends with a Resource Deployment Outlook section that groups resources by deployment phase and shows which will deploy successfully vs. which will fail.

**Example Dry Run Output (text):**
```
Dry Run Report
========================================

--- Manifest Validation ---
  ✅ Manifest loaded successfully from manifest.yaml
  ✅ Target stage 'test' resolved successfully
  ✅ Domain configuration built successfully
  ✅ All environment variable references are resolved

--- Bundle Exploration ---
  ✅ Bundle resolved from ./artifacts: ./artifacts/MyApp.zip
  ✅ Bundle contains 1 file(s)
  ✅ Catalog export validated: 19 resource(s) found

--- Permission Verification ---
  ✅ Caller identity: arn:aws:sts::123456789012:assumed-role/Admin/session
  ✅ Permission 'datazone:GetDomain' allowed on *
  ✅ Permission 's3:PutObject' allowed on arn:aws:s3:::my-bucket/*
  ⚠️  Could not verify quicksight:DescribeDashboard: AccessDenied

--- Connectivity & Reachability ---
  ✅ DataZone domain 'dzd-abc123' is reachable
  ✅ DataZone project 'my-project' exists in domain 'dzd-abc123'
  ✅ S3 bucket 'my-bucket' is accessible

--- Project Initialization ---
  ✅ Project 'my-project' exists in domain 'dzd-abc123'. No creation needed.

--- Catalog Import ---
  ✅ Catalog import would process 19 resource(s): 1 glossaries, 3 glossary terms, ...

--- Workflow Validation ---
  ✅ No workflows configured; skipping.

--- Bootstrap Actions ---
  ✅ No bootstrap actions configured.

========================================
Resource Deployment Outlook
----------------------------------------
  Will deploy (15):
    [Catalog Import]
      ✅ Business_Glossary  (glossaries)
      ✅ my_dataset  (assets)
      ...
  Will fail (1):
    [Permission Verification]
      ❌ CREATE_GLOSSARY  (datazone)
         └─ DataZone policy grant 'CREATE_GLOSSARY' is MISSING on domain unit 'dpxyz123'.
========================================
Summary: 48 OK, 1 warning(s), 1 error(s)
```

**Note on DataZone permissions:** When the manifest identifies the domain by tags (rather than a direct `domain: id:`), the dry run automatically resolves the domain ID via DataZone API. For `SimulatePrincipalPolicy`, if the domain ID or account ID cannot be resolved, a wildcard resource ARN (`*`) is used instead of a partial-wildcard ARN to avoid false-positive permission denials.

**Example Dry Run Output (json):**
```json
{
  "summary": { "ok": 48, "warnings": 1, "errors": 1 },
  "phases": {
    "Manifest Validation": [
      { "severity": "OK", "message": "Manifest loaded successfully from manifest.yaml" },
      { "severity": "OK", "message": "Target stage 'test' resolved successfully" }
    ],
    "Permission Verification": [
      { "severity": "OK", "message": "Permission 'datazone:GetDomain' allowed on *" },
      { "severity": "WARNING", "message": "Could not verify quicksight:DescribeDashboard: AccessDenied" }
    ]
  },
  "resource_outlook": {
    "will_deploy": [
      { "resource": "Business_Glossary", "type": "glossaries", "phase": "Catalog Import" }
    ],
    "will_fail": [
      { "resource": "CREATE_GLOSSARY", "service": "datazone", "phase": "Permission Verification",
        "message": "DataZone policy grant 'CREATE_GLOSSARY' is MISSING on domain unit 'dpxyz123'." }
    ]
  }
}
```

Exit codes for dry run:
- `0` — All checks passed (zero errors). Deployment is expected to succeed.
- `1` — One or more blocking errors detected. Deployment would fail.

#### Pre-Deployment Validation

By default, every `deploy` invocation (without `--dry-run`) automatically runs a dry-run validation step before beginning the actual deployment. This catches errors early and prevents partial deployments. Note that this validation is best-effort — it cannot detect transient failures or state changes that occur after validation completes.

- If the validation finds errors, the deployment is aborted and the report is displayed.
- If the validation finds only warnings or passes cleanly, the deployment proceeds normally.
- Use `--skip-validation` to bypass this step when you've already validated or need faster deployments.

#### Example Output
```
Deploying to target: test
Project: integration-test-test
Domain: cicd-test-domain
Region: us-east-1
🔧 Auto-initializing target infrastructure...
✅ Target infrastructure ready
✅ Project 'integration-test-test' exists
Bundle file: s3://my-datazone-bucket/bundles/IntegrationTestMultiTarget.zip
Downloading bundle from S3...
Deploying storage to: default.s3_shared/src (append: False)
    Synced: test-notebook1.ipynb
  Storage files synced: 1
Deploying workflows to: default.s3_shared/workflows (append: True)
    Synced: test_dag.py
    Synced: runGettingStartedNotebook.py
  Workflow files synced: 17
✅ Deployment complete! Total files synced: 18

🚀 Starting workflow validation...
✅ MWAA environment is available
🆕 New DAGs detected: runGettingStartedNotebook
```

---

### 5. monitor - Monitor Workflow Status

Monitors workflow status across target environments.

```bash
aws-smus-cicd-cli monitor [OPTIONS]
```

#### Options
- **`-m, --manifest`**: Path to bundle manifest file (default: `manifest.yaml`)
- **`-t, --targets`**: Target name(s) - single target or comma-separated list (shows all targets if not specified)
- **`-l, --live`**: Keep monitoring until all workflows complete
- **`-o, --output`**: Output format: TEXT (default) or JSON

#### Examples

```bash
# Monitor all targets (one-time snapshot)
aws-smus-cicd-cli monitor

# Monitor specific targets
aws-smus-cicd-cli monitor -t dev,test

# Live monitoring - continuously poll until workflows complete
aws-smus-cicd-cli monitor --live

# Monitor with JSON output
aws-smus-cicd-cli monitor -o JSON
```

#### Live Monitoring

When using `--live`, the monitor command:
1. Displays initial table with all workflow statuses
2. Polls every 10 seconds for status changes
3. Reports status changes as new lines: `[HH:MM:SS] workflow_name (run id): OLD_STATUS → NEW_STATUS`
4. Exits automatically when no workflows are RUNNING or QUEUED
5. Can be stopped manually with Ctrl+C

#### Example Output
```
Pipeline: IntegrationTestMultiTarget
Domain: cicd-test-domain (us-east-1)

🔍 Monitoring Status:

🎯 Stage: test
   Project: integration-test-test
   📊 Workflow Status:
      🔧 project.workflow_mwaa
         🔄 ✓ test_dag
            Schedule: Manual | Status: ACTIVE | Recent: Unknown
         🔄 ✓ runGettingStartedNotebook
            Schedule: Manual | Status: ACTIVE | Recent: Unknown
```

---

### 6. run - Run Workflow Commands

Executes workflow commands on target environments (supports both MWAA and serverless Airflow).

```bash
aws-smus-cicd-cli run [OPTIONS]
```

#### Options
- **`-w, --workflow`**: Workflow name to run (optional)
- **`-c, --command`**: Airflow CLI command to execute (optional)
- **`-t, --targets`**: Target name(s) - single target or comma-separated list (optional, defaults to first available)
- **`-m, --manifest`**: Path to bundle manifest file (default: `manifest.yaml`)
- **`-o, --output`**: Output format: TEXT (default) or JSON

#### Examples

```bash
# Trigger workflow (works with both MWAA and serverless Airflow)
aws-smus-cicd-cli run --workflow my_dag

# Run Airflow CLI command (MWAA only)
aws-smus-cicd-cli run --workflow my_dag --command version

# List all DAGs
aws-smus-cicd-cli run --workflow sample_dag --command "dags list"

# Run on specific target with JSON output
aws-smus-cicd-cli run --workflow my_dag --targets prod --output JSON
```

#### Example Output (MWAA)
```
🎯 Stage: test
🚀 Triggering workflow: test_dag
🔧 Connection: project.workflow_mwaa (DataZoneMWAAEnv-dzd_6je2k8b63qse07-broygppc8vw17r-dev)
📋 Command: dags trigger test_dag
✅ Command executed successfully
```

#### Example Output (Amazon MWAA Serverless)
```
🎯 Stage: test (Amazon MWAA Serverless)
🚀 Starting workflow run: MyPipeline_test_test_dag
🔗 ARN: arn:aws:airflow-serverless:us-east-2:123456789012:workflow/MyPipeline_test_test_dag
✅ Workflow run started successfully
📋 Run ID: manual__2025-10-15T15:45:00+00:00
📊 Status: STARTING
```

---

### 7. logs - Fetch Workflow Logs

Fetches and displays workflow logs from CloudWatch (supports serverless Airflow workflows).

```bash
aws-smus-cicd-cli logs [OPTIONS]
```

#### Options
- **`-w, --workflow`**: Workflow ARN to fetch logs for (required)
- **`-l, --live`**: Keep fetching logs until workflow terminates
- **`-o, --output`**: Output format: TEXT (default) or JSON
- **`-n, --lines`**: Number of log lines to fetch (default: 100)

#### Examples

```bash
# Fetch logs for serverless Airflow workflow
aws-smus-cicd-cli logs --workflow arn:aws:airflow-serverless:us-east-2:123456789012:workflow/MyWorkflow

# Live log monitoring
aws-smus-cicd-cli logs --workflow arn:aws:airflow-serverless:us-east-2:123456789012:workflow/MyWorkflow --live

# Fetch specific number of lines with JSON output
aws-smus-cicd-cli logs --workflow arn:aws:airflow-serverless:us-east-2:123456789012:workflow/MyWorkflow --lines 50 --output JSON
```

#### Example Output
```
📋 Fetching logs for workflow: MyPipeline_test_test_dag
🔗 ARN: arn:aws:airflow-serverless:us-east-2:123456789012:workflow/MyPipeline_test_test_dag
================================================================================
📁 Log Group: /aws/mwaa-serverless/MyPipeline_test_test_dag/
📊 Workflow Status: ACTIVE
--------------------------------------------------------------------------------
📄 Showing 15 log events:

[15:45:23] [scheduler] Starting workflow execution
[15:45:24] [task-runner] Initializing S3ListOperator task
[15:45:25] [task-runner] Task completed successfully
```

---

### 8. test - Run Pipeline Tests

Runs Python tests from the configured test folder against your deployed bundle.

```bash
aws-smus-cicd-cli test [OPTIONS]
```

#### Options
- **`-m, --manifest`**: Path to bundle manifest file (default: `manifest.yaml`)
- **`-t, --targets`**: Target name(s) - single target or comma-separated list (optional, defaults to all targets)
- **`-o, --output`**: Output format: TEXT (default) or JSON
- **`-v, --verbose`**: Show detailed test output
- **`--test-output`**: Test output mode: `console` (stream test output directly)

#### Examples

```bash
# Run tests for all targets
aws-smus-cicd-cli test --manifest manifest.yaml

# Run tests for specific targets with verbose output
aws-smus-cicd-cli test --targets test --verbose

# Stream test output directly to console
aws-smus-cicd-cli test --targets test --test-output console
```

#### Example Output
```
Pipeline: IntegrationTestMultiTarget
Domain: cicd-test-domain (us-east-1)

🎯 Stage: test
  📁 Test folder: tests/
  🔧 Project: integration-test-test (your-project-id)
  🧪 Running tests...
  ✅ Tests passed

🎯 Test Summary:
  ✅ Passed: 1
  ❌ Failed: 0
  ⚠️  Skipped: 0
  🚫 Errors: 0
```

---

### 9. integrate - Integrate with External Tools

Registers SMUS CI/CD CLI as an MCP (Model Context Protocol) server with Amazon Q CLI.

```bash
aws-smus-cicd-cli integrate <tool> [OPTIONS]
```

#### Options
- **`tool`**: Tool to integrate with (currently: `qcli`)
- **`--status`**: Show integration status
- **`--uninstall`**: Uninstall integration
- **`--configure`**: Path to custom MCP configuration file

#### Examples

```bash
# Setup Q CLI integration (MCP server)
aws-smus-cicd-cli integrate qcli

# Check integration status
aws-smus-cicd-cli integrate qcli --status

# Uninstall integration
aws-smus-cicd-cli integrate qcli --uninstall
```

#### Available MCP Tools
- `get_pipeline_example` - Generate bundle manifests from templates
- `query_smus_kb` - Search SMUS documentation and examples
- `validate_pipeline` - Validate manifest.yaml against schema

#### Example Q CLI Usage
```bash
q chat

You: Show me a notebooks pipeline example
Q: [Returns complete notebooks_manifest.yaml with explanations]

You: Validate my manifest.yaml
Q: [Validates and reports any schema errors]
```

**Logs:** `/tmp/smus_mcp_server.log`

---

### 10. destroy - Destroy All Deployed Resources

Deletes all resources previously deployed by the manifest: QuickSight dashboards/datasets/data sources, S3 objects at declared target paths, Airflow serverless workflows, workflow-created resources (e.g. Glue jobs), DataZone catalog resources, and optionally the DataZone project. This is the inverse of `deploy`.

> ⚠️ **This operation is irreversible.** Destroyed resources cannot be recovered. Always review the destruction plan carefully before confirming, and consider running without `--force` first to inspect what will be deleted.

```bash
aws-smus-cicd-cli destroy [OPTIONS]
```

#### Options

| Option | Short | Description | Example |
|--------|-------|-------------|---------|
| `--manifest` | `-m` | Path to application manifest file (default: `manifest.yaml`) | `--manifest manifest.yaml` |
| `--targets` | `-t` | Stage name(s) — single or comma-separated (required) | `--targets test` |
| `--force` | `-f` | Skip confirmation prompt | `--force` |
| `--output` | `-o` | Output format: TEXT (default) or JSON | `--output JSON` |

#### Examples

```bash
# Destroy a specific stage with confirmation prompt
aws-smus-cicd-cli destroy --targets test --manifest manifest.yaml

# Destroy multiple stages
aws-smus-cicd-cli destroy --targets test,prod --manifest manifest.yaml

# Destroy without confirmation (CI/CD pipelines)
aws-smus-cicd-cli destroy --targets test --force

# Destroy with JSON output for automation
aws-smus-cicd-cli destroy --targets test --force --output JSON
```

#### Example Output (TEXT format)

```
🔍 Validating 1 stage(s)...
  Validating stage 'test'...

📋 Destruction plan:

  Stage: test
    airflow_workflow:
      - MyApp_test-project_my_dag
    glue_job:
      - setup-covid-db-job
      - summary-glue-job
    quicksight_dashboard:
      - deployed-test-covid-TotalDeathByCountry
    quicksight_dataset:
      - deployed-test-covid-TotalDeathByCountry
    quicksight_data_source:
      - deployed-test-covid-TotalDeathByCountry
    s3_prefix:
      - dashboard-glue-quick/bundle
      - repos

  Total resources to process: 7

⚠️  WARNING: The destroy command deletes ALL resources listed above based on the
manifest and workflow definitions. This includes resources that may have been created
manually by users, not only those created by the CI/CD tool. Review the destruction
plan carefully before confirming.

⚠️  WARNING: This will permanently delete the above resources!
Are you sure you want to proceed with destruction? [y/n]: y

🗑️  Starting destruction...

  Stage: test

📊 Destruction Summary
  test: deleted=7 not_found=0 skipped=0 error=0
```

#### Example Output (JSON format)

```json
{
  "application_name": "MyApp",
  "targets": ["test"],
  "stages": {
    "test": [
      {
        "resource_type": "airflow_workflow",
        "resource_id": "MyApp_test-project_my_dag",
        "status": "deleted",
        "message": "Workflow deleted"
      },
      {
        "resource_type": "glue_job",
        "resource_id": "setup-covid-db-job",
        "status": "deleted",
        "message": "Glue job deleted"
      },
      {
        "resource_type": "s3_prefix",
        "resource_id": "dashboard-glue-quick/bundle",
        "status": "deleted",
        "message": "S3 objects deleted"
      }
    ]
  }
}
```

#### Behavior

The command follows a strict two-phase model:

**Phase 1 — Validation (read-only):**
- Resolves domain and project IDs via DataZone
- Resolves S3 connections to determine bucket/prefix targets
- Enumerates QuickSight resources by prefix and detects collisions
- Checks for active Airflow workflow runs
- Fetches and parses workflow YAML files from S3 to discover workflow-created resources (e.g. Glue jobs)
- Collects ALL errors across ALL stages before aborting — no early exit

**Phase 2 — Destruction (after confirmation):**

Resources are deleted in this fixed dependency order per stage:
1. Delete workflow-created resources (Glue jobs, etc.)
2. Delete Airflow workflows (active runs are automatically terminated)
3. Delete bootstrap connections (created by `datazone.create_connection` actions)
4. Delete QuickSight dashboards → datasets → data sources
5. Delete S3 objects at declared `targetDirectory` prefixes
6. Delete catalog resources (glossaries, terms, form types, asset types, assets, data products) in reverse dependency order
7. Delete DataZone project (only if `project.create: true` in manifest)

**Key behaviors:**
- **Idempotent**: Resources already absent are logged as `not_found`, not errors. Safe to run multiple times.
- **Single confirmation gate**: All confirmations happen in one prompt after the full plan is printed.
- **⚠️ Deletes all matching resources**: The destroy command deletes ALL resources listed in the manifest and workflow definitions, including resources that may have been created manually by users — not only those originally created by the CI/CD tool. Review the destruction plan carefully before confirming.
- **Active runs handled automatically**: MWAA Serverless terminates active workflow runs when a workflow is deleted. The validation phase detects and reports active runs in the destruction plan for visibility, but no explicit stopping is required.
- **Collision detection**: If more QuickSight resources or Airflow workflows match than declared in the manifest, destroy aborts before deleting anything.
- **Built-in connections protected**: `default.*` connections are never deleted.
- **Managed catalog resources protected**: Resources with `amazon.datazone.*` namespace are never deleted.
- **Catalog warning**: When `deployment_configuration.catalog` is present, all project-owned catalog resources are deleted (not just imported ones). Set `disable: true` under `deployment_configuration.catalog` to skip catalog deletion.

#### Use Cases

- **Clean re-deployment**: Destroy a stage and redeploy from scratch to resolve state drift
- **Environment teardown**: Remove test or staging environments after validation
- **Rollback**: Remove a failed deployment before re-deploying a previous version
- **Project cleanup**: Remove all resources before deleting the project

#### Notes
- Requires sufficient IAM permissions to list and delete all resource types declared in the manifest (QuickSight, S3, Airflow Serverless, DataZone, Glue)
- S3 bucket and prefix resolution requires a live DataZone API call — the project must be reachable at destroy time
- Use `--force` in CI/CD pipelines to skip the interactive confirmation prompt; note that `--force` with `--output JSON` is required since JSON mode cannot prompt interactively
- The `Resource_Prefix` configured in `deployment_configuration.quicksight.overrideParameters` must be unique to avoid collision errors

---

## Output Formats

### TEXT Format (Default)
- Human-readable output with emojis and formatting
- Raw stdout/stderr for run commands
- Suitable for interactive use

### JSON Format
- Structured data output
- Suitable for automation and scripting
- All commands support JSON output via `--output JSON`

## Exit Codes

- **0**: Success
- **1**: Error (check error message for details)

## Configuration Files

### Manifest
- Default location: `manifest.yaml` (current directory)
- Override with `--manifest` option
- See [Manifest Reference](manifest.md) for format

### AWS Configuration
- Uses standard AWS credential chain
- Supports AWS profiles and environment variables
- Region can be specified in bundle manifest or AWS config

## Common Workflows

### Development Workflow
```bash
# 1. Create new pipeline
aws-smus-cicd-cli create -o my-manifest.yaml

# 2. Validate configuration
aws-smus-cicd-cli describe --manifest my-manifest.yaml

# 3. Create bundle from dev
aws-smus-cicd-cli bundle --manifest my-manifest.yaml --targets dev

# 4. Deploy to test
aws-smus-cicd-cli deploy --manifest my-manifest.yaml --targets test

# 5. Monitor deployment
aws-smus-cicd-cli monitor --manifest my-manifest.yaml --targets test

# 6. Run workflow commands
aws-smus-cicd-cli run --workflow my_dag --command "dags list" --targets test
```

### Cleanup Workflow
```bash
# Destroy all deployed resources (QuickSight, S3, Airflow, Glue, catalog, project)
aws-smus-cicd-cli destroy --targets test --force

# Destroy specific stage for clean re-deployment
aws-smus-cicd-cli destroy --targets test --manifest manifest.yaml
```

### Rollback Workflow
```bash
# Option 1: Redeploy previous bundle (no destroy needed if no resource conflicts)
aws s3 cp s3://bucket/bundles/MyApp-v1.2.0.zip ./artifacts/MyApp.zip
aws-smus-cicd-cli deploy --targets prod --manifest manifest.yaml

# Option 2: Destroy bad deployment then redeploy previous version (clean slate)
aws-smus-cicd-cli destroy --targets prod --force
aws s3 cp s3://bucket/bundles/MyApp-v1.2.0.zip ./artifacts/MyApp.zip
aws-smus-cicd-cli deploy --targets prod --manifest manifest.yaml
aws-smus-cicd-cli test --targets prod
```

**See more:** [Rollback Guide](rollback-guide.md)
