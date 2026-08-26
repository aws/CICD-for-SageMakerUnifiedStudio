---
name: generate-bundle-manifest
description: >-
  Generate an SMUS CI/CD deployment manifest (and orchestration workflow) to replicate a
  SageMaker Unified Studio project environment across stages. Use when the user asks to:
  generate/create/scaffold a manifest, bundle my project, replicate environment, set up
  CI/CD, promote to test/prod, deploy my project to prod, move my project to another
  environment, package my code for deployment, set up multi-account deployment, copy
  environment to production, or asks how to promote code across environments. Does NOT
  activate for: authoring standalone Airflow DAGs unrelated to deployment, querying data,
  exploring catalog, or managing standalone Glue jobs.
---

# Generate Bundle Manifest

Generate an SMUS CI/CD Application Deployment Manifest (`manifest.yaml`) and, when needed, an MWAA orchestration workflow, to replicate the current SageMaker Unified Studio project across stages (dev, test, prod). The manifest declares WHAT to deploy and WHERE.

This skill is READ-ONLY: it discovers project resources and presents generated content in the chat. It does not write or modify anything. The user saves and edits the output.

## Grounding Rules

- READ-ONLY: You MUST NOT write, upload, create, or modify any files anywhere. You only read (discovery) and present generated content in the chat; the user saves and edits it.
- Auto-discover and generate defaults ONLY when the user gives no configuration. If the user specifies resources, stages, or domain/project details, use exactly what they provide and do not add auto-discovered extras.
- You MUST use `{proj.connection.<name>.<property>}` substitution in generated workflow YAMLs. You MUST NOT hardcode S3 bucket names, IAM role ARNs, or regions. See the substitutions doc (link in References).
- All projects referenced in the manifest MUST be pre-created by the user in each target environment before deploy. There is no `create` field; the CLI does not create projects.
- You MUST include `workflow.create` in bootstrap actions for any deploy-target stage that has workflows.
- You MUST follow the manifest schema (field names, types, required properties) as the source of truth — see the manifest schema doc linked in References.
- You MUST validate that every `connectionName` in `content.storage`, `content.workflows`, and `deployment_configuration.storage` exists in the discovered connections.
- You MUST add this exact exclude list to EVERY storage content item: `[".ipynb_checkpoints/", "__pycache__/", "*.pyc", ".libs.json"]`. When a storage item's `include` pattern would match the manifest itself (any `include` containing `*`, `*.yaml`, or the manifest's directory), you MUST also add `manifest.yaml` to that item's exclude list. Always append such extra excludes in addition to — never instead of — the four required patterns.
- You MUST write `targetDirectory` values WITHOUT a trailing slash (`jobs`, not `jobs/`); the CLI appends the separator, so a trailing slash yields a malformed `//` path. Use `.` for the connection root.
- Include a notebooks storage item when `.ipynb` files are discovered in the `notebooks/` folder (same as how Glue scripts are included when `.py` files are found). These are user-created notebooks used with `SageMakerNotebookOperator` or JupyterLab. If no `.ipynb` files are found, omit the notebooks storage item.
- Catalog is OPT-IN. You MUST NOT set `content.catalog.enabled: true` unless the user explicitly asks. Catalog import needs `datazone:AddPolicyGrant` and ABORTS the whole deploy if the role lacks it. When you omit it, tell the user it can be enabled if they have DataZone catalog admin permissions.
- You SHOULD use `append: true` for workflow storage items and omit `append` for source code.
- You MUST set `applicationName` to match `^[A-Za-z0-9][A-Za-z0-9_-]*$` (max 50 chars). Derive it deterministically:
  1. Strip a leading environment prefix if present: `dev-`, `test-`, `prod-`, `dev_`, `test_`, `prod_`.
  2. Split the remaining name on `-` and `_`, PascalCase each segment, and join them.
  3. If the joined result does NOT already end with one of these suffixes — `Pipeline`, `Bundle`, `App`, `Application`, `Workflow` — append `Pipeline`.
  Worked examples: `dev-marketing` → `MarketingPipeline`; `test_sales_app` → `SalesApp`; `analytics-bundle` → `AnalyticsBundle`; `prod-fraud-detection` → `FraudDetectionPipeline`.
- You MUST use `${VAR_NAME:default}` for stage-specific infrastructure values (regions, project names) so the manifest works with CI/CD env vars. Pre-fill real values only for the dev stage from discovery.
- Scope IAM roles referenced in the manifest to specific resource ARNs, never `*`. Recommend encryption at rest (SSE-KMS) and in transit for target buckets, and CloudTrail/S3 audit logging, when the manifest references IAM roles or target buckets.

## Workflow

### Step 1: Discover Project Resources (read-only)

Skip if the user already specified exactly what to include. Use your agent's AWS access (e.g. the AWS CLI or an equivalent capability) to discover, scoping every call to the project's domain and project. Obtain the `domainId`/`projectId` from the user, or resolve them by listing (`aws datazone list-domains`, `aws datazone list-projects`).

1. **Connections** — `aws datazone list-connections --domain-identifier <id> --project-identifier <id>`. Reveals S3, Glue, Athena, MWAA, MLflow, Redshift, EMR. See the connections doc linked in References.
2. **Storage structure** — `aws s3 ls` (or `list-objects-v2` with delimiter `/`) on the shared S3 connection to find prefixes/file types: `src/` (`.py`), `workflows/` (`.yaml`), `data/`, `models/`, `notebooks/` (`.ipynb`).
3. **MWAA Serverless workflows** — list workflows and post-filter by project tag `AmazonDataZoneProject`.

Not discoverable this way (manifest-only): git repos, environment variables, IAM role policies, EventBridge events, workflow schedules (a DAG YAML field), QuickSight dashboards.

### Step 2: Generate the Manifest

When the user gives no specifics, generate with these defaults:

- `applicationName` — derived per the deterministic rule above.
- Three stages (dev, test, prod). The **dev stage IS the current project**: fill in its real domain (ID or name), region, and project name from discovery — no `${VAR}` placeholders and NO bootstrap actions (dev is only the bundle source; deploy never runs on it). Test and prod use `${VAR}` placeholders and get bootstrap actions.
- Include all discovered storage content and workflows (add a notebooks storage item only if `.ipynb` files were discovered). Do NOT enable catalog unless asked.
- If Glue scripts exist without a referencing workflow, generate one orchestration workflow (Step 5).

When the user specifies what they want, include ONLY that.

Then report: what was discovered/included, stages configured, which `${VAR}` placeholders need filling, and the reminder that target projects must be pre-created.

### Step 3: Map Resources to `content`

| Discovered | Maps to |
|---|---|
| `.py` scripts in `src/` (or `src/glue-jobs/`, `jobs/`) | `content.storage` `name: code` |
| `.ipynb` files in `notebooks/` | `content.storage` `name: notebooks` (include `notebooks/`) |
| `.yaml` DAGs in `workflows/` | `content.storage` `name: workflows`, `append: true` |
| Data in `data/` | `content.storage` `name: data` |
| Model artifacts in `models/` | `content.storage` `name: models` |
| Each discovered/generated workflow | one `content.workflows[]` entry |
| Catalog (only if user opts in) | `content.catalog.enabled: true` |
| QuickSight / Git (user-specified) | `content.quicksight[]` / `content.git[]` |

`.ipynb` files co-located with a DAG in `workflows/` run via `SageMakerNotebookOperator` inside that DAG — keep them in the workflows storage item. Standalone `.ipynb` files (no DAG) are just replicated, no workflow needed. For SageMaker script-mode code (`sagemaker_submit_directory`), set `compression: tar.gz` on the deployment_configuration storage item.

### Step 4: Deployment Configuration per Stage

For each stage set: `domain` (dev: real ID/name; test/prod: `${STAGE_DOMAIN_NAME}`/`${STAGE_DOMAIN_ID}`), `region` (dev: real; test/prod: `${STAGE_DOMAIN_REGION:us-east-1}`), `project.name` (dev: real; test/prod: `${STAGE_PROJECT_NAME:default}`), `deployment_configuration.storage` (map each content item to a `targetDirectory`, no trailing slash), and `environment_variables`.

`bootstrap.actions` — test/prod only, never dev:
- Add ONE `workflow.create` (no `workflowName`) — it creates ALL workflows listed in `content.workflows[]`. Do not list names again or add one action per workflow.
- Add `workflow.run` ONLY for a generated orchestration workflow (its Glue jobs are created by running it). Pre-existing replicated workflows get created but not run.

See the bootstrap actions doc (linked in References) for the full action catalog and ordering.

#### Workflow declaration (two entries, not duplication)

Each workflow needs (1) inclusion in the `workflows` storage item so its DAG YAML deploys to S3, and (2) a `content.workflows[]` entry so `workflow.create` creates it. Missing (2) means the file lands in S3 but no workflow is created.

**Example:** 2 existing workflows + 2 orphaned Glue scripts → register 3 workflows (2 existing + 1 generated `glue_orchestration` with a `GlueJobOperator` task per script); one `workflow.create` makes all 3; one `workflow.run` runs only `glue_orchestration`.

```yaml
content:
  workflows:
    - workflowName: workflow_a
      connectionName: default.workflow_serverless
    - workflowName: workflow_b
      connectionName: default.workflow_serverless
    - workflowName: glue_orchestration
      connectionName: default.workflow_serverless
stages:
  test:
    bootstrap:
      actions:
        - type: workflow.create
        - type: workflow.run
          workflowName: glue_orchestration
          trailLogs: true
```

### Step 5: Generate Orchestration Workflow (when required)

Before generating, check whether each Glue script is already referenced: read each discovered DAG and inspect every `GlueJobOperator.script_location`; a match (by file name or S3 path) means that script is covered.

- If every Glue script is already referenced → do NOT generate a workflow; just include the scripts in a storage item.
- Otherwise → generate exactly ONE orchestration workflow containing a `GlueJobOperator` task for each UNREFERENCED script (never one workflow per script), register it in `content.workflows[]`, and add a `workflow.run` action.

Generate an orchestration workflow when: (a) unreferenced Glue/VETL scripts exist, (b) the user explicitly asks for a post-deployment workflow, or (c) multiple `.ipynb` files exist in S3 that the user has indicated must run in a defined order.

Glue job replication: `GlueJobOperator` with `update_config: true` and `create_job_kwargs` creates-or-updates AND runs the job in one operation (`script_location` points to the deployed S3 path) — there is no separate create-job call, which is why generated Glue workflows MUST include `workflow.run`.

When you generate a workflow, produce the actual DAG YAML in the response (do not just offer to). Default to MWAA Serverless YAML unless the user asks for provisioned (Python DAG). Use `{proj.connection.default.s3_shared.s3Uri}`, `{proj.iam_role_name}`, `{domain.region}` for substitution. For operator names, parameters, and DAG-authoring rules, consult:
- The AWS operators/sensors available in SMUS workflows — https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md
- MWAA Serverless user guide — https://docs.aws.amazon.com/mwaa/latest/mwaa-serverless-userguide/what-is-mwaa-serverless.html

Orchestration-workflow correctness is best-effort; the essential deliverable is a clean, valid manifest.

### Step 6: Present Output (no writes)

Present `manifest.yaml` in a fenced YAML block, and any orchestration workflow in a separate block. Validate structure against the manifest schema doc (linked in References). Tell the user to save `manifest.yaml` to their working directory and the workflow under `workflows/`, and to review the `${VAR}` placeholders and defaults (project names, regions, target directories, schedules). Then present the CLI commands:

```bash
aws-smus-cicd-cli describe --manifest manifest.yaml --connect
aws-smus-cicd-cli bundle --manifest manifest.yaml --targets dev --output-dir ./bundles
aws-smus-cicd-cli deploy --manifest manifest.yaml --targets test --bundle-archive-path ./bundles/<archive>.zip
aws-smus-cicd-cli deploy --manifest manifest.yaml --targets prod --bundle-archive-path ./bundles/<archive>.zip
```

Remind the user that target projects (test, prod) must already exist in SMUS.

## References

Bundled with this skill:
- references/example-manifests.md — complete working manifests (DataOps, MLOps, analytics, minimal)

In the SMUS CI/CD repo docs (authoritative source of truth):
- Manifest fields and schema — https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest.md and https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest-schema.md
- `${VAR}` and `{proj.*}` substitution syntax — https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/substitutions-and-variables.md
- Bootstrap action catalog and ordering — https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/bootstrap-actions.md
- Connection types — https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/connections.md
- CLI reference — https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md
- Airflow AWS operators for orchestration workflows — https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md
- GitHub Actions CI/CD integration — https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/github-actions-integration.md
- Full docs: https://github.com/aws/CICD-for-SageMakerUnifiedStudio

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| Missing `workflow.create` in bootstrap | Add it before `workflow.run` |
| Hardcoded S3 paths in workflows | Use `{proj.connection.default.s3_shared.s3Uri}` |
| Missing `append: true` on workflows storage | Set it to avoid overwriting DAGs |
| Incomplete `exclude` list | Include all four temp-file patterns on every storage item |
| Trailing slash on `targetDirectory` | Remove it; use `.` for root |
| `workflow.run` on the dev stage | Dev gets no bootstrap actions |
| Enabling catalog by default | Opt-in only (needs `datazone:AddPolicyGrant`) |

## Troubleshooting

| Error | Fix |
|-------|-----|
| `ValidationError: applicationName` | Alphanumeric/hyphen/underscore only, max 50 |
| `Connection not found` | Verify with `describe --connect` or add a bootstrap connection |
| `Bundle is empty` | Check include patterns match actual paths |
| `Project not found` | Pre-create the target project in SMUS |
| `Workflow creation failed` | Add `datazone.create_connection` for the workflow engine before `workflow.create` |
| `Variable unresolved` | Check the substitution variable exists in project properties |
| Catalog import `AccessDenied` on `AddPolicyGrant` | Disable catalog, or grant the user DataZone catalog admin permissions |
