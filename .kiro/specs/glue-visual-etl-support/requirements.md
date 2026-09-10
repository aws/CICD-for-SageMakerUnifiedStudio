# Requirements Document

## Introduction

SageMaker Unified Studio lets data engineers build AWS Glue ETL pipelines with a **visual, drag-and-drop editor** instead of writing Spark code by hand. A job built this way is a **Visual ETL job** (`JobMode: VISUAL` in Glue), and is backed by three files in the project's S3 bucket: the generated PySpark script (`<job>.py`), the visual graph definition (`<job>.vetl`, which lets the UI re-open the drag-and-drop editor), and the job metadata (`<job>.json`).

Teams build and test such jobs in a **development** project and need to promote them to **test** and **production** projects. The SMUS CI/CD CLI has no first-class way to move a Glue job between projects today, so users hand-copy files and hand-write orchestration. This is error-prone, and — critically — a job created without the right Unified Studio metadata is **invisible in the target project's UI** even though it exists in Glue.

This feature adds an end-to-end, declarative way to promote Visual ETL jobs across environments. A new `content.glue_jobs` manifest section declares which jobs to promote, and the CLI handles the full lifecycle:

- **bundle (export)**: read a Visual ETL job from a source project and package its definition + three backing files into a portable bundle
- **deploy**: recreate the job in a target project with a target-scoped configuration and the tags/arguments required for it to appear correctly in the Unified Studio UI
- **destroy**: remove jobs previously deployed by this tool from a target project
- **deploy --dry-run**: validate the prerequisites before making changes

### What makes a deployed job appear correctly in the Unified Studio UI

The Unified Studio UI only shows Glue jobs that are tagged as belonging to a project, and it only renders the visual editor when the job points at its `.vetl` file. Three elements are required, and this feature injects all three automatically during deploy:

1. the `AmazonDataZoneProject` tag set to the target project ID — without it, the job exists in Glue but is **invisible** in the UI
2. the `--smus-orig-asset` argument pointing at the deployed `.vetl` — without it, the job appears but **without its visual graph** (shows as a plain script job)
3. the `--custom-logGroup-prefix` argument scoped to the target project — without it, the job's **logs are not associated** with the project

### Scope

**In scope:**
- The `content.glue_jobs` manifest section and per-stage `deployment_configuration.glue_jobs`
- Exporting `JobMode: VISUAL` jobs during `bundle`
- Deploying them into a target project during `deploy` (create/update, tag injection, argument/role/S3 rewrites)
- Idempotent in-place updates tracked by a source tag, with an ownership guard
- `destroy` support and `deploy --dry-run` validation
- Documentation of behavior and prerequisites

**Out of scope:**
- Script-mode Glue jobs (`JobMode: SCRIPT`) — only `JobMode: VISUAL` in this iteration
- Glue crawlers, triggers, and Glue-native workflows
- Running a job (execution) — this feature deploys job *definitions*
- Automatically granting Glue IAM permissions to the target project role (documented as a prerequisite)

## Glossary

- **CLI**: The `aws-smus-cicd-cli` command-line interface for SMUS CI/CD operations
- **Bundle_Command / Deploy_Command / Destroy_Command**: The CLI commands that package, deploy, and remove content
- **Visual_ETL_Exporter**: The component that reads each configured job via `GetJob`, verifies it is `JobMode: VISUAL`, copies its S3 files, sanitizes the definition, and produces the export manifest
- **Visual_ETL_Importer**: The component that uploads job files to the target S3, rewrites project-scoped fields, injects tags, and creates/updates the job via `CreateJob`/`UpdateJob`
- **Visual_ETL_Job**: A Glue job with `JobMode: VISUAL`, backed by `<job>.py`, `<job>.vetl`, and `<job>.json`
- **Job_Artifact**: One of the backing files: the `.py` script, the `.vetl` visual definition, or the `.json` metadata
- **Glue_Job_Entry**: An element of the `content.glue_jobs` list, with fields `name` (required), `source` (optional source stage), and `targetName` (optional target job base name)
- **DataZone_Project**: A project within a DataZone domain that owns Glue job resources
- **Source_Stage / Target_Stage**: The stage/project a job is exported from, and the stage/project it is deployed to
- **Glue_Jobs_Export_Manifest**: A JSON file in the bundle recording each exported job's name, mode, target name, sanitized definition, and file paths
- **DataZone_Project_Tag**: The tag `AmazonDataZoneProject` (value = project ID) that makes a Glue job visible in the Unified Studio UI
- **Source_Job_Tracking_Tag**: The tag `smus-cicd-source-job-name` written to deployed jobs to track which source job they came from
- **Orig_Asset_Argument**: The `--smus-orig-asset` Glue argument that points to the `.vetl` file and activates the visual graph
- **Log_Group_Prefix_Argument**: The `--custom-logGroup-prefix` Glue argument (pattern `datazone-<projectId>-<stage>`) that associates logs with the project
- **GetJob_API / GetJobs_API / CreateJob_API / UpdateJob_API / DeleteJob_API**: Glue APIs to read, list, create, update, and delete job definitions
- **S3_Connection**: The `default.s3_shared` connection of a project, used to read and write job artifacts
- **Manifest**: The `manifest.yaml` file defining content and stages

## Requirements

### Requirement 1: Manifest Configuration for Glue Jobs

**User Story:** As a data engineer, I want to declare Glue jobs in my manifest.yaml using the `content.glue_jobs` list, so that the CLI knows which Visual ETL jobs to promote and where.

#### Acceptance Criteria

1. THE Manifest SHALL support a `content.glue_jobs` section defined as a list of Glue_Job_Entry objects
2. EACH Glue_Job_Entry SHALL support a required `name` field (the source Glue job name) matching the pattern `[a-zA-Z0-9_-]{1,255}`
3. EACH Glue_Job_Entry SHALL support an optional `source` field naming the source stage to export from; IF omitted, THE Bundle_Command SHALL use the bundle's default source stage (consistent with other content types)
4. EACH Glue_Job_Entry SHALL support an optional `targetName` field giving the base name of the deployed job; IF omitted, THE deployed job's base name SHALL default to the entry's `name`
5. THE Manifest SHALL support a per-stage `deployment_configuration.glue_jobs` section with an optional `disable` boolean (default false), an optional `target_suffix` string appended to each deployed job name, and an optional `overrides` map keyed by source job name whose values are partial job definitions
6. IF the `content.glue_jobs` list is absent or empty, THEN THE Bundle_Command SHALL skip Glue job export and SHALL NOT create a `glue_jobs/` directory in the bundle
7. IF two or more Glue_Job_Entry objects share the same `name` within the same `source`, THEN THE Bundle_Command SHALL raise a validation error identifying the duplicate
8. IF a Glue_Job_Entry has a `name` that does not match the required pattern, THEN THE Bundle_Command SHALL raise a validation error

### Requirement 2: Validate Configured Jobs Before Export

**User Story:** As a data engineer, I want the bundle command to validate every configured job upfront, so that a misconfiguration fails fast before any file is copied.

#### Acceptance Criteria

1. WHEN the bundle command runs and `content.glue_jobs` is non-empty, THE Visual_ETL_Exporter SHALL call the GetJob_API for each entry's `name` (resolved against the entry's source stage project) to retrieve the full job definition, before copying any files
2. IF any configured job name does not exist (GetJob_API returns EntityNotFoundException), THEN THE Bundle_Command SHALL fail with a non-zero exit code and an error listing all missing job names — no files shall be copied
3. IF any configured job exists but its `JobMode` is not `VISUAL`, THEN THE Bundle_Command SHALL fail with a non-zero exit code and an error listing the non-visual job names and stating that only Visual ETL jobs are supported in this iteration — no files shall be copied
4. IF all configured jobs exist and are `JobMode: VISUAL`, THEN THE Visual_ETL_Exporter SHALL proceed to export them

### Requirement 3: Export Visual ETL Jobs During Bundle

**User Story:** As a data engineer, I want the bundle command to export Visual ETL job definitions and their backing files, so that they can be deployed to another project.

#### Acceptance Criteria

1. FOR EACH configured job, THE Visual_ETL_Exporter SHALL determine the job's file locations — the `.py` from `Command.ScriptLocation`, the `.vetl` from the `--smus-orig-asset` argument (resolved relative to the script location when the argument is a relative path), and the `.json` as the script's sibling — and copy each located file from the source project's S3 into a `glue_jobs/<name>/` directory within the bundle archive
2. THE Visual_ETL_Exporter SHALL verify that a `.vetl` file was located and copied for each job; IF the `.vetl` cannot be located or copied, THEN THE Visual_ETL_Exporter SHALL log an error identifying the job, count the job as failed, and continue with the next job
3. THE Visual_ETL_Exporter SHALL produce a `glue_jobs/glue_jobs_export_manifest.json` recording, for each exported job, `sourceJobName`, `sourceStage`, `targetName` (or null), `jobMode`, the sanitized definition (see Requirement 4), and the relative bundle paths of its copied files
4. IF the S3 copy of a file fails for a specific job, THEN THE Visual_ETL_Exporter SHALL log the error including the job name and S3 key, count the job as failed, and continue with the next job
5. WHEN all configured jobs have been processed, IF any jobs failed to export, THEN THE Bundle_Command SHALL exit with a non-zero exit code and output a failure message listing the names of all jobs that failed and their error messages
6. THE `sourceStage` recorded per job SHALL equal the entry's `source` field, or the bundle's default source stage when `source` was omitted

### Requirement 4: Export Manifest Serialization

**User Story:** As a data engineer, I want the exported metadata stored in a structured, portable manifest, so that deploy has everything it needs to recreate the job in a target project.

#### Acceptance Criteria

1. THE Visual_ETL_Exporter SHALL produce a UTF-8 encoded JSON file at `glue_jobs/glue_jobs_export_manifest.json` within the bundle archive
2. THE Glue_Jobs_Export_Manifest SHALL contain a top-level object with exactly two keys: `metadata` (object) and `jobs` (array)
3. THE `metadata` section SHALL include `sourceProjectId`, `sourceDomainId`, `sourceRegion`, `exportTimestamp` (ISO 8601), and `jobCount` (equal to the length of the `jobs` array)
4. EACH entry in the `jobs` array SHALL include `sourceJobName` (string), `sourceStage` (string), `targetName` (string or null), `jobMode` (always `VISUAL`), `definition` (object, the sanitized create-job kwargs — see AC#5 and AC#6), and `artifacts` (object mapping `script`/`visual`/`metadata` to the relative bundle path of an existing file, omitting kinds not present on the source)
5. THE `definition` object SHALL be derived from the source `GetJob` response and SHALL retain: `JobMode`, `GlueVersion`, `WorkerType`, `NumberOfWorkers`, `Timeout`, `MaxRetries`, `ExecutionClass`, `Command` (name and python version, but NOT the source `ScriptLocation`), and `DefaultArguments`
6. THE `definition` object SHALL exclude source-specific fields recomputed at deploy time: `Role`, `Connections`, `Command.ScriptLocation`, and any pre-existing `Tags`. The source values of the `--TempDir`, `--spark-event-logs-path`, `--custom-logGroup-prefix`, and `--smus-orig-asset` arguments SHALL be replaced with a `<TARGET_REWRITE>` placeholder
7. THE Glue_Jobs_Export_Manifest SHALL be valid JSON, and every file path referenced in any job entry SHALL correspond to a file present in the bundle archive
8. THE `definition` object SHALL NOT contain the source AWS account ID, source S3 bucket names, or source region strings in any retained value

### Requirement 5: Deploy Command Integration

**User Story:** As a data engineer, I want Glue job deployment integrated into the deploy command, so that jobs are deployed alongside other bundle content.

#### Acceptance Criteria

1. THE Deploy_Command SHALL process Glue job deployment after storage and catalog deployments, and before bootstrap actions
2. WHERE the stage's `deployment_configuration.glue_jobs.disable` is true, THE Deploy_Command SHALL skip Glue job deployment for that stage and log an informational message
3. IF Glue job deployment is not disabled and the bundle contains a `glue_jobs/glue_jobs_export_manifest.json`, THEN THE Deploy_Command SHALL invoke the Visual_ETL_Importer for the jobs in the manifest
4. IF the bundle does not contain a `glue_jobs/glue_jobs_export_manifest.json`, THEN THE Deploy_Command SHALL skip Glue job deployment without warning or error
5. WHEN the Visual_ETL_Importer completes, THE Deploy_Command SHALL report the deployment summary (created, updated, failed counts) in the deployment output

### Requirement 6: Deploy Visual ETL Jobs into the Target Project

**User Story:** As a data engineer, I want deployed jobs to run correctly and appear in the Unified Studio UI, so that promoted jobs behave identically to the source.

#### Acceptance Criteria

1. THE Visual_ETL_Importer SHALL execute, for each job in the manifest, this ordered sequence: (Step 1) upload the job's files to the target project's `default.s3_shared` S3 URI under `glue_jobs/<deployedName>/`; (Step 2) build target create/update kwargs with all rewrites and tags; (Step 3) create or update the job (see Requirement 7); (Step 4) record the result
2. THE Visual_ETL_Importer SHALL set `Command.ScriptLocation` to the uploaded `.py` file's target S3 URI
3. THE Visual_ETL_Importer SHALL set the job's `Role` to the target project's IAM role ARN resolved from the target project context
4. THE Visual_ETL_Importer SHALL rewrite `DefaultArguments` to target-scoped values: `--TempDir` and `--spark-event-logs-path` to the target project's S3 shared paths, `--smus-orig-asset` to the deployed `.vetl` file's location, and `--custom-logGroup-prefix` to `datazone-<targetProjectId>-<stage>`
5. THE Visual_ETL_Importer SHALL set the job `Tags` to include `AmazonDataZoneProject` equal to the target project ID and `smus-cicd-source-job-name` equal to the manifest entry's `sourceJobName`
6. THE Visual_ETL_Importer SHALL preserve `JobMode` (VISUAL), `GlueVersion`, `WorkerType`, `NumberOfWorkers`, `Timeout`, `MaxRetries`, and `ExecutionClass` from the manifest definition, applying any per-job `overrides` from the deployment configuration
7. THE deployed job's name SHALL be `(targetName or sourceJobName) + target_suffix` where `target_suffix` comes from the stage's deployment configuration (default empty)
8. IF CreateJob_API or UpdateJob_API returns an error, THEN THE Visual_ETL_Importer SHALL log the job name and error, count the job as failed, and continue with the next job
9. WHEN a job entry references a file path not present in the bundle, THE Visual_ETL_Importer SHALL log an error identifying the missing file and job, count it as failed, and continue
10. WHEN all jobs have been processed, IF any failed, THEN THE Deploy_Command SHALL exit with a non-zero exit code and list the names of all jobs that failed and their error messages
11. IF an `overrides` entry references a source job name not present in the manifest, THEN THE Deploy_Command SHALL log a warning identifying the unknown name and continue

### Requirement 7: Idempotent In-Place Update with Ownership Guard

**User Story:** As a data engineer, I want re-deployments to update the existing target job rather than create duplicates, and to never clobber jobs the tool doesn't own, so that promotion is safe and idempotent.

#### Acceptance Criteria

1. WHEN deploying a job, THE Visual_ETL_Importer SHALL call GetJob_API on the computed deployed job name to determine whether it already exists
2. IF the target job exists and its `smus-cicd-source-job-name` tag equals the manifest entry's `sourceJobName`, THEN THE Visual_ETL_Importer SHALL update it in place via UpdateJob_API
3. IF the target job exists but does NOT carry a `smus-cicd-source-job-name` tag, THEN THE Visual_ETL_Importer SHALL log a warning that a non-CI/CD-managed job with the same name exists, count the job as failed, and continue — it SHALL NOT overwrite it
4. IF the target job does not exist (EntityNotFoundException), THEN THE Visual_ETL_Importer SHALL create it via CreateJob_API
5. THE Visual_ETL_Importer SHALL make create/update idempotent such that repeated deploys of an unchanged manifest converge to the same target job definition

### Requirement 8: S3 Artifact Upload for Deployment

**User Story:** As a data engineer, I want job files uploaded to the target project S3 before job creation, so that the job's script and visual graph resolve correctly in the target.

#### Acceptance Criteria

1. THE Visual_ETL_Importer SHALL upload files to the target project's `default.s3_shared` S3 URI, constructing keys as `{s3Uri}glue_jobs/<deployedName>/<fileName>`
2. WHEN uploading, THE Visual_ETL_Importer SHALL verify that the target project's `default.s3_shared` connection exists and contains a non-empty `s3Uri` before attempting any uploads
3. IF a file upload fails, THEN THE Visual_ETL_Importer SHALL log the error including the job name and S3 key, count that job as failed, skip its create/update, and continue
4. IF the target project's `default.s3_shared` connection does not exist or has no `s3Uri`, THEN THE Visual_ETL_Importer SHALL raise an error indicating the missing connection and skip all Glue job deployment for that project

### Requirement 9: Destroy Command Support for Glue Jobs

**User Story:** As a data engineer, I want the destroy command to clean up deployed Glue jobs, so that I can remove all deployed resources from a target environment.

#### Acceptance Criteria

1. WHEN the destroy command validates a target stage and `content.glue_jobs` is non-empty, THE Destroy_Command SHALL discover Glue jobs in the target account/region carrying the `AmazonDataZoneProject` tag equal to the target project ID, handling pagination until all are retrieved
2. FOR EACH discovered job, THE Destroy_Command SHALL read its tags and check whether they contain the key `smus-cicd-source-job-name`
3. THE Destroy_Command SHALL include a job in the destruction plan only if its tags contain `smus-cicd-source-job-name` (indicating it was deployed by this tool)
4. THE Destroy_Command SHALL additionally filter the destruction plan to include only jobs whose `smus-cicd-source-job-name` tag value equals the `name` of some entry in `content.glue_jobs`
5. THE Destroy_Command SHALL display each filtered job's name in the destruction plan under a `glue_job` resource type for user confirmation before deletion
6. IF the user confirms destruction, THEN THE Destroy_Command SHALL delete each job via DeleteJob_API, continuing to the next job if an individual deletion fails
7. IF a job is not found at deletion time (EntityNotFoundException), THEN THE Destroy_Command SHALL record the job as `not_found` and continue
8. IF a job deletion fails with any other error, THEN THE Destroy_Command SHALL log the job name and error, record the job as `error`, and continue
9. THE Destroy_Command SHALL report the count of deleted, not_found, and failed deletions in the destruction summary consistent with the existing format
10. THE `glue_job` resource type SHALL be present in both `DEPLOY_RESOURCE_TYPES` (`resource_types.py`) and `DESTROY_SUPPORTED_RESOURCE_TYPES` (`destroy_models.py`), keeping the `TestDeployDestroyDrift` unit test passing

### Requirement 10: Dry-Run Validation for Glue Jobs

**User Story:** As a data engineer, I want `deploy --dry-run` to validate Glue job prerequisites, so that I can detect issues before deploying.

#### Acceptance Criteria

1. WHEN the deploy command runs with `--dry-run` and the bundle contains a `glue_jobs/glue_jobs_export_manifest.json`, THE Dry_Run_Engine SHALL verify that the target project's `default.s3_shared` connection exists and is reachable via a HEAD request against the resolved S3 bucket
2. WHEN the deploy command runs with `--dry-run` and the bundle contains a `glue_jobs/glue_jobs_export_manifest.json`, THE Dry_Run_Engine SHALL verify that the IAM identity has `glue:GetJob`, `glue:CreateJob`, `glue:UpdateJob`, `glue:TagResource`, `glue:GetTags`, and `s3:PutObject` permissions via `iam:SimulatePrincipalPolicy`
3. THE Dry_Run_Engine SHALL report the number of jobs that would be deployed (from the manifest's `jobCount`)
4. IF the S3 connection is not found or the HEAD request fails, THEN THE Dry_Run_Engine SHALL report a WARNING finding indicating the connection name and reason
5. IF `iam:SimulatePrincipalPolicy` reports a denied decision for any required permission, THEN THE Dry_Run_Engine SHALL report a WARNING finding indicating the denied permission and resource
6. IF any manifest job is VISUAL but has no `visual` artifact, THEN THE Dry_Run_Engine SHALL report a WARNING finding indicating the visual graph will be missing in the target UI

### Requirement 11: Documentation — UI Visibility and Prerequisites

**User Story:** As a data engineer reading the documentation, I want to understand what makes a deployed job appear in the Unified Studio UI and what prerequisites exist, so that my promoted jobs are usable.

#### Acceptance Criteria

1. THE documentation SHALL explain that three elements are required for a deployed job to appear correctly in the Unified Studio UI — the `AmazonDataZoneProject` tag (visibility), the `--smus-orig-asset` argument (visual graph), and the `--custom-logGroup-prefix` argument (log association) — and that this feature injects all three automatically
2. THE documentation SHALL state that jobs are tracked across environments via the `smus-cicd-source-job-name` tag, that existing target jobs with a matching tag are updated in place, and that jobs without the tag are never modified or deleted
3. THE documentation SHALL explain that destroy only removes jobs carrying the `smus-cicd-source-job-name` tag, so manually created jobs are not affected
4. THE documentation SHALL document the prerequisite that the target project IAM role must have Glue job management permissions (`glue:CreateJob`, `glue:GetJob`, `glue:UpdateJob`, `glue:StartJobRun`, `glue:GetJobRun`, `glue:GetJobRuns`, `glue:BatchStopJobRun`, `glue:GetSecurityConfigurations`), since the toolkit does not grant these automatically
5. THE documentation SHALL document that on IdC-based domains the CI/CD principal must be a member (owner) of the target project for connection resolution to succeed
6. THE documentation SHALL explain the `content.glue_jobs` schema (`name`, `source`, `targetName`) and the per-stage `deployment_configuration.glue_jobs` options (`disable`, `target_suffix`, `overrides`)
