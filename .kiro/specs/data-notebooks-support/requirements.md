# Requirements Document

## Introduction

This feature adds native Data Notebooks support to the SMUS CI/CD package, enabling promotion of SageMaker Unified Studio notebooks across environments (dev → test → prod) using the DataZone Notebook APIs. The export process discovers and downloads notebooks from a source project via `GetNotebook`, `StartNotebookExport`, and `GetNotebookExport`. When specific notebook IDs are configured in the manifest, each ID is validated directly via `GetNotebook`; when no IDs are specified, `ListNotebooks` with pagination retrieves all active notebooks. During deployment, the `StartNotebookSync` API handles both creation and in-place update of notebooks in the target project: when called without a `notebookId` it creates a new notebook, and when called with a `notebookId` it updates the existing notebook's `.ipynb` content in-place, preserving run history. After syncing, `UpdateNotebook` applies metadata including a special tracking key (`smus-cicd-source-notebook-id`) that maps target notebooks back to their source. On subsequent deploys, the tool discovers existing target notebooks via `ListNotebooks` + `GetNotebook`, checks metadata for the source ID marker, and passes the matched target `notebookId` to `StartNotebookSync` for an in-place update — or omits it to create a new notebook if no match is found.

Key benefits of this architecture: run history is preserved across deployments (no delete + recreate), the flow is simpler (StartNotebookSync handles both create and update), and source-to-target mapping is maintained via metadata. All operations reference notebooks by their unique IDs, eliminating ambiguity from name-based matching.

**Out of Scope:** Notebook schedules are explicitly out of scope for this iteration. No public API for schedule management currently exists; this will be revisited with the team in a future iteration.

## Glossary

- **CLI**: The `aws-smus-cicd-cli` command-line interface for SMUS CI/CD operations
- **Bundle_Command**: The CLI command that packages application content into a deployable ZIP archive
- **Deploy_Command**: The CLI command that deploys a bundle archive to a target stage's DataZone project
- **Notebook_Exporter**: The component responsible for resolving notebook IDs (either from the manifest list or via `ListNotebooks`), exporting them via `StartNotebookExport`, polling via `GetNotebookExport`, and downloading the exported `.ipynb` files
- **Notebook_Importer**: The component responsible for uploading `.ipynb` files to S3, discovering existing target notebooks via metadata lookup, performing sync operations via `StartNotebookSync` (create or update in-place), and applying metadata via `UpdateNotebook`
- **DataZone_Domain**: An Amazon DataZone domain that contains projects and notebook resources
- **DataZone_Project**: A project within a DataZone domain that owns notebook resources
- **SMUS_Notebook**: A notebook resource natively managed by SageMaker Unified Studio, identified by a notebook ID and owned by a project
- **Notebook_Export**: An asynchronous operation initiated by `StartNotebookExport` that exports a notebook to a specified file format and stores the output in S3
- **StartNotebookSync_API**: The DataZone API that creates or updates a notebook's content from an S3 source. Without `notebookId` it creates a new notebook; with `notebookId` it updates existing content in-place, preserving run history
- **Notebook_Export_Manifest**: A JSON metadata file included in the bundle that records the mapping between notebook names, IDs, and exported file paths
- **Source_Notebook_Metadata_Key**: The metadata key `smus-cicd-source-notebook-id` stored in the target notebook's metadata field to track which source notebook it originated from
- **ListNotebooks_API**: The DataZone API that lists notebooks owned by a project, supporting pagination, status filtering, and sorting
- **GetNotebook_API**: The DataZone API that retrieves the full details of a notebook resource including its parameters, metadata, and environmentConfiguration fields
- **StartNotebookExport_API**: The DataZone API that initiates an asynchronous notebook export to PDF or IPYNB format
- **GetNotebookExport_API**: The DataZone API that retrieves the status and output location of a notebook export operation
- **UpdateNotebook_API**: The DataZone API that updates a notebook resource's mutable fields including name, description, parameters, metadata, and environmentConfiguration. Note: `cellOrder` is handled by StartNotebookSync via the .ipynb file content, and `status` is not ported (target notebooks are always ACTIVE)
- **DeleteNotebook_API**: The DataZone API that deletes a notebook resource from a project, used during destroy operations
- **Manifest**: The `manifest.yaml` file that defines application content, stages, and deployment configuration
- **S3_Connection**: An S3 connection associated with a DataZone project, used for storing exported notebook files
- **Output_Location**: The S3 URI where an exported notebook file is stored after a successful export operation

## Requirements

### Requirement 1: Manifest Configuration for Notebook Export

**User Story:** As a developer, I want to configure notebook export in my manifest.yaml, so that the bundle command knows which notebooks to export from my SMUS project.

#### Acceptance Criteria

1. THE Manifest SHALL support a `content.notebooks` section to configure notebook export
2. THE `content.notebooks` section SHALL support an `enabled` boolean field to enable or disable notebook export (default: false)
3. THE `content.notebooks` section SHALL support an optional `notebook_ids` field containing a list of notebook ID strings to export, where each entry matches the pattern `[a-zA-Z0-9_-]{1,36}`
4. IF `content.notebooks.enabled` is true and `notebook_ids` is specified, THEN THE Bundle_Command SHALL validate all listed IDs upfront by calling the GetNotebook_API for each ID before starting any export operations
5. IF any notebook ID in the `notebook_ids` list does not exist (GetNotebook_API returns ResourceNotFoundException), THEN THE Bundle_Command SHALL immediately fail with a non-zero exit code and an error message listing the invalid notebook ID — no exports shall proceed
6. IF all notebook IDs in the `notebook_ids` list are validated successfully, THEN THE Bundle_Command SHALL proceed to export only those notebooks
7. IF `content.notebooks.enabled` is true and `notebook_ids` is omitted, THEN THE Bundle_Command SHALL call the ListNotebooks_API to discover and export all active notebooks owned by the source project
8. IF the `notebook_ids` field is present but contains an empty list, THEN THE Bundle_Command SHALL raise a validation error indicating that the list must contain at least one entry
9. IF `content.notebooks.enabled` is false or absent, THEN THE Bundle_Command SHALL skip notebook export regardless of whether a `notebook_ids` field is present

### Requirement 2: Export Notebooks During Bundle

**User Story:** As a developer, I want the bundle command to export SMUS notebooks from my source project, so that I can promote notebook resources across stages.

#### Acceptance Criteria

1. WHEN the bundle command runs and `content.notebooks.enabled` is true and `notebook_ids` is specified, THE Notebook_Exporter SHALL first validate all IDs by calling the GetNotebook_API with `domainIdentifier` and `notebookIdentifier` for each ID in the list to retrieve the notebook's `parameters`, `metadata`, and `environmentConfiguration` fields
2. IF any ID validation fails with ResourceNotFoundException during the upfront validation pass, THEN THE Bundle_Command SHALL immediately fail with a non-zero exit code and an error message listing all invalid IDs — no exports shall be initiated
3. WHEN the bundle command runs and `content.notebooks.enabled` is true and `notebook_ids` is omitted, THE Notebook_Exporter SHALL call the ListNotebooks_API to discover all active notebooks owned by the source project, using the `domainIdentifier`, `owningProjectIdentifier` parameter, and filtering by `status=ACTIVE`
4. THE Notebook_Exporter SHALL handle pagination by following `nextToken` until all notebooks are retrieved from the ListNotebooks_API (when listing all notebooks)
5. WHEN a notebook is selected for export, THE Notebook_Exporter SHALL call the StartNotebookExport_API with `domainIdentifier`, `fileFormat` set to `IPYNB`, the notebook's identifier as `notebookIdentifier`, and the source project identifier as `owningProjectIdentifier`
6. WHEN the StartNotebookExport_API returns an export identifier, THE Notebook_Exporter SHALL poll the GetNotebookExport_API using exponential backoff starting at 2 seconds and capped at 30 seconds per interval, until the export status transitions from `IN_PROGRESS` to `SUCCEEDED` or `FAILED`
7. WHEN the export status is `SUCCEEDED`, THE Notebook_Exporter SHALL download the exported `.ipynb` file from the `outputLocation` S3 URI returned by GetNotebookExport_API
8. IF the export status is `FAILED`, THEN THE Notebook_Exporter SHALL log the error message from the `error` field, count the notebook as failed, and continue with the next notebook
9. THE Notebook_Exporter SHALL store each exported `.ipynb` file in a `notebooks/` directory within the bundle archive, using the notebook's source identifier as the filename (e.g., `notebooks/{sourceNotebookId}.ipynb`)
10. THE Notebook_Exporter SHALL produce a `notebooks/notebook_export_manifest.json` metadata file in the bundle containing the list of exported notebooks with their source IDs, names, descriptions, file paths within the bundle, and the `parameters`, `metadata`, and `environmentConfiguration` fields retrieved from the GetNotebook_API
11. THE Notebook_Exporter SHALL implement a configurable polling timeout with a default of 300 seconds per notebook to avoid indefinite waiting on stuck export operations
12. IF the polling timeout is exceeded for a notebook export, THEN THE Notebook_Exporter SHALL log a warning including the notebook ID and elapsed time, count the notebook as failed, and continue with the next notebook
13. WHEN all notebooks have been processed, IF any notebooks failed to export, THEN THE Bundle_Command SHALL exit with a non-zero exit code and output a failure message listing the IDs of all notebooks that failed and their respective error messages

### Requirement 3: Notebook Export Manifest Serialization

**User Story:** As a developer, I want the exported notebook metadata to be stored in a structured manifest file, so that the sync process has the information needed to create or update notebooks in the target project.

#### Acceptance Criteria

1. THE Notebook_Exporter SHALL produce a UTF-8 encoded JSON file at `notebooks/notebook_export_manifest.json` within the bundle archive
2. THE Notebook_Export_Manifest SHALL contain a top-level object with exactly two keys: `metadata` (object) and `notebooks` (array)
3. THE `metadata` section SHALL include `sourceProjectId` (string), `sourceDomainId` (string), `exportTimestamp` (string in ISO 8601 format, e.g. `2024-01-15T10:30:00Z`), and `notebookCount` (integer equal to the length of the `notebooks` array)
4. EACH entry in the `notebooks` array SHALL include `sourceNotebookId` (string), `name` (string), `description` (string, empty string if the source notebook has no description), `filePath` (string, relative path within the bundle pointing to an existing `.ipynb` file), `exportedAt` (string in ISO 8601 format), `parameters` (object, string-to-string map with up to 50 entries where keys are max 128 characters and values are max 1024 characters, empty object if the source notebook has no parameters), `metadata` (object, string-to-string map with up to 50 entries where keys are max 128 characters and values are max 1024 characters, empty object if the source notebook has no metadata), and `environmentConfiguration` (object containing `imageVersion` string, and `packageConfig` object with `packageManager` string and `packageSpecification` string; null if the source notebook has no environment configuration)
5. THE Notebook_Export_Manifest SHALL store ALL fields needed for the UpdateNotebook_API call on the target: `name`, `description`, `parameters`, `metadata`, and `environmentConfiguration` — these are the fields that will be applied to the target notebook via UpdateNotebook after sync
6. THE Notebook_Export_Manifest SHALL be valid JSON parseable by a standard JSON parser, and every `filePath` entry SHALL correspond to a file present in the bundle archive
7. IF the source project has no active notebooks (when listing all) or all specified notebook IDs failed validation, THEN THE Notebook_Exporter SHALL produce a Notebook_Export_Manifest with an empty `notebooks` array and `notebookCount` set to 0

### Requirement 4: Sync Notebooks During Deploy

**User Story:** As a developer, I want the deploy command to sync notebooks into the target SMUS project using StartNotebookSync, so that notebooks are created or updated in-place (preserving run history) in the target environment.

#### Acceptance Criteria

1. WHEN the deploy command processes a bundle containing a `notebooks/notebook_export_manifest.json` file, THE Deploy_Command SHALL invoke the Notebook_Importer after storage deployments
2. THE Notebook_Importer SHALL execute the following deployment sequence in strict order:
   - Step 1: Upload all `.ipynb` files referenced in the Notebook_Export_Manifest to the target project's `default.s3_shared` connection S3 URI under a `notebooks/imports/` prefix
   - Step 2: Call ListNotebooks_API on the target project with `owningProjectIdentifier` and `status=ACTIVE` filter (handling pagination via `nextToken`), then call GetNotebook_API for EACH discovered notebook to read its metadata
   - Step 3: Build a source-to-target mapping: for each target notebook whose metadata contains the key `smus-cicd-source-notebook-id`, record the mapping `{sourceNotebookId → targetNotebookId}`
   - Step 4: For each notebook in the Notebook_Export_Manifest whose file was successfully uploaded, perform the sync operation (see AC#3 through AC#7)
   - Step 5: After each successful StartNotebookSync, call UpdateNotebook_API to apply `name`, `description`, `metadata` (including `smus-cicd-source-notebook-id`), `parameters`, and `environmentConfiguration` from the manifest
   - Step 6: Report sync summary with counts of created, updated, and failed notebooks
3. FOR EACH notebook in the manifest, THE Notebook_Importer SHALL look up the manifest entry's `sourceNotebookId` in the source-to-target mapping built in Step 3
4. IF a matching target notebook is found in the mapping, THEN THE Notebook_Importer SHALL call StartNotebookSync_API WITH the matched target `notebookId` (update in-place, preserving run history)
5. IF no matching target notebook is found in the mapping, THEN THE Notebook_Importer SHALL call StartNotebookSync_API WITHOUT a `notebookId` (create new notebook)
6. IF StartNotebookSync_API called WITH a `notebookId` returns a ResourceNotFoundException, THEN THE Notebook_Importer SHALL log a warning indicating the target notebook was manually deleted, and retry the call WITHOUT `notebookId` (create new notebook instead)
7. THE Notebook_Importer SHALL call the StartNotebookSync_API with `domainIdentifier`, `owningProjectIdentifier`, `sourceLocation` (the uploaded S3 URI), and optionally `notebookId`, `name`, `description`, and a `clientToken` derived from the source notebook ID and deployment timestamp (truncated to a maximum of 64 characters) to ensure idempotent operations
8. WHEN StartNotebookSync_API succeeds and returns a notebook identifier, THE Notebook_Importer SHALL call the UpdateNotebook_API with the target project's `domainIdentifier`, the synced notebook's identifier, `owningProjectIdentifier`, and the `name`, `description`, `parameters`, `metadata` (including `smus-cicd-source-notebook-id` set to the manifest entry's `sourceNotebookId`), and `environmentConfiguration` values from the manifest
9. IF the UpdateNotebook_API returns any error, THEN THE Notebook_Importer SHALL log the notebook ID and error details, and count the notebook as FAILED (the notebook is not in its intended final state)
10. IF the StartNotebookSync_API returns any error other than ResourceNotFoundException, THEN THE Notebook_Importer SHALL log the error with notebook ID and error details, count the notebook as failed, and continue with the next notebook
11. WHEN a notebook entry in the Notebook_Export_Manifest references a `filePath` that does not exist in the bundle archive, THE Notebook_Importer SHALL log an error identifying the missing file and notebook ID, count it as failed, and continue with the next notebook
12. WHEN all notebooks in the Notebook_Export_Manifest have been processed, THE Notebook_Importer SHALL output to stdout the counts of created (new, no previous target notebook existed), updated (synced to an existing target notebook), and failed notebooks as the sync summary
13. WHEN all notebooks in the Notebook_Export_Manifest have been processed, IF any notebooks failed to sync or update, THEN THE Deploy_Command SHALL exit with a non-zero exit code and output a failure message listing the IDs of all notebooks that failed and their respective error messages

### Requirement 5: Deploy Command Integration

**User Story:** As a developer, I want notebook sync to be integrated into the existing deploy command flow, so that notebooks are deployed alongside other bundle content.

#### Acceptance Criteria

1. THE Deploy_Command SHALL process notebook sync after storage and catalog deployments, and before bootstrap actions
2. WHERE the stage's `deployment_configuration.notebooks.disable` is set to true, THE Deploy_Command SHALL skip notebook sync for that stage and log an informational message indicating notebook sync was skipped due to configuration
3. IF notebook sync is not disabled and the bundle contains a `notebooks/notebook_export_manifest.json`, THEN THE Deploy_Command SHALL invoke the Notebook_Importer to process the notebooks listed in the manifest
4. IF the bundle does not contain a `notebooks/notebook_export_manifest.json`, THEN THE Deploy_Command SHALL skip notebook sync without producing any warning or error output
5. WHEN the Notebook_Importer completes processing, THE Deploy_Command SHALL report the notebook sync summary (created count, updated count, failed count) in the overall deployment output

### Requirement 6: S3 Upload for Notebook Sync

**User Story:** As a developer, I want exported notebook files to be uploaded to an accessible S3 location before sync, so that the StartNotebookSync API can read them.

#### Acceptance Criteria

1. THE Notebook_Importer SHALL upload `.ipynb` files to the target project's `default.s3_shared` connection S3 URI under a `notebooks/imports/` prefix, constructing the full S3 key as `{s3Uri}/notebooks/imports/{sourceNotebookId}.ipynb`
2. WHEN uploading notebook files, THE Notebook_Importer SHALL verify that the target project's `default.s3_shared` connection exists and contains a non-empty `s3Uri` value before attempting any uploads
3. IF the S3 upload fails for a notebook file, THEN THE Notebook_Importer SHALL log the error including the notebook ID and S3 destination key, skip the sync for that notebook, and continue processing remaining notebooks
4. IF the target project's `default.s3_shared` connection does not exist or does not contain an `s3Uri` value, THEN THE Notebook_Importer SHALL raise an error indicating the missing connection and skip all notebook sync operations for that project

### Requirement 7: Error Handling and Resilience

**User Story:** As a developer, I want robust error handling during notebook export and sync, so that partial failures do not block the entire deployment.

#### Acceptance Criteria

1. IF the ListNotebooks_API returns an error during export (when listing all notebooks), THEN THE Notebook_Exporter SHALL raise an exception with an error message including the domain identifier, project identifier, and the error response from the API
2. IF the source project has no active notebooks (when listing all) or all specified notebook IDs are not found, THEN THE Notebook_Exporter SHALL produce an empty Notebook_Export_Manifest with zero notebooks and log an informational message indicating the project identifier and any IDs that were not found
3. IF a StartNotebookExport_API call fails for a specific notebook during export, THEN THE Notebook_Exporter SHALL log the notebook ID and error, then continue with the next notebook
4. IF the Notebook_Export_Manifest is missing the top-level `metadata` or `notebooks` keys, or if `metadata` is missing any of `sourceProjectId`, `sourceDomainId`, `exportTimestamp`, or `notebookCount`, THEN THE Notebook_Importer SHALL raise a validation error before attempting any API calls
5. IF any notebook syncs or UpdateNotebook calls fail, THEN THE Deploy_Command SHALL fail the overall deployment with a non-zero exit code and output a failure message listing the IDs of all notebooks that failed and their respective error messages
6. THE Notebook_Exporter SHALL implement exponential backoff with jitter when polling GetNotebookExport_API, starting at an initial interval of 1 second, doubling on each poll, up to a maximum interval of 30 seconds per poll
7. IF a ThrottlingException is received from any DataZone API call, THEN THE component SHALL retry the request with exponential backoff starting at 1 second and doubling on each retry, up to a maximum of 3 retries
8. IF StartNotebookSync_API with a `notebookId` returns a ResourceNotFoundException, THEN THE Notebook_Importer SHALL log a warning indicating the notebook may have been manually deleted in the target, and retry without `notebookId` to create a new notebook

### Requirement 8: Dry Run Validation for Notebooks

**User Story:** As a developer, I want the deploy dry-run to validate notebook sync prerequisites, so that I can detect issues before actual deployment.

#### Acceptance Criteria

1. WHEN the deploy command runs with `--dry-run` and the bundle contains a `notebooks/notebook_export_manifest.json` file, THE Dry_Run_Engine SHALL verify that the target project's `default.s3_shared` connection exists and is reachable by performing a HEAD request against the S3 bucket resolved from the connection
2. WHEN the deploy command runs with `--dry-run` and the bundle contains a `notebooks/notebook_export_manifest.json` file, THE Dry_Run_Engine SHALL verify that the IAM identity has the `datazone:StartNotebookSync`, `datazone:UpdateNotebook`, `datazone:GetNotebook`, `datazone:ListNotebooks`, and `s3:PutObject` permissions using `iam:SimulatePrincipalPolicy`
3. WHEN the deploy command runs with `--dry-run` and the bundle contains a `notebooks/notebook_export_manifest.json` file, THE Dry_Run_Engine SHALL report the number of notebooks that would be synced (read from the manifest's `notebookCount` field) in the dry-run output
4. IF the S3 connection is not found or the HEAD request to the resolved S3 bucket fails, THEN THE Dry_Run_Engine SHALL report a WARNING finding indicating the connection name and the failure reason
5. IF `iam:SimulatePrincipalPolicy` reports a denied decision for `datazone:StartNotebookSync`, `datazone:UpdateNotebook`, `datazone:GetNotebook`, `datazone:ListNotebooks`, or `s3:PutObject`, THEN THE Dry_Run_Engine SHALL report a WARNING finding indicating the denied permission name and resource ARN

### Requirement 9: Destroy Command Support for Notebooks

**User Story:** As a developer, I want the destroy command to clean up synced notebooks, so that I can remove all deployed resources from a target environment.

#### Acceptance Criteria

1. WHEN the destroy command validates a target stage and `content.notebooks.enabled` is true in the manifest, THE Destroy_Command SHALL call the ListNotebooks_API with the target project's `owningProjectIdentifier` and `status=ACTIVE` filter to discover notebook resources, handling pagination by following `nextToken` until all notebooks are retrieved
2. FOR EACH discovered target notebook, THE Destroy_Command SHALL call the GetNotebook_API to read its metadata and check whether the metadata contains the key `smus-cicd-source-notebook-id`
3. THE Destroy_Command SHALL include a target notebook in the destruction plan only if its metadata contains the `smus-cicd-source-notebook-id` key (indicating it was deployed by this CI/CD tool)
4. IF `notebook_ids` is specified in the manifest's `content.notebooks` section, THEN THE Destroy_Command SHALL additionally filter the destruction plan to include only target notebooks whose `smus-cicd-source-notebook-id` metadata value is present in the manifest's `notebook_ids` list
5. IF `notebook_ids` is omitted in the manifest's `content.notebooks` section, THEN THE Destroy_Command SHALL include all target notebooks that have the `smus-cicd-source-notebook-id` metadata key in the destruction plan
6. THE Destroy_Command SHALL display each filtered notebook's name and identifier in the destruction plan under a `notebook` resource type for user confirmation before deletion
7. IF the user confirms destruction, THEN THE Destroy_Command SHALL delete each notebook resource in the target project using the DeleteNotebook_API with the DataZone domain identifier and notebook identifier, continuing to the next notebook if an individual deletion fails
8. IF a notebook is not found at deletion time (ResourceNotFoundException), THEN THE Destroy_Command SHALL record the notebook as `not_found` and continue with the next notebook
9. IF a notebook deletion fails with any other error, THEN THE Destroy_Command SHALL log the notebook name and error details, record the notebook as `error`, and continue with the next notebook
10. THE Destroy_Command SHALL report the count of deleted, not_found, and failed notebook deletions in the destruction summary consistent with the existing summary format
11. IF the ListNotebooks_API returns an error during validation, THEN THE Destroy_Command SHALL report the error and fail validation for that stage

**Note on source environment behavior:** The `notebook_ids` in the manifest are SOURCE notebook IDs. The destroy command looks for target notebooks tagged with `smus-cicd-source-notebook-id` matching those source IDs. If the user runs destroy on the SOURCE environment (where notebooks do not have this metadata tag), no notebooks will match and the command will report zero deletions. This is expected behavior — destroy is designed for TARGET environments where notebooks were deployed by this tool. Running destroy on the source environment is not a supported use-case since the source notebooks are the authoritative versions needed for future deployments.

### Requirement 10: Port Notebook Metadata via UpdateNotebook

**User Story:** As a developer, I want synced notebooks to retain their parameters, metadata, and environment configuration from the source notebook, so that notebooks in the target environment behave identically to those in the source environment.

#### Acceptance Criteria

1. WHEN StartNotebookSync_API succeeds, THE Notebook_Importer SHALL call the UpdateNotebook_API with the target project's `domainIdentifier`, the synced notebook's identifier, `owningProjectIdentifier`, and the `name`, `description`, `parameters`, `metadata` (including `smus-cicd-source-notebook-id` set to the manifest entry's `sourceNotebookId`), and `environmentConfiguration` values from the manifest
2. THE Notebook_Importer SHALL always include `smus-cicd-source-notebook-id` with the value of the manifest entry's `sourceNotebookId` in the metadata passed to UpdateNotebook_API, merging it with any existing metadata from the manifest
3. IF the `parameters` field in the manifest entry is an empty object, THEN THE Notebook_Importer SHALL omit the `parameters` field from the UpdateNotebook_API call
4. IF the `metadata` field in the manifest entry is an empty object, THEN THE Notebook_Importer SHALL still include the `metadata` field in the UpdateNotebook_API call containing only the `smus-cicd-source-notebook-id` key
5. IF the `environmentConfiguration` field in the manifest entry is null, THEN THE Notebook_Importer SHALL omit the `environmentConfiguration` field from the UpdateNotebook_API call
6. IF the UpdateNotebook_API returns any error (including ValidationException), THEN THE Notebook_Importer SHALL log the notebook ID and error details, and count the notebook as FAILED
7. WHEN the Notebook_Importer reports the sync summary, THE Notebook_Importer SHALL report counts of created, updated, and failed notebooks — there is no separate warning category
8. THE UpdateNotebook_API call covers the fields `name`, `description`, `parameters`, `metadata`, and `environmentConfiguration`. The `cellOrder` field (notebook cell structure) is handled by StartNotebookSync via the .ipynb file content. The `status` field is not ported — target notebooks remain ACTIVE

### Requirement 11: Documentation — Source ID-Based Tracking Semantics

**User Story:** As a developer reading the documentation, I want to clearly understand that notebooks are tracked by source ID via metadata, so that I am aware of how deployments create, update, and destroy notebooks in the target environment.

#### Acceptance Criteria

1. THE customer-facing documentation for this feature SHALL prominently state that notebooks are tracked across environments using the `smus-cicd-source-notebook-id` metadata key, and that existing notebooks with matching source IDs in the target project will be updated in-place during deployment
2. THE documentation SHALL include a warning or callout box emphasizing that run history is preserved when updating existing notebooks, and that notebooks without the metadata marker are never modified or deleted by the deploy command
3. THE documentation SHALL explain that the destroy command only removes notebooks that contain the `smus-cicd-source-notebook-id` metadata key, ensuring manually created notebooks in the target are not affected
4. THE documentation SHALL recommend that users do not manually modify the `smus-cicd-source-notebook-id` metadata key in target notebooks, as doing so could disrupt the source-to-target tracking mechanism
5. THE documentation SHALL explain that the optional `notebook_ids` manifest field allows users to selectively export specific notebooks by their ID, and that omitting the field exports all active notebooks from the source project
6. THE documentation SHALL appear in the CLI help text for the deploy command (when notebooks are involved) as a brief reminder that notebook updates preserve run history and are tracked by source ID
