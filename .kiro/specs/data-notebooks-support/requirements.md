# Requirements Document

## Introduction

This feature adds native Data Notebooks support to the SMUS CI/CD package, enabling promotion of SageMaker Unified Studio notebooks across environments (dev → test → prod) using the DataZone Import/Export Notebook APIs. Unlike the current approach where notebooks are deployed as raw `.ipynb` files to S3 storage connections (and then executed via the SageMakerNotebookOperator in Airflow), this feature uses the DataZone `StartNotebookExport` and `StartNotebookImport` APIs to export notebooks from a source SMUS project and import them as first-class notebook resources into target SMUS projects. This enables notebooks to appear natively in the SageMaker Unified Studio IDE in the target environment, preserving their metadata, name, and description rather than existing only as S3 files that require workflow-based execution.

The export process uses `ListNotebooks` to discover notebooks in the source project, `GetNotebook` to retrieve each notebook's full metadata (including parameters, metadata, and environmentConfiguration), `StartNotebookExport` to initiate an asynchronous export of each notebook to IPYNB format, and `GetNotebookExport` to poll for completion and retrieve the S3 output location. The exported `.ipynb` files are included in the bundle archive. During deployment, the import follows an upsert (create-or-replace) strategy: existing notebooks with the same name in the target project are detected via `ListNotebooks`, the new notebook is always created via `StartNotebookImport` (since duplicate names are allowed), polled until ACTIVE via `GetNotebook`, updated with metadata via `UpdateNotebook`, and only then is the old notebook (if any) deleted via `DeleteNotebook`. This ensures zero data loss — the old version is only removed after the new version is confirmed working.

**Out of Scope:** Notebook schedules are explicitly out of scope for this iteration. No public API for schedule management currently exists; this will be revisited with the team in a future iteration.

## Glossary

- **CLI**: The `aws-smus-cicd-cli` command-line interface for SMUS CI/CD operations
- **Bundle_Command**: The CLI command that packages application content into a deployable ZIP archive
- **Deploy_Command**: The CLI command that deploys a bundle archive to a target stage's DataZone project
- **Notebook_Exporter**: The component responsible for listing project notebooks via `ListNotebooks`, exporting them via `StartNotebookExport`, polling via `GetNotebookExport`, and downloading the exported `.ipynb` files
- **Notebook_Importer**: The component responsible for uploading `.ipynb` files to S3, performing upsert operations (create new notebook via `StartNotebookImport`, poll until ACTIVE, apply metadata via `UpdateNotebook`, then delete old notebook with same name if one existed)
- **DataZone_Domain**: An Amazon DataZone domain that contains projects and notebook resources
- **DataZone_Project**: A project within a DataZone domain that owns notebook resources
- **SMUS_Notebook**: A notebook resource natively managed by SageMaker Unified Studio, identified by a notebook ID and owned by a project
- **Notebook_Export**: An asynchronous operation initiated by `StartNotebookExport` that exports a notebook to a specified file format and stores the output in S3
- **Notebook_Import**: An operation initiated by `StartNotebookImport` that creates a notebook resource in a project from an S3 source location (always creates a new notebook; duplicate names are allowed)
- **Notebook_Export_Manifest**: A JSON metadata file included in the bundle that records the mapping between notebook names, IDs, and exported file paths
- **ListNotebooks_API**: The DataZone API that lists notebooks owned by a project, supporting pagination, status filtering, and sorting
- **GetNotebook_API**: The DataZone API that retrieves the full details of a notebook resource including its parameters, metadata, and environmentConfiguration fields
- **StartNotebookExport_API**: The DataZone API that initiates an asynchronous notebook export to PDF or IPYNB format
- **GetNotebookExport_API**: The DataZone API that retrieves the status and output location of a notebook export operation
- **StartNotebookImport_API**: The DataZone API that imports a notebook from an S3 location into a project, creating a new notebook resource (duplicate names are allowed, so this always succeeds without ConflictException)
- **UpdateNotebook_API**: The DataZone API that updates a notebook resource's mutable fields including parameters, metadata, and environmentConfiguration
- **DeleteNotebook_API**: The DataZone API that deletes a notebook resource from a project, used to remove old versions after a successful upsert
- **Manifest**: The `manifest.yaml` file that defines application content, stages, and deployment configuration
- **S3_Connection**: An S3 connection associated with a DataZone project, used for storing exported notebook files
- **Output_Location**: The S3 URI where an exported notebook file is stored after a successful export operation
- **Upsert**: The import strategy where a notebook is always created as new, and if an existing notebook with the same name was found, the old one is deleted only after the new one is fully active and updated

## Requirements

### Requirement 1: Manifest Configuration for Notebook Export

**User Story:** As a developer, I want to configure notebook export in my manifest.yaml, so that the bundle command knows which notebooks to export from my SMUS project.

#### Acceptance Criteria

1. THE Manifest SHALL support a `content.notebooks` section to configure notebook export
2. THE `content.notebooks` section SHALL support an `enabled` boolean field to enable or disable notebook export (default: false)
3. IF `content.notebooks.enabled` is set to true and no `include_names` filter is specified, THEN THE Bundle_Command SHALL export all active notebooks owned by the source project
4. THE `content.notebooks` section SHALL support an optional `include_names` field containing a list of notebook name strings to export, where each entry is a notebook name (up to 256 characters, case-sensitive exact match)
5. IF `content.notebooks.include_names` is specified, THEN THE Bundle_Command SHALL match each entry against the notebook name (case-sensitive exact match) and export notebooks that match
6. THE `content.notebooks` section SHALL support an optional `exclude_names` field containing a list of notebook name strings to exclude from export, where each entry is a notebook name (up to 256 characters, case-sensitive exact match)
7. IF `content.notebooks.exclude_names` is specified, THEN THE Bundle_Command SHALL match each entry against the notebook name (case-sensitive exact match) and exclude notebooks that match
8. IF both `include_names` and `exclude_names` are specified, THEN THE Bundle_Command SHALL apply `include_names` first (selecting matching notebooks), then apply `exclude_names` to remove any matches from the selected set
9. IF `content.notebooks.enabled` is false or absent, THEN THE Bundle_Command SHALL skip notebook export regardless of whether `include_names` or `exclude_names` fields are present
10. IF an entry in the `include_names` list does not match any active notebook in the source project, THEN THE Bundle_Command SHALL log a warning identifying the unmatched name and continue exporting the remaining matched notebooks
11. IF the `include_names` or `exclude_names` field is present but contains an empty list, THEN THE Bundle_Command SHALL raise a validation error indicating that the list must contain at least one entry

### Requirement 2: Export Notebooks During Bundle

**User Story:** As a developer, I want the bundle command to export SMUS notebooks from my source project, so that I can promote notebook resources across stages.

#### Acceptance Criteria

1. WHEN the bundle command runs and `content.notebooks.enabled` is true, THE Notebook_Exporter SHALL call the ListNotebooks_API to discover all active notebooks owned by the source project, using the `domainIdentifier`, `owningProjectIdentifier` parameter, and filtering by `status=ACTIVE`
2. THE Notebook_Exporter SHALL handle pagination by following `nextToken` until all notebooks are retrieved from the ListNotebooks_API
3. WHEN a notebook is selected for export, THE Notebook_Exporter SHALL call the GetNotebook_API with `domainIdentifier` and `notebookIdentifier` to retrieve the notebook's `parameters`, `metadata`, and `environmentConfiguration` fields
4. WHEN a notebook is selected for export, THE Notebook_Exporter SHALL call the StartNotebookExport_API with `domainIdentifier`, `fileFormat` set to `IPYNB`, the notebook's identifier as `notebookIdentifier`, and the source project identifier as `owningProjectIdentifier`
5. WHEN the StartNotebookExport_API returns an export identifier, THE Notebook_Exporter SHALL poll the GetNotebookExport_API using exponential backoff starting at 2 seconds and capped at 30 seconds per interval, until the export status transitions from `IN_PROGRESS` to `SUCCEEDED` or `FAILED`
6. WHEN the export status is `SUCCEEDED`, THE Notebook_Exporter SHALL download the exported `.ipynb` file from the `outputLocation` S3 URI returned by GetNotebookExport_API
7. IF the export status is `FAILED`, THEN THE Notebook_Exporter SHALL log the error message from the `error` field, count the notebook as failed, and continue with the next notebook
8. THE Notebook_Exporter SHALL store each exported `.ipynb` file in a `notebooks/` directory within the bundle archive, using the notebook's source identifier as the filename (e.g., `notebooks/{sourceNotebookId}.ipynb`)
9. THE Notebook_Exporter SHALL produce a `notebooks/notebook_export_manifest.json` metadata file in the bundle containing the list of exported notebooks with their source IDs, names, descriptions, file paths within the bundle, and the `parameters`, `metadata`, and `environmentConfiguration` fields retrieved from the GetNotebook_API
10. THE Notebook_Exporter SHALL implement a configurable polling timeout with a default of 300 seconds per notebook to avoid indefinite waiting on stuck export operations
11. IF the polling timeout is exceeded for a notebook export, THEN THE Notebook_Exporter SHALL log a warning including the notebook name and elapsed time, count the notebook as failed, and continue with the next notebook
12. WHEN all notebooks have been processed, IF any notebooks failed to export, THEN THE Bundle_Command SHALL exit with a non-zero exit code and output a failure message listing the names of all notebooks that failed and their respective error messages

### Requirement 3: Notebook Export Manifest Serialization

**User Story:** As a developer, I want the exported notebook metadata to be stored in a structured manifest file, so that the import process has the information needed to recreate notebooks in the target project.

#### Acceptance Criteria

1. THE Notebook_Exporter SHALL produce a UTF-8 encoded JSON file at `notebooks/notebook_export_manifest.json` within the bundle archive
2. THE Notebook_Export_Manifest SHALL contain a top-level object with exactly two keys: `metadata` (object) and `notebooks` (array)
3. THE `metadata` section SHALL include `sourceProjectId` (string), `sourceDomainId` (string), `exportTimestamp` (string in ISO 8601 format, e.g. `2024-01-15T10:30:00Z`), and `notebookCount` (integer equal to the length of the `notebooks` array)
4. EACH entry in the `notebooks` array SHALL include `sourceNotebookId` (string), `name` (string), `description` (string, empty string if the source notebook has no description), `filePath` (string, relative path within the bundle pointing to an existing `.ipynb` file), `exportedAt` (string in ISO 8601 format), `parameters` (object, string-to-string map with up to 50 entries where keys are max 128 characters and values are max 1024 characters, empty object if the source notebook has no parameters), `metadata` (object, string-to-string map with up to 50 entries where keys are max 128 characters and values are max 1024 characters, empty object if the source notebook has no metadata), and `environmentConfiguration` (object containing `imageVersion` string, and `packageConfig` object with `packageManager` string and `packageSpecification` string; null if the source notebook has no environment configuration)
5. THE Notebook_Export_Manifest SHALL be valid JSON parseable by a standard JSON parser, and every `filePath` entry SHALL correspond to a file present in the bundle archive
6. IF the source project has no notebooks matching the configured filters, THEN THE Notebook_Exporter SHALL produce a Notebook_Export_Manifest with an empty `notebooks` array and `notebookCount` set to 0

### Requirement 4: Import Notebooks During Deploy (Upsert)

**User Story:** As a developer, I want the deploy command to import notebooks into the target SMUS project using an upsert strategy, so that exported notebooks appear as native notebook resources in the target environment and existing notebooks with the same name are seamlessly replaced without data loss.

#### Acceptance Criteria

1. WHEN the deploy command processes a bundle containing a `notebooks/notebook_export_manifest.json` file, THE Deploy_Command SHALL invoke the Notebook_Importer after storage deployments
2. WHEN the Notebook_Importer is invoked, THE Notebook_Importer SHALL upload each exported `.ipynb` file referenced in the Notebook_Export_Manifest from the bundle to the target project's `default.s3_shared` connection S3 URI under a `notebooks/imports/` prefix
3. WHEN a notebook entry in the Notebook_Export_Manifest references a `filePath` that does not exist in the bundle archive, THE Notebook_Importer SHALL log an error identifying the missing file and notebook name, count it as failed, and continue with the next notebook
4. BEFORE importing each notebook, THE Notebook_Importer SHALL call the ListNotebooks_API with `owningProjectIdentifier` and `status=ACTIVE` filter on the target project to discover existing notebooks, and identify any notebook with a name matching the notebook being imported as the "old version"
5. FOR EACH notebook in the Notebook_Export_Manifest whose file was successfully uploaded, THE Notebook_Importer SHALL call the StartNotebookImport_API with the notebook's `name`, `description`, the target project's `owningProjectIdentifier`, the target project's domain identifier as `domainIdentifier`, and the `sourceLocation` pointing to the uploaded S3 file
6. THE Notebook_Importer SHALL use a client token derived from the notebook name and deployment timestamp, truncated to a maximum of 64 characters, to ensure idempotent import operations (retrying the same deploy produces the same notebook, not a duplicate)
7. WHEN the StartNotebookImport_API returns a notebook identifier, THE Notebook_Importer SHALL poll the GetNotebook_API with exponential backoff (starting at 1 second, doubling on each poll, capped at 10 seconds) until the newly created notebook's status is `ACTIVE`
8. IF the newly created notebook does not reach `ACTIVE` status within 120 seconds, THEN THE Notebook_Importer SHALL log a warning including the notebook name and elapsed time, count the notebook as failed, and continue with the next notebook
9. WHEN the newly created notebook status is `ACTIVE`, THE Notebook_Importer SHALL call the UpdateNotebook_API with the target project's `domainIdentifier`, the newly imported notebook's identifier, `owningProjectIdentifier`, and the `parameters`, `metadata`, and `environmentConfiguration` values from the manifest
10. IF an old version notebook with the same name was identified in step 4, THEN THE Notebook_Importer SHALL delete the old notebook via the DeleteNotebook_API only after the new notebook has reached ACTIVE status and the UpdateNotebook_API call has completed (successfully or with a warning)
11. IF the DeleteNotebook_API call for the old version fails, THEN THE Notebook_Importer SHALL log a warning including the old notebook name and identifier and error details, but SHALL NOT count the overall notebook import as failed (the new notebook is already active)
12. IF the StartNotebookImport_API returns any error, THEN THE Notebook_Importer SHALL log the error with notebook name and error details, count the notebook as failed, and continue with the next notebook
13. WHEN all notebooks in the Notebook_Export_Manifest have been processed, THE Notebook_Importer SHALL output to stdout the counts of imported (new, no previous version existed), updated (replaced an existing notebook with the same name), and failed notebooks as the import summary
14. WHEN all notebooks in the Notebook_Export_Manifest have been processed, IF any notebooks failed to import or update, THEN THE Deploy_Command SHALL exit with a non-zero exit code and output a failure message listing the names of all notebooks that failed and their respective error messages

### Requirement 5: Deploy Command Integration

**User Story:** As a developer, I want notebook import to be integrated into the existing deploy command flow, so that notebooks are deployed alongside other bundle content.

#### Acceptance Criteria

1. THE Deploy_Command SHALL process notebook imports after storage and catalog deployments, and before bootstrap actions
2. WHERE the stage's `deployment_configuration.notebooks.disable` is set to true, THE Deploy_Command SHALL skip notebook import for that stage and log an informational message indicating notebook import was skipped due to configuration
3. IF notebook import is not disabled and the bundle contains a `notebooks/notebook_export_manifest.json`, THEN THE Deploy_Command SHALL invoke the Notebook_Importer to process the notebooks listed in the manifest
4. IF the bundle does not contain a `notebooks/notebook_export_manifest.json`, THEN THE Deploy_Command SHALL skip notebook import without producing any warning or error output
5. WHEN the Notebook_Importer completes processing, THE Deploy_Command SHALL report the notebook import summary (imported count, updated count, failed count) in the overall deployment output

### Requirement 6: S3 Upload for Notebook Import

**User Story:** As a developer, I want exported notebook files to be uploaded to an accessible S3 location before import, so that the StartNotebookImport API can read them.

#### Acceptance Criteria

1. THE Notebook_Importer SHALL upload `.ipynb` files to the target project's `default.s3_shared` connection S3 URI under a `notebooks/imports/` prefix, constructing the full S3 key as `{s3Uri}/notebooks/imports/{sourceNotebookId}.ipynb`
2. WHEN uploading notebook files, THE Notebook_Importer SHALL verify that the target project's `default.s3_shared` connection exists and contains a non-empty `s3Uri` value before attempting any uploads
3. IF the S3 upload fails for a notebook file, THEN THE Notebook_Importer SHALL log the error including the notebook name and S3 destination key, skip the import for that notebook, and continue processing remaining notebooks
4. IF the target project's `default.s3_shared` connection does not exist or does not contain an `s3Uri` value, THEN THE Notebook_Importer SHALL raise an error indicating the missing connection and skip all notebook imports for that project

### Requirement 7: Error Handling and Resilience

**User Story:** As a developer, I want robust error handling during notebook export and import, so that partial failures do not block the entire deployment.

#### Acceptance Criteria

1. IF the ListNotebooks_API returns an error during export, THEN THE Notebook_Exporter SHALL raise an exception with an error message including the domain identifier, project identifier, and the error response from the API
2. IF the source project has no notebooks matching the configured filters, THEN THE Notebook_Exporter SHALL produce an empty Notebook_Export_Manifest with zero notebooks and log an informational message indicating the project identifier and the filters that were applied
3. IF a StartNotebookExport_API call fails for a specific notebook during export, THEN THE Notebook_Exporter SHALL log the notebook name and error, then continue with the next notebook
4. IF the Notebook_Export_Manifest is missing the top-level `metadata` or `notebooks` keys, or if `metadata` is missing any of `sourceProjectId`, `sourceDomainId`, `exportTimestamp`, or `notebookCount`, THEN THE Notebook_Importer SHALL raise a validation error before attempting any API calls
5. IF any notebook imports or updates fail, THEN THE Deploy_Command SHALL fail the overall deployment with a non-zero exit code and output a failure message listing the names of all notebooks that failed and their respective error messages
6. THE Notebook_Exporter SHALL implement exponential backoff with jitter when polling GetNotebookExport_API, starting at an initial interval of 1 second, doubling on each poll, up to a maximum interval of 30 seconds per poll
7. IF a ThrottlingException is received from any DataZone API call, THEN THE component SHALL retry the request with exponential backoff starting at 1 second and doubling on each retry, up to a maximum of 3 retries

### Requirement 8: Dry Run Validation for Notebooks

**User Story:** As a developer, I want the deploy dry-run to validate notebook import prerequisites, so that I can detect issues before actual deployment.

#### Acceptance Criteria

1. WHEN the deploy command runs with `--dry-run` and the bundle contains a `notebooks/notebook_export_manifest.json` file, THE Dry_Run_Engine SHALL verify that the target project's `default.s3_shared` connection exists and is reachable by performing a HEAD request against the S3 bucket resolved from the connection
2. WHEN the deploy command runs with `--dry-run` and the bundle contains a `notebooks/notebook_export_manifest.json` file, THE Dry_Run_Engine SHALL verify that the IAM identity has the `datazone:StartNotebookImport`, `datazone:UpdateNotebook`, `datazone:GetNotebook`, `datazone:DeleteNotebook`, `datazone:ListNotebooks`, and `s3:PutObject` permissions using `iam:SimulatePrincipalPolicy`
3. WHEN the deploy command runs with `--dry-run` and the bundle contains a `notebooks/notebook_export_manifest.json` file, THE Dry_Run_Engine SHALL report the number of notebooks that would be imported (read from the manifest's `notebookCount` field) in the dry-run output
4. IF the S3 connection is not found or the HEAD request to the resolved S3 bucket fails, THEN THE Dry_Run_Engine SHALL report a WARNING finding indicating the connection name and the failure reason
5. IF `iam:SimulatePrincipalPolicy` reports a denied decision for `datazone:StartNotebookImport`, `datazone:UpdateNotebook`, `datazone:GetNotebook`, `datazone:DeleteNotebook`, `datazone:ListNotebooks`, or `s3:PutObject`, THEN THE Dry_Run_Engine SHALL report a WARNING finding indicating the denied permission name and resource ARN

### Requirement 9: Destroy Command Support for Notebooks

**User Story:** As a developer, I want the destroy command to clean up imported notebooks, so that I can remove all deployed resources from a target environment.

#### Acceptance Criteria

1. WHEN the destroy command validates a target stage and `content.notebooks.enabled` is true in the manifest, THE Destroy_Command SHALL call the ListNotebooks_API with the target project's `owningProjectIdentifier` and `status=ACTIVE` filter to discover notebook resources, handling pagination by following `nextToken` until all notebooks are retrieved
2. THE Destroy_Command SHALL apply the same `include_names` and `exclude_names` filters from the manifest's `content.notebooks` section to the discovered notebooks, matching by notebook name (case-sensitive exact match) only
3. IF the ListNotebooks_API returns an error during validation, THEN THE Destroy_Command SHALL report the error and fail validation for that stage
4. THE Destroy_Command SHALL display each filtered notebook's name and identifier in the destruction plan under a `notebook` resource type for user confirmation before deletion
5. IF the user confirms destruction, THEN THE Destroy_Command SHALL delete each notebook resource in the target project using the DataZone domain identifier and notebook identifier, continuing to the next notebook if an individual deletion fails
6. IF a notebook is not found at deletion time (ResourceNotFoundException), THEN THE Destroy_Command SHALL record the notebook as `not_found` and continue with the next notebook
7. IF a notebook deletion fails with any other error, THEN THE Destroy_Command SHALL log the notebook name and error details, record the notebook as `error`, and continue with the next notebook
8. THE Destroy_Command SHALL report the count of deleted, not_found, and failed notebook deletions in the destruction summary consistent with the existing summary format

### Requirement 10: Port Notebook Metadata via UpdateNotebook (Upsert Flow)

**User Story:** As a developer, I want imported notebooks to retain their parameters, metadata, and environment configuration from the source notebook, so that notebooks in the target environment behave identically to those in the source environment.

#### Acceptance Criteria

1. WITHIN the upsert flow, WHEN the newly imported notebook reaches `ACTIVE` status, THE Notebook_Importer SHALL call the UpdateNotebook_API with the target project's `domainIdentifier`, the newly imported notebook's identifier, `owningProjectIdentifier`, and the `parameters`, `metadata`, and `environmentConfiguration` values from the manifest, BEFORE deleting the old notebook (if one exists)
2. IF the `parameters` field in the manifest entry is an empty object, THEN THE Notebook_Importer SHALL omit the `parameters` field from the UpdateNotebook_API call
3. IF the `metadata` field in the manifest entry is an empty object, THEN THE Notebook_Importer SHALL omit the `metadata` field from the UpdateNotebook_API call
4. IF the `environmentConfiguration` field in the manifest entry is null, THEN THE Notebook_Importer SHALL omit the `environmentConfiguration` field from the UpdateNotebook_API call
5. IF the UpdateNotebook_API returns a ValidationException due to invalid parameter constraints (keys exceeding 128 characters, values exceeding 1024 characters, or more than 50 entries), THEN THE Notebook_Importer SHALL log a warning including the notebook name and validation error, and count the notebook as imported with a metadata warning
6. IF the UpdateNotebook_API returns any other error, THEN THE Notebook_Importer SHALL log the notebook name and error details, and count the notebook as imported with a metadata warning rather than failed
7. WHEN the Notebook_Importer reports the import summary, THE Notebook_Importer SHALL include a count of notebooks where metadata porting encountered warnings alongside the imported, updated, and failed counts

### Requirement 11: Unique Notebook Names Enforcement

**User Story:** As a developer, I want the bundle command to enforce unique notebook names, so that the upsert logic can reliably identify which notebook is being created or updated in the target environment.

#### Acceptance Criteria

1. AFTER applying the `include_names` and `exclude_names` filters, THE Notebook_Exporter SHALL verify that all selected notebooks have unique names (case-sensitive comparison)
2. IF the selected notebooks contain two or more notebooks with the same name, THEN THE Bundle_Command SHALL skip exporting any notebooks and fail with a non-zero exit code, outputting an error message listing the duplicate notebook name(s) and the count of notebooks sharing each name
3. THE error message SHALL instruct the user to resolve the duplicate names in the source project or use `include_names`/`exclude_names` filters to select only one notebook per name before re-running the bundle command
4. WHEN the Notebook_Importer reads the `notebook_export_manifest.json` during deploy, THE Notebook_Importer SHALL validate that all notebook entries in the manifest have unique `name` values (case-sensitive comparison) before attempting any import operations
5. IF the manifest contains two or more notebook entries with the same `name` value, THEN THE Notebook_Importer SHALL raise a validation error and skip all notebook imports for that deployment

### Requirement 12: Documentation — Notebook Name-Based Update Semantics

**User Story:** As a developer reading the documentation, I want to clearly understand that notebook updates are matched by name, so that I am aware that notebooks with the same name in the target environment will be replaced during deployment.

#### Acceptance Criteria

1. THE customer-facing documentation for this feature SHALL prominently state that notebooks are identified by name across environments, and that any existing notebook with the same name in the target project will be replaced (old version deleted) during deployment
2. THE documentation SHALL include a warning or callout box emphasizing that notebooks in the target environment with matching names will be overwritten regardless of whether they were originally deployed by this CI/CD tool
3. THE documentation SHALL recommend that users adopt a consistent notebook naming convention across environments and verify notebook names before running the deploy command
4. THE documentation SHALL appear in the CLI help text for the deploy command (when notebooks are involved) as a brief reminder that notebook updates are name-based
