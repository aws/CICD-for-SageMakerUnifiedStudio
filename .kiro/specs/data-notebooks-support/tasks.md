# Implementation Plan: Data Notebooks Support

## Overview

Add native Data Notebooks support to the SMUS CI/CD CLI, enabling promotion of SageMaker Unified Studio notebooks across environments (dev → test → prod) using the DataZone Notebook Import/Export APIs. Implementation follows the existing catalog import/export architecture — new helper modules (`notebook_export.py`, `notebook_import.py`), manifest configuration, and integration into `bundle`, `deploy`, `destroy`, and `dry-run` commands.

## Tasks

- [ ] 1. Manifest configuration and data models
  - [ ] 1.1 Add NotebookConfig dataclass and update ContentConfig in `application_manifest.py`
    - Add `NotebookConfig` dataclass with `enabled: bool`, `include_names: Optional[List[str]]`, `exclude_names: Optional[List[str]]`
    - Add `notebooks: Optional[NotebookConfig] = None` field to `ContentConfig`
    - Add `notebooks: Optional[Dict[str, Any]] = None` field to `DeploymentConfiguration`
    - Add parsing logic in `ApplicationManifest.from_dict()` for `content.notebooks` section and `deployment_configuration.notebooks` section
    - Validate that `include_names`/`exclude_names` cannot be empty lists (raise validation error)
    - _Requirements: 1.1, 1.2, 1.9, 1.11_

  - [ ] 1.2 Add "notebook" resource type to destroy models
    - Add `"notebook"` to `DESTROY_SUPPORTED_RESOURCE_TYPES` in `src/smus_cicd/helpers/destroy_models.py`
    - Update `resource_types.py` to include `"notebook"` in `DEPLOY_RESOURCE_TYPES` if applicable
    - _Requirements: 9.1, 9.4_

  - [ ]* 1.3 Write unit tests for manifest NotebookConfig parsing
    - Test enabled/disabled configurations
    - Test include_names/exclude_names parsing
    - Test empty list validation error
    - Test deployment_configuration.notebooks.disable parsing
    - **File**: `tests/unit/test_notebook_manifest_config.py`
    - _Requirements: 1.1, 1.2, 1.9, 1.11_

- [ ] 2. Implement notebook export helper
  - [ ] 2.1 Create `src/smus_cicd/helpers/notebook_export.py` with core export logic
    - Implement `export_notebooks()` main entry point
    - Implement `_list_all_notebooks()` with pagination via `nextToken`
    - Implement `_apply_filters()` with include-first-then-exclude logic (case-sensitive exact match on notebook name)
    - Implement `_matches_name_filter()` helper
    - Implement `_warn_duplicate_names()` to enforce unique names in the filtered set
    - Implement `_export_single_notebook()` calling StartNotebookExport, polling, and downloading
    - Implement `_poll_export_status()` with exponential backoff (initial 2s, cap 30s, timeout 300s)
    - Implement `_build_export_manifest()` to produce the `notebook_export_manifest.json` structure
    - Implement `_retry_on_throttle()` decorator for ThrottlingException handling (max 3 retries)
    - Define `ExportedNotebook` and `NotebookExportManifest` dataclasses
    - _Requirements: 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.10, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 2.10, 2.11, 2.12, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 7.1, 7.2, 7.3, 7.6, 7.7, 11.1, 11.2, 11.3_

  - [ ]* 2.2 Write property test: Include/Exclude Name-Only Filter Algebra
    - **Property 1: Include/Exclude Name-Only Filter Algebra**
    - **Validates: Requirements 1.5, 1.7, 1.8, 9.2**
    - **File**: `tests/unit/helpers/test_notebook_properties.py`

  - [ ]* 2.3 Write property test: Pagination Completeness
    - **Property 2: Pagination Completeness**
    - **Validates: Requirements 2.2**
    - **File**: `tests/unit/helpers/test_notebook_properties.py`

  - [ ]* 2.4 Write property test: Export Manifest Schema Completeness
    - **Property 3: Export Manifest Schema Completeness**
    - **Validates: Requirements 3.2, 3.3, 3.4**
    - **File**: `tests/unit/helpers/test_notebook_properties.py`

  - [ ]* 2.5 Write property test: Bundle Internal Consistency
    - **Property 4: Bundle Internal Consistency**
    - **Validates: Requirements 3.5**
    - **File**: `tests/unit/helpers/test_notebook_properties.py`

  - [ ]* 2.6 Write property test: Unique Name Enforcement
    - **Property 10: Unique Name Enforcement**
    - **Validates: Requirements 11.1, 11.2**
    - **File**: `tests/unit/helpers/test_notebook_properties.py`

  - [ ]* 2.7 Write property test: Exponential Backoff Intervals
    - **Property 8: Exponential Backoff Intervals**
    - **Validates: Requirements 2.5, 7.6**
    - **File**: `tests/unit/helpers/test_notebook_properties.py`

  - [ ]* 2.8 Write unit tests for notebook export
    - Test happy path: ListNotebooks → GetNotebook → StartNotebookExport → GetNotebookExport → S3 download
    - Test partial failures (some notebooks fail, others succeed)
    - Test duplicate notebook names (bundle fails)
    - Test empty project (empty manifest produced)
    - Test unmatched include_names (warning logged, continue)
    - Test polling timeout handling
    - **File**: `tests/unit/helpers/test_notebook_export.py`
    - _Requirements: 2.1, 2.2, 2.7, 2.10, 2.11, 2.12, 3.6, 7.1, 7.2, 7.3, 11.1_

- [ ] 3. Checkpoint - Ensure export tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 4. Implement notebook import helper
  - [ ] 4.1 Create `src/smus_cicd/helpers/notebook_import.py` with upsert import logic
    - Implement `import_notebooks()` main entry point
    - Implement `_validate_notebook_manifest()` to validate required manifest keys
    - Implement `_warn_manifest_duplicates()` to reject duplicate names in manifest
    - Implement `_list_existing_notebooks_by_name()` to discover old versions in target project
    - Implement `_find_old_versions()` helper
    - Implement `_upload_notebook_to_s3()` using `{s3Uri}/notebooks/imports/{sourceNotebookId}.ipynb`
    - Implement `_generate_client_token()` (deterministic, max 64 chars)
    - Implement `_import_single_notebook()` with full upsert flow: upload → StartNotebookImport → poll ACTIVE → UpdateNotebook → delete old versions
    - Implement `_poll_notebook_active()` (exponential backoff: initial 1s, cap 10s, timeout 120s)
    - Implement `_apply_notebook_metadata()` calling UpdateNotebook API
    - Implement `_build_update_kwargs()` omitting empty fields
    - Implement `_delete_old_versions()` with warning-only on failure
    - Define `NotebookImportSummary`, `ImportResult`, `ImportStatus` dataclasses/enum
    - Reuse `_retry_on_throttle()` from notebook_export or shared utility
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.9, 4.10, 4.11, 4.12, 4.13, 4.14, 6.1, 6.2, 6.3, 6.4, 7.4, 7.5, 7.7, 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 11.4, 11.5_

  - [ ]* 4.2 Write property test: Client Token Determinism and Bounds
    - **Property 5: Client Token Determinism and Bounds**
    - **Validates: Requirements 4.6**
    - **File**: `tests/unit/helpers/test_notebook_properties.py`

  - [ ]* 4.3 Write property test: Manifest Validation Rejects Malformed Input
    - **Property 6: Manifest Validation Rejects Malformed Input**
    - **Validates: Requirements 7.4**
    - **File**: `tests/unit/helpers/test_notebook_properties.py`

  - [ ]* 4.4 Write property test: UpdateNotebook Omits Empty Fields
    - **Property 7: UpdateNotebook Omits Empty Fields**
    - **Validates: Requirements 10.2, 10.3, 10.4**
    - **File**: `tests/unit/helpers/test_notebook_properties.py`

  - [ ]* 4.5 Write property test: Upsert Ordering Safety
    - **Property 9: Upsert Ordering Safety**
    - **Validates: Requirements 4.10, 10.1**
    - **File**: `tests/unit/helpers/test_notebook_properties.py`

  - [ ]* 4.6 Write unit tests for notebook import
    - Test upsert happy path: old version found → upload → import → poll ACTIVE → UpdateNotebook → delete old
    - Test new notebook import (no old version exists)
    - Test missing file in bundle (counted as failed)
    - Test UpdateNotebook failure (metadata warning, not failed)
    - Test DeleteNotebook failure for old version (warning, not failed)
    - Test manifest with duplicate names (validation error, import rejected)
    - Test missing S3 connection (error raised, all imports skipped)
    - Test malformed manifest (validation error before any API calls)
    - **File**: `tests/unit/helpers/test_notebook_import.py`
    - _Requirements: 4.1, 4.3, 4.10, 4.11, 4.12, 6.4, 7.4, 10.5, 10.6, 11.4, 11.5_

- [ ] 5. Checkpoint - Ensure import tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 6. Integrate into bundle command
  - [ ] 6.1 Modify `src/smus_cicd/commands/bundle.py` to call notebook export
    - After catalog export section, check `content.notebooks.enabled`
    - Call `export_notebooks()` with domain_id, project_id, region, include_names, exclude_names
    - Write exported `.ipynb` files to `notebooks/` directory in the bundle ZIP
    - Write `notebooks/notebook_export_manifest.json` to the bundle ZIP
    - Fail bundle with non-zero exit code if any notebooks failed to export
    - _Requirements: 1.3, 1.9, 2.1, 2.8, 2.9, 2.12_

  - [ ]* 6.2 Write unit tests for bundle command notebook integration
    - Test bundle with notebooks enabled (exports included in ZIP)
    - Test bundle with notebooks disabled (no notebook processing)
    - Test bundle with partial export failures (non-zero exit)
    - **File**: `tests/unit/test_bundle_notebooks.py`
    - _Requirements: 1.9, 2.12_

- [ ] 7. Integrate into deploy command
  - [ ] 7.1 Modify `src/smus_cicd/commands/deploy.py` to call notebook import
    - After storage and catalog deployments, check for `notebooks/notebook_export_manifest.json` in bundle
    - Check `deployment_configuration.notebooks.disable` for the stage — skip if true
    - Resolve `default.s3_shared` connection's S3 URI from the target project
    - Call `import_notebooks()` with domain_id, project_id, region, manifest_data, notebook_files, s3_uri
    - Report import summary (imported, updated, failed counts) in deployment output
    - Fail deploy with non-zero exit code if any notebooks failed
    - _Requirements: 4.1, 4.13, 4.14, 5.1, 5.2, 5.3, 5.4, 5.5_

  - [ ]* 7.2 Write unit tests for deploy command notebook integration
    - Test deploy with notebook manifest present (imports invoked)
    - Test deploy with no manifest (skip silently)
    - Test deploy with notebooks disabled in deployment_configuration
    - Test deploy with import failures (non-zero exit)
    - **File**: `tests/unit/commands/test_deploy_notebooks.py`
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_

- [ ] 8. Integrate into destroy command
  - [ ] 8.1 Add notebook discovery and deletion to destroy validator and executor
    - Add `_discover_notebooks()` to `src/smus_cicd/helpers/destroy_validator.py` using ListNotebooks with pagination and name-only filters
    - Add `_delete_notebook()` to `src/smus_cicd/helpers/destroy_executor.py` calling DeleteNotebook API
    - Handle ResourceNotFoundException as `not_found`, other errors as `error`
    - Display notebooks in destruction plan under `notebook` resource type
    - Report deleted/not_found/failed counts in destruction summary
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 9.8_

  - [ ]* 8.2 Write unit tests for destroy notebook integration
    - Test notebook discovery with name filtering
    - Test deletion with mixed results (deleted, not_found, error)
    - Test ListNotebooks API failure during validation
    - **File**: `tests/unit/commands/test_notebook_destroy.py`
    - _Requirements: 9.1, 9.2, 9.3, 9.5, 9.6, 9.7, 9.8_

- [ ] 9. Implement dry-run notebook checker
  - [ ] 9.1 Create `src/smus_cicd/commands/dry_run/checkers/notebook_checker.py`
    - Implement `NotebookChecker` class with `check()` method
    - Verify `default.s3_shared` connection exists and bucket is accessible (HEAD request)
    - Verify IAM permissions via `iam:SimulatePrincipalPolicy`: `datazone:StartNotebookImport`, `datazone:UpdateNotebook`, `datazone:GetNotebook`, `datazone:DeleteNotebook`, `datazone:ListNotebooks`, `s3:PutObject`
    - Report notebook count from manifest's `notebookCount` field
    - Report WARNING findings for missing connection or denied permissions
    - Register checker in the dry-run engine (`src/smus_cicd/commands/dry_run/engine.py`)
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5_

  - [ ]* 9.2 Write unit tests for dry-run notebook checker
    - Test S3 connection found and accessible
    - Test S3 connection missing (WARNING finding)
    - Test IAM permissions denied (WARNING findings)
    - Test notebook count reported correctly
    - **File**: `tests/unit/commands/test_notebook_dry_run.py`
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5_

- [ ] 10. Checkpoint - Ensure all integration tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 11. Create example and documentation
  - [ ] 11.1 Create `examples/notebook-import-export/` example directory
    - Create `examples/notebook-import-export/manifest.yaml` with `content.notebooks` configuration including `include_names`
    - Create `examples/notebook-import-export/README.md` with usage documentation
    - Create `examples/notebook-import-export/app_tests/test_notebook_lifecycle.py` end-to-end test
    - Document name-based update semantics prominently with callout warning
    - _Requirements: 12.1, 12.2, 12.3, 12.4_

  - [ ]* 11.2 Write integration test for full notebook lifecycle
    - Test flow: create notebooks → bundle → deploy (new) → verify → update source → bundle → deploy (upsert) → verify update → destroy → verify removed
    - **File**: `examples/notebook-import-export/app_tests/test_notebook_lifecycle.py`
    - _Requirements: 4.1, 4.10, 4.13, 9.5, 9.8_

- [ ] 12. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document (Properties 1–10)
- Unit tests validate specific examples, edge cases, and error paths
- The implementation mirrors the existing `catalog_export.py` / `catalog_import.py` architecture
- All property tests go in a single file `tests/unit/helpers/test_notebook_properties.py` following the existing `test_catalog_export_properties.py` pattern
- The `_retry_on_throttle()` utility may be shared between export and import modules or extracted to a common helper

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["1.3", "2.1"] },
    { "id": 2, "tasks": ["2.2", "2.3", "2.4", "2.5", "2.6", "2.7", "2.8"] },
    { "id": 3, "tasks": ["4.1"] },
    { "id": 4, "tasks": ["4.2", "4.3", "4.4", "4.5", "4.6"] },
    { "id": 5, "tasks": ["6.1", "8.1", "9.1"] },
    { "id": 6, "tasks": ["6.2", "7.1", "8.2", "9.2"] },
    { "id": 7, "tasks": ["7.2"] },
    { "id": 8, "tasks": ["11.1"] },
    { "id": 9, "tasks": ["11.2"] }
  ]
}
```
