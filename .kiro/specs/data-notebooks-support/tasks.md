# Implementation Plan: Data Notebooks Support

## Overview

Implements native Data Notebooks support for the SMUS CI/CD CLI, enabling promotion of SageMaker Unified Studio notebooks across environments using DataZone Notebook APIs. The implementation follows the existing catalog import/export architecture with two new helper modules (`notebook_export.py` and `notebook_import.py`), manifest configuration extensions, and integration into the `bundle`, `deploy`, `destroy`, and `dry-run` commands.

## Tasks

- [ ] 1. Extend manifest configuration and resource types
  - [ ] 1.1 Add NotebookConfig dataclass and extend ContentConfig/DeploymentConfiguration in `application_manifest.py`
    - Add `NotebookConfig` dataclass with `enabled: bool` and `notebook_ids: Optional[List[str]]` fields
    - Add `notebooks: Optional[NotebookConfig] = None` to `ContentConfig`
    - Add `notebooks: Optional[Dict[str, Any]] = None` to `DeploymentConfiguration`
    - Update `ApplicationManifest.from_dict()` to parse `content.notebooks` and `deployment_configuration.notebooks` sections
    - Validate `notebook_ids` entries match pattern `[a-zA-Z0-9_-]{1,36}` and list is non-empty when present
    - _Requirements: 1.1, 1.2, 1.3, 1.8, 1.9_

  - [ ] 1.2 Add `notebook` resource type to `resource_types.py` and `destroy.py`
    - Add `"notebook"` to `DEPLOY_RESOURCE_TYPES` frozenset in `src/smus_cicd/resource_types.py`
    - Add `"notebook"` to `DESTROY_SUPPORTED_RESOURCE_TYPES` in `src/smus_cicd/commands/destroy.py`
    - Ensure the existing `TestDeployDestroyDrift` unit test still passes
    - _Requirements: 9.6_

- [ ] 2. Implement notebook export module
  - [ ] 2.1 Create `src/smus_cicd/helpers/notebook_export.py` with core export logic
    - Implement `export_notebooks()` public function with `domain_id`, `project_id`, `region`, `notebook_ids`, `polling_timeout` parameters
    - Implement `_validate_notebook_ids()` — calls GetNotebook for each ID, collects invalid IDs, raises on any failure (fail-fast)
    - Implement `_list_all_notebooks()` — paginated ListNotebooks with `owningProjectIdentifier` and `status=ACTIVE`
    - Implement `_export_single_notebook()` — StartNotebookExport → poll → download from S3
    - Implement `_poll_export_status()` — exponential backoff (initial 1s, double each poll, cap 30s) with jitter
    - Implement `_build_export_manifest()` — builds the `notebook_export_manifest.json` structure
    - Implement `_generate_client_token()` — deterministic, max 64 chars, from source ID + timestamp
    - Define `ExportedNotebook` dataclass
    - _Requirements: 1.4, 1.5, 1.6, 1.7, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 2.10, 2.11, 2.12, 2.13, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 7.1, 7.2, 7.3, 7.6, 7.7_

  - [ ]* 2.2 Write property test for fail-fast validation completeness (Property 1)
    - **Property 1: Fail-Fast Validation Completeness**
    - Test that for any list of notebook IDs with at least one invalid, all IDs are validated, all invalid IDs are collected, and zero exports occur
    - Use hypothesis strategies: random lists of IDs with random invalid subsets, mock GetNotebook responses
    - **Validates: Requirements 1.4, 1.5, 2.1, 2.2**

  - [ ]* 2.3 Write property test for pagination completeness (Property 2)
    - **Property 2: Pagination Completeness**
    - Test that for any paginated ListNotebooks response, all entries across all pages are accumulated correctly
    - Use hypothesis strategies: random page counts (1-10) and page sizes (1-50), mock paginator
    - **Validates: Requirements 2.3, 2.4, 9.1**

  - [ ]* 2.4 Write property test for export manifest schema correctness (Property 3)
    - **Property 3: Export Manifest Schema Correctness**
    - Test that `_build_export_manifest()` always produces valid schema with correct metadata and required fields
    - Use hypothesis strategies: random ExportedNotebook lists (0-20 entries)
    - **Validates: Requirements 3.2, 3.3, 3.4, 3.6**

  - [ ]* 2.5 Write property test for client token determinism and bounds (Property 5)
    - **Property 5: Client Token Determinism and Bounds**
    - Test that `_generate_client_token()` is deterministic, ≤64 chars, and distinct inputs produce distinct tokens
    - Use hypothesis strategies: random source IDs + timestamps
    - **Validates: Requirements 4.7**

  - [ ]* 2.6 Write property test for exponential backoff intervals (Property 8)
    - **Property 8: Exponential Backoff Intervals**
    - Test that delay for attempt i equals min(initial × 2^(i-1), max_interval)
    - Use hypothesis strategies: random poll counts (1-20)
    - **Validates: Requirements 2.6, 7.6**

  - [ ]* 2.7 Write unit tests for notebook export (`tests/unit/helpers/test_notebook_export.py`)
    - Test manifest parsing: `content.notebooks` section (enabled/disabled, with/without notebook_ids)
    - Test fail-fast: `notebook_ids` with mix of valid/invalid IDs → error lists all invalid
    - Test fail-fast: `notebook_ids` with empty list → validation error
    - Test export happy path: GetNotebook → StartNotebookExport → GetNotebookExport → S3 download
    - Test export partial failure: some notebooks fail export, others succeed
    - Test polling timeout: elapsed time exceeds limit → notebook counted as failed
    - _Requirements: 1.4, 1.5, 1.8, 2.5, 2.6, 2.7, 2.8, 2.11, 2.12, 2.13, 7.1, 7.2, 7.3_

- [ ] 3. Integrate notebook export into the bundle command
  - [ ] 3.1 Modify `src/smus_cicd/commands/bundle.py` to call `export_notebooks()` when `content.notebooks.enabled` is true
    - After catalog export section, add notebook export integration
    - Check `content.notebooks.enabled` flag; skip if false/absent
    - Call `export_notebooks()` with domain_id, project_id, region, and optional notebook_ids
    - Write exported `.ipynb` files to `notebooks/` directory in bundle ZIP
    - Write `notebooks/notebook_export_manifest.json` to bundle ZIP
    - Handle export failures: exit non-zero if any notebooks failed
    - _Requirements: 1.4, 1.5, 1.6, 1.7, 1.9, 2.9, 2.10, 2.13, 3.1, 3.5_

  - [ ]* 3.2 Write property test for bundle internal consistency (Property 4)
    - **Property 4: Bundle Internal Consistency**
    - Test that every `filePath` in manifest corresponds to a file in the bundle, and every `.ipynb` file is referenced
    - Use hypothesis strategies: random notebook sets with file content, end-to-end export pipeline (mocked APIs)
    - **Validates: Requirements 3.5**

- [ ] 4. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 5. Implement notebook import/sync module
  - [ ] 5.1 Create `src/smus_cicd/helpers/notebook_import.py` with core sync logic
    - Implement `sync_notebooks()` public function with the 6-step deployment sequence
    - Implement `_validate_notebook_manifest()` — checks required metadata/notebooks keys
    - Implement `_discover_target_notebooks()` — ListNotebooks + GetNotebook → build source→target map from metadata
    - Implement `_upload_notebook_to_s3()` — upload to `{s3Uri}/notebooks/imports/{sourceNotebookId}.ipynb`
    - Implement `_sync_single_notebook()` — StartNotebookSync with/without notebookId, fallback on ResourceNotFoundException
    - Implement `_apply_notebook_metadata()` — UpdateNotebook API with name, description, metadata, params, envConfig
    - Implement `_build_update_kwargs()` — construct kwargs with conditional field inclusion
    - Define `NotebookSyncSummary`, `SyncResult`, `SyncStatus` dataclasses/enum
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.9, 4.10, 4.11, 4.12, 4.13, 6.1, 6.2, 6.3, 6.4, 7.4, 7.5, 7.7, 7.8, 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.8_

  - [ ]* 5.2 Write property test for manifest validation (Property 6)
    - **Property 6: Manifest Validation Rejects Malformed Input**
    - Test that any JSON missing required keys raises validation error before API calls
    - Use hypothesis strategies: random dicts with strategically missing keys
    - **Validates: Requirements 7.4**

  - [ ]* 5.3 Write property test for UpdateNotebook kwargs construction (Property 7)
    - **Property 7: UpdateNotebook Kwargs Construction**
    - Test metadata always includes tracking key, parameters omitted when empty, environmentConfiguration omitted when None, name/description always present
    - Use hypothesis strategies: random manifest entries with various empty/None/populated fields
    - **Validates: Requirements 10.1, 10.2, 10.3, 10.4, 10.5**

  - [ ]* 5.4 Write property test for source-to-target metadata mapping (Property 9)
    - **Property 9: Source-to-Target Metadata Mapping Correctness**
    - Test that mapping includes exactly those notebooks with `smus-cicd-source-notebook-id` metadata
    - Use hypothesis strategies: random notebooks with/without metadata key, mocked APIs
    - **Validates: Requirements 4.3, 9.2, 9.3**

  - [ ]* 5.5 Write property test for summary count invariant (Property 11)
    - **Property 11: Summary Count Invariant**
    - Test that `created + updated + failed` always equals total notebooks attempted
    - Use hypothesis strategies: random sequences of SyncResult outcomes
    - **Validates: Requirements 4.12, 10.7**

  - [ ]* 5.6 Write unit tests for notebook import (`tests/unit/helpers/test_notebook_import.py`)
    - Test sync happy path: upload → discover targets → StartNotebookSync (create) → UpdateNotebook
    - Test sync update path: existing target found via metadata → StartNotebookSync WITH notebookId
    - Test sync fallback: StartNotebookSync with notebookId → ResourceNotFoundException → retry without
    - Test sync with UpdateNotebook failure → notebook counts as FAILED
    - Test sync with missing file in bundle → count as FAILED
    - Test manifest validation: missing keys → error before API calls
    - Test S3 connection missing → raise error, skip all sync
    - _Requirements: 4.3, 4.4, 4.5, 4.6, 4.8, 4.9, 4.10, 4.11, 6.2, 6.3, 6.4, 7.4, 7.8_

- [ ] 6. Integrate notebook sync into the deploy command
  - [ ] 6.1 Modify `src/smus_cicd/commands/deploy.py` to call `sync_notebooks()` after catalog import
    - Add `_sync_notebooks_from_bundle()` function following `_import_catalog_from_bundle()` pattern
    - Check `deployment_configuration.notebooks.disable` — skip with informational message if true
    - Check bundle for `notebooks/notebook_export_manifest.json` — skip silently if absent
    - Extract manifest and `.ipynb` files from bundle ZIP
    - Resolve S3 connection (`default.s3_shared`) from target project
    - Call `sync_notebooks()` with extracted data
    - Report sync summary (created/updated/failed) in deployment output
    - Return non-zero exit code if any notebooks failed
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 7.5_

  - [ ]* 6.2 Write unit tests for deploy notebook integration (`tests/unit/commands/test_deploy_notebook_sync.py`)
    - Test `deployment_configuration.notebooks.disable: true` → skip with message
    - Test no `notebook_export_manifest.json` in bundle → skip silently
    - Test happy path: manifest present → sync invoked → summary reported
    - Test sync failures → deploy exits non-zero
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_

- [ ] 7. Implement destroy support for notebooks
  - [ ] 7.1 Add notebook discovery and deletion to `destroy_validator.py` and `destroy_executor.py`
    - Add `_discover_notebooks()` to `destroy_validator.py`: ListNotebooks + GetNotebook → filter by `smus-cicd-source-notebook-id` metadata → optional `notebook_ids` filter
    - Integrate into `_validate_stage()` after catalog resources section
    - Add `_delete_notebook()` to `destroy_executor.py`: DeleteNotebook API, handle ResourceNotFoundException (not_found) and other errors
    - Display notebooks in destruction plan under `notebook` resource type
    - Report counts (deleted, not_found, error) in destruction summary
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 9.8, 9.9, 9.10, 9.11_

  - [ ]* 7.2 Write property test for destroy metadata-based filtering (Property 10)
    - **Property 10: Destroy Metadata-Based Filtering**
    - Test that only notebooks with metadata key are included; when `notebook_ids` specified, further filter by matching source IDs
    - Use hypothesis strategies: random target notebooks + random notebook_ids lists, mocked APIs
    - **Validates: Requirements 9.2, 9.3, 9.4, 9.5**

  - [ ]* 7.3 Write unit tests for notebook destroy (`tests/unit/commands/test_notebook_destroy.py`)
    - Test discovery: filters by metadata correctly, respects `notebook_ids` list
    - Test deletion: handles ResourceNotFoundException (not_found), other errors
    - Test source environment: no metadata → zero deletions
    - Test ListNotebooks API failure → validation error
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.7, 9.8, 9.9, 9.10, 9.11_

- [ ] 8. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 9. Implement dry-run checker for notebooks
  - [ ] 9.1 Create `src/smus_cicd/commands/dry_run/checkers/notebook_checker.py`
    - Implement `NotebookChecker` class following `catalog_checker.py` pattern
    - Check S3 connection (`default.s3_shared`) exists and bucket is reachable (HEAD request)
    - Check IAM permissions via SimulatePrincipalPolicy: `datazone:StartNotebookSync`, `datazone:UpdateNotebook`, `datazone:GetNotebook`, `datazone:ListNotebooks`, `s3:PutObject`
    - Report notebook count from manifest's `notebookCount` field
    - Report WARNING findings for missing connection or denied permissions
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5_

  - [ ] 9.2 Integrate `NotebookChecker` into the dry-run engine (`engine.py`)
    - Register `NotebookChecker` in the checker list after catalog checker
    - Invoke only when bundle contains `notebooks/notebook_export_manifest.json`
    - _Requirements: 8.1, 8.2, 8.3_

  - [ ]* 9.3 Write unit tests for notebook dry-run checker (`tests/unit/commands/test_notebook_dry_run.py`)
    - Test S3 connection found and reachable → pass
    - Test S3 connection missing → WARNING
    - Test IAM permission denied → WARNING per denied action
    - Test notebook count reported from manifest
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5_

- [ ] 10. Wire all property-based tests into a single test file
  - [ ] 10.1 Create `tests/unit/helpers/test_notebook_properties.py` consolidating all 11 property tests
    - Aggregate property tests from tasks 2.2-2.6, 3.2, 5.2-5.5, 7.2 into one file following `test_catalog_export_properties.py` pattern
    - Each property uses `@settings(max_examples=100)` and `@given()` decorators
    - Tag format: `# Feature: data-notebooks-support, Property {N}: {description}`
    - Include shared hypothesis strategies for notebook IDs, manifest entries, metadata dicts
    - _Requirements: All correctness properties P1-P11_

- [ ] 11. Integration test following existing patterns
  - [ ] 11.1 Create integration test directory and helpers at `tests/integration/data-notebooks/`
    - Create `tests/integration/data-notebooks/__init__.py`
    - Create `tests/integration/data-notebooks/notebook_test_helpers.py` with shared utilities (create test notebook, read metadata, find bundle)
    - Create test manifest files (`manifest.yaml`, `manifest-notebooks-disabled.yaml`)
    - _Requirements: 1.1, 5.2, 9.6_

  - [ ]* 11.2 Create integration test `tests/integration/data-notebooks/test_notebook_round_trip.py`
    - Follow `test_catalog_round_trip.py` pattern, extend `IntegrationTestBase`
    - Test full round-trip: bundle with `notebook_ids` → deploy to target (create) → verify metadata/params → deploy again (update in-place) → verify run history preserved → destroy → verify only CI/CD-managed notebooks deleted
    - Test `deployment_configuration.notebooks.disable: true` → skip
    - Test empty project (no notebooks) → valid manifest with zero entries
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.8, 5.1, 5.2, 9.2, 9.3, 9.7, 10.1, 10.2_

- [ ] 12. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties defined in the design
- Unit tests validate specific examples and edge cases
- The implementation language is Python, matching the existing codebase
- All new modules follow the existing `catalog_export.py` / `catalog_import.py` patterns
- Integration tests extend `IntegrationTestBase` and follow `test_catalog_round_trip.py` structure

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["2.1"] },
    { "id": 2, "tasks": ["2.2", "2.3", "2.4", "2.5", "2.6", "2.7", "3.1"] },
    { "id": 3, "tasks": ["3.2", "5.1"] },
    { "id": 4, "tasks": ["5.2", "5.3", "5.4", "5.5", "5.6", "6.1"] },
    { "id": 5, "tasks": ["6.2", "7.1"] },
    { "id": 6, "tasks": ["7.2", "7.3", "9.1"] },
    { "id": 7, "tasks": ["9.2", "9.3"] },
    { "id": 8, "tasks": ["10.1", "11.1"] },
    { "id": 9, "tasks": ["11.2"] }
  ]
}
```
