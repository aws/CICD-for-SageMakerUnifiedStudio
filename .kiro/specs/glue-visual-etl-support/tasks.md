# Implementation Plan: Glue Visual ETL Support

## Overview

Implements end-to-end promotion of Glue **Visual ETL** jobs (`JobMode: VISUAL`) across environments in the SMUS CI/CD CLI: `bundle` (export), `deploy` (create/update with UI-visibility tags and target rewrites), `destroy`, and `deploy --dry-run`. Follows the catalog import/export architecture with two new helper modules (`visual_etl_export.py`, `visual_etl_import.py`), manifest configuration extensions, and integration into the `bundle`, `deploy`, `destroy`, and dry-run flows.

## Tasks

- [ ] 1. Extend manifest configuration and resource types
  - [ ] 1.1 Add GlueJobEntry and `content.glue_jobs` list, plus `deployment_configuration.glue_jobs`, in `application_manifest.py`
    - Add `GlueJobEntry` dataclass (`name`, `source`, `target_name`)
    - Add `glue_jobs: List[GlueJobEntry] = field(default_factory=list)` to `ContentConfig`
    - Add `glue_jobs: Optional[Dict[str, Any]] = None` to `DeploymentConfiguration` (`disable`, `target_suffix`, `overrides`)
    - Parse `content.glue_jobs` (YAML `targetName` → `target_name`) and `deployment_configuration.glue_jobs`
    - Validate `name` pattern `[a-zA-Z0-9_-]{1,255}` and unique `(name, source)`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8_

  - [ ] 1.2 Add `glue_job` to `DESTROY_SUPPORTED_RESOURCE_TYPES` in `destroy_models.py`
    - `glue_job` is already in `DEPLOY_RESOURCE_TYPES` — add it to the destroy set
    - Ensure `TestDeployDestroyDrift` passes
    - _Requirements: 9.10_

  - [ ]* 1.3 Write property test for entry uniqueness and pattern (Property 2)
    - **Validates: Requirements 1.2, 1.7, 1.8**

- [ ] 2. Implement the export module
  - [ ] 2.1 Create `src/smus_cicd/helpers/visual_etl_export.py`
    - `export_visual_etl_jobs()`, `_validate_entries()` (fail-fast, VISUAL-only), `_locate_artifacts()`, `_copy_artifact()`, `_sanitize_definition()`, `_build_export_manifest()`
    - Define `ExportedVisualEtlJob`, `VisualEtlExportSummary`
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8_

  - [ ]* 2.2 Property test: fail-fast validation completeness (Property 1) — **Validates: 2.1, 2.2, 2.3**
  - [ ]* 2.3 Property test: export manifest schema (Property 3) — **Validates: 4.2, 4.3, 4.4, 4.7**
  - [ ]* 2.4 Property test: definition sanitization (Property 4) — **Validates: 4.5, 4.6, 4.8**
  - [ ]* 2.5 Property test: visual artifact requirement (Property 6) — **Validates: 3.2, 3.4**
  - [ ]* 2.6 Unit tests `tests/unit/helpers/test_visual_etl_export.py`
    - Fail-fast missing/non-visual, VISUAL happy path, missing `.json`, missing `.vetl`, sanitization, `source` default
    - _Requirements: 2.2, 2.3, 3.1, 3.2, 3.6, 4.5, 4.6_

- [ ] 3. Integrate export into the bundle command
  - [ ] 3.1 Modify `src/smus_cicd/commands/bundle.py` to call `export_visual_etl_jobs()` when `content.glue_jobs` is non-empty
    - After catalog export; skip (no `glue_jobs/` dir) when absent/empty
    - Write files under `glue_jobs/<name>/` and `glue_jobs_export_manifest.json` into the ZIP
    - Report summary; non-zero exit on failures
    - _Requirements: 1.6, 3.3, 3.5_

  - [ ]* 3.2 Property test: bundle internal consistency (Property 5) — **Validates: 4.7**

- [ ] 4. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 5. Implement the import/deploy module
  - [ ] 5.1 Create `src/smus_cicd/helpers/visual_etl_import.py`
    - `deploy_visual_etl_jobs()` (4-step per-job sequence), `_validate_export_manifest()`, `_upload_artifacts()`, `_deployed_name()`, `_build_target_kwargs()`, `_rewrite_default_arguments()`, `_resolve_target_job()`, `_owns_job()`, `_create_or_update()`
    - Define `GlueJobDeploySummary`, `DeployResult`, `DeployStatus`
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8, 6.9, 6.10, 6.11, 7.1, 7.2, 7.3, 7.4, 7.5, 8.1, 8.2, 8.3, 8.4_

  - [ ]* 5.2 Property test: manifest validation before API calls — **Validates: 6.1 (manifest handling)**
  - [ ]* 5.3 Property test: target kwargs construction invariants (Property 7) — **Validates: 6.2, 6.3, 6.4, 6.5**
  - [ ]* 5.4 Property test: ownership guard (Property 8) — **Validates: 7.2, 7.3, 7.4**
  - [ ]* 5.5 Property test: deployed name and override application (Property 9) — **Validates: 6.7, 6.11**
  - [ ]* 5.6 Property test: summary count invariant (Property 11) — **Validates: 3.5, 6.10**
  - [ ]* 5.7 Unit tests `tests/unit/helpers/test_visual_etl_import.py`
    - Create path, update path, ownership guard, overrides, deployed name, missing file, S3 connection missing, manifest validation
    - _Requirements: 6.2, 6.3, 6.4, 6.5, 6.7, 6.8, 6.9, 7.2, 7.3, 7.4, 8.2, 8.3, 8.4_

- [ ] 6. Integrate deploy into the deploy command
  - [ ] 6.1 Modify `src/smus_cicd/commands/deploy.py` to call `deploy_visual_etl_jobs()` after catalog import
    - Add `_deploy_glue_jobs_from_bundle()` following `_import_catalog_from_bundle()`
    - Honor `deployment_configuration.glue_jobs.disable` (skip w/ message), skip silently if no manifest in bundle
    - Extract manifest + files; resolve `default.s3_shared` S3 URI and target IAM role ARN; pass `target_suffix`/`overrides`
    - Report summary; non-zero exit on failures
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 6.10, 6.11_

  - [ ]* 6.2 Unit tests `tests/unit/commands/test_deploy_glue_jobs.py`
    - disable → skip; no manifest → skip; happy path → summary; failures → non-zero
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_

- [ ] 7. Implement destroy support
  - [ ] 7.1 Add discovery/deletion to `destroy_validator.py` and `destroy_executor.py`
    - `_discover_glue_jobs()`: list by `AmazonDataZoneProject` tag → filter by `smus-cicd-source-job-name` → filter by configured source names
    - `_delete_glue_job()`: DeleteJob, handle EntityNotFoundException (not_found) + other errors
    - Display under `glue_job` resource type; report deleted/not_found/error
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 9.8, 9.9_

  - [ ]* 7.2 Property test: destroy tag-based filtering (Property 10) — **Validates: 9.3, 9.4**
  - [ ]* 7.3 Unit tests `tests/unit/commands/test_glue_job_destroy.py`
    - Filter by tag, respect configured names, source env → zero, EntityNotFoundException/other errors, list failure
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.6, 9.7, 9.8, 9.9_

- [ ] 8. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 9. Implement dry-run checker
  - [ ] 9.1 Create `src/smus_cicd/commands/dry_run/checkers/glue_job_checker.py`
    - `GlueJobChecker` following `catalog_checker.py`: S3 reachable (HEAD), IAM perms (glue:GetJob/CreateJob/UpdateJob/TagResource/GetTags, s3:PutObject), jobCount, VISUAL-without-vetl warning
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6_

  - [ ] 9.2 Register `GlueJobChecker` in the dry-run engine (`engine.py`) after the catalog checker; invoke only when the bundle has `glue_jobs/glue_jobs_export_manifest.json`
    - _Requirements: 10.1, 10.2, 10.3_

  - [ ]* 9.3 Unit tests `tests/unit/commands/test_glue_job_dry_run.py`
    - S3 reachable/missing, IAM denied, jobCount reported, VISUAL-without-vetl warning
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6_

- [ ] 10. Consolidate property-based tests
  - [ ] 10.1 Create `tests/unit/helpers/test_glue_job_properties.py` aggregating all 11 property tests
    - From tasks 1.3, 2.2-2.5, 3.2, 5.2-5.6, 7.2, following `test_catalog_export_properties.py`
    - Each uses `@settings(max_examples=100)`; tag `# Feature: glue-visual-etl-support, Property {N}: {description}`
    - _Requirements: All correctness properties P1-P11_

- [ ] 11. Example and integration test
  - [ ] 11.1 Create example at `examples/analytic-workflow/glue-visual-etl/`
    - `manifest.yaml` with `content.glue_jobs` and per-stage `deployment_configuration.glue_jobs`
    - `README.md` documenting the three UI-visibility elements, source-tag tracking, destroy semantics, and IAM/IdC prerequisites (Requirement 11)
    - Sample source job files (`.py`, `.vetl`, `.json`) for a representative Visual ETL job
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6_

  - [ ] 11.2 Create integration helpers at `tests/integration/glue-visual-etl/`
    - `__init__.py`, `glue_job_test_helpers.py` (seed job, read tags, find bundle), test manifests (incl. disabled)
    - _Requirements: 1.1, 5.2, 9.5_

  - [ ]* 11.3 Integration test `tests/integration/glue-visual-etl/test_glue_job_round_trip.py`
    - Extend `IntegrationTestBase`, follow `test_catalog_round_trip.py`
    - Round-trip: bundle → deploy (create) → verify tags + JobMode + smus-orig-asset → deploy again (update) → override → destroy → verify only CI/CD-managed job deleted
    - `disable: true` → skip; `JobMode: SCRIPT` configured → bundle fails
    - _Requirements: 5.2, 6.2, 6.3, 6.4, 6.5, 6.7, 7.2, 9.3, 9.6_

- [ ] 12. Documentation
  - [ ] 12.1 Add `docs/glue-visual-etl.md`
    - Cover `content.glue_jobs` and `deployment_configuration.glue_jobs`, the three UI-visibility elements, `smus-cicd-source-job-name` tracking, destroy semantics, `<TARGET_REWRITE>` sanitization, and IAM/IdC prerequisites
    - Link from `README.md` and `docs/examples-guide.md`; add a brief reminder in the deploy command help when Glue jobs are involved
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6_

- [ ] 13. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Only `JobMode: VISUAL` jobs are handled; script jobs are rejected at validation (future work)
- All new modules follow the existing `catalog_export.py` / `catalog_import.py` patterns
- Integration tests extend `IntegrationTestBase` and follow `test_catalog_round_trip.py`
- The `glue_job` resource type already exists in `DEPLOY_RESOURCE_TYPES`; this feature adds the destroy side and the deletion logic

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["1.3", "2.1"] },
    { "id": 2, "tasks": ["2.2", "2.3", "2.4", "2.5", "2.6", "3.1"] },
    { "id": 3, "tasks": ["3.2", "5.1"] },
    { "id": 4, "tasks": ["5.2", "5.3", "5.4", "5.5", "5.6", "5.7", "6.1"] },
    { "id": 5, "tasks": ["6.2", "7.1"] },
    { "id": 6, "tasks": ["7.2", "7.3", "9.1"] },
    { "id": 7, "tasks": ["9.2", "9.3"] },
    { "id": 8, "tasks": ["10.1", "11.1", "11.2"] },
    { "id": 9, "tasks": ["11.3", "12.1"] }
  ]
}
```
