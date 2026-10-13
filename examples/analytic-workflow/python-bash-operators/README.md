# Python / Bash Operators Example

This example deploys two MWAA Serverless workflows that run **custom operator
code** — one using `PythonOperator` (a pre-built `.zip` of two modules) and one
using `BashOperator` (a single `.sh` file).

It demonstrates the per-workflow `code` field: the operator code is bundled,
uploaded to S3 on deploy, and passed to MWAA Serverless as the
`CreateWorkflow`/`UpdateWorkflow` `Code` parameter. See the AWS docs for
[PythonOperator and BashOperator on MWAA Serverless](https://docs.aws.amazon.com/mwaa/latest/mwaa-serverless-userguide/operators-python-bash-detail.html)
and [uploading operator files in SageMaker Unified Studio](https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/serverless-visual-workflows.html#serverless-python-bash-operators).

## Layout

```
python-bash-operators/
├── manifest.yaml                         # dev/test/prod stages, 2 workflows
├── workflows/
│   ├── python_operator_workflow.yaml     # DAG using PythonOperator
│   └── bash_operator_workflow.yaml       # DAG using BashOperator
├── code/
│   ├── python/
│   │   ├── greeting.py                    # entry module (imports helpers)
│   │   ├── helpers.py                     # second module
│   │   └── hello_package.zip              # pre-built: greeting.py + helpers.py
│   └── bash/
│       └── greet.sh                       # single-file BashOperator script
└── app_tests/
    └── test_operators_ran.py             # asserts both runs reach SUCCESS
```

## How the `code` field works

Each workflow entry names its operator code file:

```yaml
workflows:
- workflowName: python_operator_workflow
  connectionName: default.workflow_serverless
  code: code/python/hello_package.zip      # a pre-built .zip (2 modules)
- workflowName: bash_operator_workflow
  connectionName: default.workflow_serverless
  code: code/bash/greet.sh                 # a single .sh file
```

The `code` value must be a single **`.py`**, **`.sh`**, or **`.zip`** file that
is included in the bundle via `content.storage`. On deploy the CLI uploads it to
S3 **as-is** (no repackaging) under the **`operatorFiles/`** prefix — the same
location the SageMaker Unified Studio console uses —
as `operatorFiles/<workflow_name>-<timestamp>.<ext>`. The DAG YAML continues to
be sent as `DefinitionS3Location`; the two are distinct S3 objects and are
versioned together by MWAA Serverless.

### Multiple modules or third-party dependencies

The CLI does **not** zip for you — it uploads the `.zip` verbatim. This example
ships `hello_package.zip` containing two modules (`greeting.py` imports
`helpers.py`), both at the archive root, so the run only succeeds if the whole
archive is replicated and unpacked correctly.

To rebuild the zip after editing the modules:

```bash
cd code/python
zip -j hello_package.zip greeting.py helpers.py -x "*__pycache__*" "*.pyc"
```

To include third-party dependencies, follow the MWAA Serverless docs and stage
them at the archive root (Linux x86_64 / Python 3.12 wheels):

```bash
mkdir my_package
cp *.py my_package/
pip install -r requirements.txt --target my_package \
    --platform manylinux2014_x86_64 --python-version 3.12 --only-binary=:all:
cd my_package && zip -r ../my_package.zip . -x "*__pycache__*" "*.pyc"
```

## Deploy

This example is deployed by the `analytic-python-bash-operators.yml` GitHub
workflow, which calls the reusable `smus-bundle-deploy.yml` pipeline (bundle
from `dev` → deploy to `test` → run tests → deploy to `prod`, then destroy the
test environment).

To run locally against a target:

```bash
aws-smus-cicd-cli deploy \
  --manifest examples/analytic-workflow/python-bash-operators/manifest.yaml \
  --targets test
```

Use `--dry-run` first to validate that each workflow's `code` file is present in
the bundle before deploying.
