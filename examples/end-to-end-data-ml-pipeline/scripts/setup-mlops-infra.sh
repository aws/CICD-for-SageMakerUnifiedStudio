#!/usr/bin/env bash
# =============================================================================
# MLOps Infrastructure Setup
# =============================================================================
# One-time setup script that provisions the infrastructure needed for the
# MLOps pipeline: event-based deploy trigger.
#
# Usage:
#   ./scripts/setup-mlops-infra.sh <dev|test|prod> <account-id> <region> <project-name>
#
# Examples:
#   ./scripts/setup-mlops-infra.sh dev 973414239152 us-east-1 bank-mktg-dev
#   ./scripts/setup-mlops-infra.sh prod 987654321098 us-west-2 bank-mktg-prod
#
# What it creates:
#   1. Deploy Trigger (Lambda function, EventBridge rule, IAM role)
#
# Prerequisites:
#   - AWS credentials for the target account
#   - Project deployed with aws-smus-cicd-cli
# =============================================================================

set -euo pipefail
export AWS_PAGER=""

ENV="${1:?Usage: $0 <dev|test|prod> <account-id> <region> <project-name>}"
ACCOUNT_ID="${2:?Usage: $0 <dev|test|prod> <account-id> <region> <project-name>}"
REGION="${3:?Usage: $0 <dev|test|prod> <account-id> <region> <project-name>}"
PROJECT_NAME="${4:?Usage: $0 <dev|test|prod> <account-id> <region> <project-name>}"

echo "╔══════════════════════════════════════════════════════════╗"
echo "║  MLOps Infrastructure Setup                              ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  Environment : $ENV"
echo "║  Account     : $ACCOUNT_ID"
echo "║  Region      : $REGION"
echo "║  Project     : $PROJECT_NAME"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# Verify we're in the right account
CURRENT_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
if [ "$CURRENT_ACCOUNT" != "$ACCOUNT_ID" ]; then
  echo "ERROR: Authenticated to account $CURRENT_ACCOUNT but expected $ACCOUNT_ID"
  exit 1
fi

# =============================================================================
# PART 1: Event-Based Deploy Trigger
# =============================================================================
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Event-Based Deploy Trigger"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

FUNCTION_NAME="bank-mktg-airflow-deploy-trigger"
RULE_NAME="bank-mktg-model-approved"
LAMBDA_ROLE_NAME="bank-mktg-airflow-deploy-trigger-role"
MODEL_PACKAGE_GROUP="bank-mktg-prediction-models"

# Step 1.1: Create Lambda execution role
echo ""
echo "[1.1] Lambda execution role: $LAMBDA_ROLE_NAME..."

LAMBDA_TRUST=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "lambda.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
EOF
)

if aws iam get-role --role-name "$LAMBDA_ROLE_NAME" &>/dev/null; then
  echo "  Role exists."
else
  aws iam create-role \
    --role-name "$LAMBDA_ROLE_NAME" \
    --assume-role-policy-document "$LAMBDA_TRUST" \
    --description "Lambda role for Airflow deploy trigger" > /dev/null
  echo "  Created."
fi

LAMBDA_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "Logs",
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "arn:aws:logs:${REGION}:${ACCOUNT_ID}:*"
    },
    {
      "Sid": "SecretsManager",
      "Effect": "Allow",
      "Action": ["secretsmanager:GetSecretValue"],
      "Resource": "arn:aws:secretsmanager:${REGION}:${ACCOUNT_ID}:secret:bank-mktg/github-token*"
    }
  ]
}
EOF
)

aws iam put-role-policy \
  --role-name "$LAMBDA_ROLE_NAME" \
  --policy-name "AirflowDeployTriggerPolicy" \
  --policy-document "$LAMBDA_POLICY"
echo "  Policy attached."

echo "  Waiting for role propagation..."
sleep 10

LAMBDA_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${LAMBDA_ROLE_NAME}"

# Step 1.2: Create Lambda function
echo "[1.2] Lambda function: $FUNCTION_NAME..."

LAMBDA_DIR=$(mktemp -d)
cat > "$LAMBDA_DIR/lambda_function.py" << 'PYEOF'
"""Lambda: Trigger GitHub Actions deploy workflow on model approval.

Triggered by EventBridge when a model is approved in the registry.
Sends a repository_dispatch event to GitHub Actions which then triggers
the dev -> test -> prod promotion cascade (mlops-promote.yml).

Environment Variables:
  GITHUB_TOKEN_SECRET_ARN - Secrets Manager ARN for GitHub PAT
  GITHUB_REPO             - GitHub repo (owner/repo format)
  PROJECT_NAME            - Project name to derive stage (e.g., bank-mktg-dev → dev)
"""
import json
import logging
import os
import urllib.request
import urllib.error
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_github_token():
    """Retrieve GitHub PAT from Secrets Manager."""
    secret_arn = os.environ["GITHUB_TOKEN_SECRET_ARN"]
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_arn)
    return response["SecretString"]


def handler(event, context):
    logger.info(f"Event: {json.dumps(event)}")

    detail = event.get("detail", {})
    approval_status = detail.get("ModelApprovalStatus", "")
    updated_fields = detail.get("UpdatedModelPackageFields", []) or []

    if approval_status != "Approved":
        logger.info(f"Ignoring non-approval event: {approval_status}")
        return {"statusCode": 200, "body": "Skipped - not an approval"}

    # Loop protection. The promotion workflow stamps CustomerMetadataProperties
    # on the package after each successful deploy, which re-emits a
    # "Model Package State Change" event with ModelApprovalStatus=Approved.
    # Those re-emits update ONLY CustomerMetadataProperties, whereas a genuine
    # approval changes ModelApprovalStatus. SageMaker reports what changed in
    # UpdatedModelPackageFields, so dispatch on every event that actually
    # changed the approval status and skip metadata-only stamps. This works
    # regardless of previousModelApprovalStatus (API/UI approvals omit it), so
    # every genuine approval triggers the pipeline. If UpdatedModelPackageFields
    # is absent we err toward dispatching; deploy stamps are update-model-package
    # calls that always carry ["CustomerMetadataProperties"], so the loop stays
    # protected.
    if updated_fields and "ModelApprovalStatus" not in updated_fields:
        logger.info(
            "Ignoring update that did not change approval status "
            f"(UpdatedModelPackageFields={updated_fields}). Likely a deploy "
            "metadata stamp, not a new approval."
        )
        return {"statusCode": 200, "body": "Skipped - approval status unchanged"}

    # Configuration
    github_repo = os.environ.get("GITHUB_REPO", "")
    project_name = os.environ.get("PROJECT_NAME", "bank-mktg-dev")

    # Derive stage from project name
    stage = project_name.rsplit("-", 1)[-1]

    model_package_arn = detail.get("ModelPackageArn", "")
    model_package_group = detail.get("ModelPackageGroupName", "bank-mktg-prediction-models")

    logger.info(f"Model approved: {model_package_arn}")
    logger.info(f"Triggering deploy for stage: {stage}")

    # Get GitHub token from Secrets Manager
    github_token = get_github_token()

    # Trigger GitHub Actions repository_dispatch
    url = f"https://api.github.com/repos/{github_repo}/dispatches"
    payload = {
        "event_type": "model-approved",
        "client_payload": {
            "stage": stage,
            "model_package_arn": model_package_arn,
            "model_package_group": model_package_group,
            "approval_status": "Approved",
        },
    }

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {github_token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
    }

    req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers=headers, method="POST")

    try:
        with urllib.request.urlopen(req) as response:
            status = response.status
            logger.info(f"GitHub dispatch triggered successfully (HTTP {status})")
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        logger.error(f"GitHub API error: HTTP {e.code} - {body}")
        raise RuntimeError(f"Failed to trigger GitHub Actions: HTTP {e.code} - {body}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Deploy workflow triggered",
            "stage": stage,
            "model_package_arn": model_package_arn,
            "github_repo": github_repo,
        }),
    }
PYEOF

(cd "$LAMBDA_DIR" && zip -q function.zip lambda_function.py)

# GITHUB_REPO is required so the deploy trigger dispatches model-approved
# events to the correct repository. Fail fast with a clear message if it's
# missing or not in owner/repo format.
if [[ -z "${GITHUB_REPO:-}" ]]; then
  echo "ERROR: GITHUB_REPO is not set. Export it as 'owner/repo' before running this script." >&2
  exit 1
fi
if [[ "$GITHUB_REPO" != */* ]]; then
  echo "ERROR: GITHUB_REPO must be in 'owner/repo' format (got: '$GITHUB_REPO')." >&2
  exit 1
fi
GITHUB_TOKEN_SECRET_ARN="arn:aws:secretsmanager:${REGION}:${ACCOUNT_ID}:secret:bank-mktg/github-token"

if aws lambda get-function --function-name "$FUNCTION_NAME" &>/dev/null; then
  aws lambda update-function-code \
    --function-name "$FUNCTION_NAME" \
    --zip-file "fileb://${LAMBDA_DIR}/function.zip" > /dev/null
  echo "  Updated function code."

  # Wait for code update to complete before updating configuration
  aws lambda wait function-updated --function-name "$FUNCTION_NAME" 2>/dev/null || sleep 5

  aws lambda update-function-configuration \
    --function-name "$FUNCTION_NAME" \
    --environment "Variables={GITHUB_REPO=${GITHUB_REPO},GITHUB_TOKEN_SECRET_ARN=${GITHUB_TOKEN_SECRET_ARN},PROJECT_NAME=${PROJECT_NAME}}" \
    --timeout 30 \
    --memory-size 128 > /dev/null
  echo "  Updated configuration."
else
  aws lambda create-function \
    --function-name "$FUNCTION_NAME" \
    --runtime python3.11 \
    --role "$LAMBDA_ROLE_ARN" \
    --handler lambda_function.handler \
    --zip-file "fileb://${LAMBDA_DIR}/function.zip" \
    --timeout 30 \
    --memory-size 128 \
    --environment "Variables={GITHUB_REPO=${GITHUB_REPO},GITHUB_TOKEN_SECRET_ARN=${GITHUB_TOKEN_SECRET_ARN},PROJECT_NAME=${PROJECT_NAME}}" > /dev/null
  echo "  Created."
fi

rm -rf "$LAMBDA_DIR"

LAMBDA_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"

# Step 1.3: EventBridge rule
echo "[1.3] EventBridge rule: $RULE_NAME..."

EVENT_PATTERN=$(cat <<EOF
{
  "source": ["aws.sagemaker"],
  "detail-type": ["SageMaker Model Package State Change"],
  "detail": {
    "ModelPackageGroupName": ["${MODEL_PACKAGE_GROUP}"],
    "ModelApprovalStatus": ["Approved"]
  }
}
EOF
)
# Note: previously this also required previousModelApprovalStatus != "Approved"
# at the rule level. SageMaker omits previousModelApprovalStatus from some
# state-change events (notably API-driven update-model-package calls), and
# EventBridge's content filter rejects events where the required field is
# missing - so user approvals were silently dropped.
#
# The Lambda handler still protects against the deploy-stamp loop on its own:
# it skips events whose UpdatedModelPackageFields changed only
# CustomerMetadataProperties (the post-deploy stamp) and dispatches on every
# event that actually changed ModelApprovalStatus. The rule is intentionally
# permissive: match every approval event, let the Lambda decide whether to
# dispatch.

aws events put-rule \
  --name "$RULE_NAME" \
  --event-pattern "$EVENT_PATTERN" \
  --description "Triggers Airflow deploy DAG when model is approved" \
  --state ENABLED > /dev/null
echo "  Rule created."

aws events put-targets \
  --rule "$RULE_NAME" \
  --targets "Id=airflow-deploy-trigger,Arn=${LAMBDA_ARN}" > /dev/null
echo "  Target added."

aws lambda add-permission \
  --function-name "$FUNCTION_NAME" \
  --statement-id "EventBridgeInvoke" \
  --action "lambda:InvokeFunction" \
  --principal "events.amazonaws.com" \
  --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}" 2>/dev/null || true
echo "  Lambda permission granted."

# =============================================================================
# SUMMARY
# =============================================================================
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  Setup Complete                                          ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║                                                          ║"
echo "║  Deploy Trigger                                          ║"
echo "║    Rule:      $RULE_NAME"
echo "║    Lambda:    $FUNCTION_NAME"
echo "║    GitHub:    $GITHUB_REPO"
echo "║                                                          ║"
echo "║  Flow:                                                   ║"
echo "║    Model approved → EventBridge → Lambda                 ║"
echo "║    → GitHub Actions (mlops-promote.yml)                  ║"
echo "║    → dev → test → prod cascade with manual gates         ║"
echo "║                                                          ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
echo "Prerequisites:"
echo "  1. Store a GitHub PAT in Secrets Manager:"
echo "     aws secretsmanager create-secret \\"
echo "       --name bank-mktg/github-token \\"
echo "       --secret-string 'ghp_your_token_here'"
echo ""
echo "Next steps:"
echo "  cd examples/end-to-end-data-ml-pipeline/examples/mlops-pipeline"
echo "  aws-smus-cicd-cli deploy --manifest manifest.yaml --targets $ENV"
