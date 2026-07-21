#!/usr/bin/env bash
# =============================================================================
# Await manual approval via a GitHub issue.
# =============================================================================
# Opens an approval issue and waits for an approve/deny comment. Only users
# with write/triage access to the repository (i.e. who can manage and close
# issues) may approve or deny — read-only users and non-collaborators are
# ignored. Uses only the GITHUB_TOKEN with `issues: write`.
#
# Required environment variables (set by the workflow):
#   GH_TOKEN   - GitHub token (github.token)
#   REPO       - owner/repo
#   STAGE      - stage label (e.g. TEST, PROD)
#   MODEL_ARN  - model package ARN being promoted
#   MODEL_VER  - model package version
#   RUN_URL    - URL of the triggering workflow run
#
# Optional:
#   APPROVAL_TIMEOUT_SECONDS - default 86400 (24h)
#   APPROVAL_POLL_SECONDS    - default 30
# =============================================================================
set -uo pipefail

: "${GH_TOKEN:?GH_TOKEN is required}"
: "${REPO:?REPO is required}"
: "${STAGE:?STAGE is required}"
MODEL_ARN="${MODEL_ARN:-unknown}"
MODEL_VER="${MODEL_VER:-unknown}"
RUN_URL="${RUN_URL:-}"
TIMEOUT="${APPROVAL_TIMEOUT_SECONDS:-86400}"
POLL="${APPROVAL_POLL_SECONDS:-30}"

APPROVE_RE='^(approved|approve|lgtm|yes)$'
DENY_RE='^(denied|deny|no)$'

# Create the approval issue.
body=$(cat <<EOF
Approve to deploy model package \`${MODEL_ARN}\` (version ${MODEL_VER}) to the **${STAGE}** environment.

Reply \`approved\`, \`approve\`, \`lgtm\`, or \`yes\` to proceed; \`denied\`, \`deny\`, or \`no\` to cancel.

> Only users with **write access** to this repository (who can manage/close issues) can approve. Comments from read-only users are ignored.

Workflow run: ${RUN_URL}
EOF
)

issue_url=$(gh issue create --repo "$REPO" \
  --title "Approve ${STAGE} deploy: model v${MODEL_VER}" \
  --body "$body")
issue_number=$(basename "$issue_url")
echo "Created approval issue #${issue_number}: ${issue_url}"

# Returns 0 if the given user may approve (write/triage/maintain/admin).
can_approve() {
  local user="$1" assoc="$2" role
  # Preferred: explicit permission check. role_name is one of
  # admin|maintain|write|triage|read. Anything triage+ can close issues.
  role=$(gh api "repos/${REPO}/collaborators/${user}/permission" \
    --jq '.role_name' 2>/dev/null || true)
  if [ -n "$role" ]; then
    case "$role" in
      admin|maintain|write|triage) return 0 ;;
      *) return 1 ;;
    esac
  fi
  # Fallback when the permission endpoint is not readable by the token:
  # trust the comment's author_association.
  case "$assoc" in
    OWNER|MEMBER|COLLABORATOR) return 0 ;;
    *) return 1 ;;
  esac
}

finish() {
  local msg="$1"
  gh issue comment "$issue_number" --repo "$REPO" --body "$msg" >/dev/null 2>&1 || true
  gh issue close "$issue_number" --repo "$REPO" >/dev/null 2>&1 || true
}

deadline=$(( $(date +%s) + TIMEOUT ))
declare -A seen=()   # remember comments we already evaluated

echo "Waiting for approval (timeout ${TIMEOUT}s, polling every ${POLL}s)..."
while :; do
  if [ "$(date +%s)" -ge "$deadline" ]; then
    finish "⏱️ Approval timed out after ${TIMEOUT}s. Cancelling ${STAGE} deploy."
    echo "Timed out waiting for approval."
    exit 1
  fi

  # Oldest-first so the first valid decision wins.
  while IFS=$'\t' read -r cid login assoc raw; do
    [ -n "$cid" ] || continue
    [ -n "${seen[$cid]:-}" ] && continue
    seen[$cid]=1

    decision=$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]' | tr -d '\r' \
      | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')

    if printf '%s' "$decision" | grep -Eq "$APPROVE_RE"; then
      if can_approve "$login" "$assoc"; then
        finish "✅ Approved by @${login}. Proceeding with ${STAGE} deploy."
        echo "Approved by ${login}."
        exit 0
      else
        gh issue comment "$issue_number" --repo "$REPO" \
          --body "⚠️ @${login} does not have write access; approval ignored." >/dev/null 2>&1 || true
        echo "Ignoring approval from ${login} (insufficient access)."
      fi
    elif printf '%s' "$decision" | grep -Eq "$DENY_RE"; then
      if can_approve "$login" "$assoc"; then
        finish "🛑 Denied by @${login}. Cancelling ${STAGE} deploy."
        echo "Denied by ${login}."
        exit 1
      else
        echo "Ignoring denial from ${login} (insufficient access)."
      fi
    fi
  done < <(gh api --paginate "repos/${REPO}/issues/${issue_number}/comments" \
             --jq '.[] | [(.id|tostring), .user.login, .author_association, (.body|gsub("[\r\n]+";" "))] | @tsv' \
             2>/dev/null)

  sleep "$POLL"
done
