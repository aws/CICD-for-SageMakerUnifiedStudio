# Generate a Manifest with AI (Skill)

← [Back to Main README](../README.md)

Don't want to write `manifest.yaml` by hand? This repo ships an [Agent Skill](https://agentskills.io/) that scans your SageMaker Unified Studio project and generates a deployment manifest — and an orchestration workflow when one is needed — for you.

## What it does

You point a coding agent (Kiro, Amazon Q CLI, Claude Code, or any agent that supports the [AgentSkills](https://agentskills.io/) standard) at the skill and ask it to generate a manifest. The skill:

- Discovers your project's connections, storage layout, and workflows using your agent's AWS access.
- Produces a ready-to-review `manifest.yaml` that follows the [manifest schema](manifest-schema.md), with `${VAR}` placeholders for test/prod stages.
- Generates an MWAA orchestration workflow when your Glue jobs need one to run.

The skill is **read-only** — it never writes or changes anything in your AWS account or your files. It presents the generated content in the chat, and you save and edit the output.

## How to use it

### 1. Get the skill

Either clone this repo:

```bash
git clone https://github.com/aws/CICD-for-SageMakerUnifiedStudio.git
# the skill lives at:
#   CICD-for-SageMakerUnifiedStudio/skills/generate-bundle-manifest/
```

Or sparse-checkout just the skill folder without the full repo:

```bash
git clone --no-checkout --depth 1 https://github.com/aws/CICD-for-SageMakerUnifiedStudio.git
cd CICD-for-SageMakerUnifiedStudio
git sparse-checkout set skills/generate-bundle-manifest
git checkout
```

### 2. Add it to your agent

Copy the `skills/generate-bundle-manifest/` directory into wherever your agent loads skills from (for example, a `skills/` or `.kiro/skills/` folder in your project). The skill is a self-contained `SKILL.md` plus a bundled `references/` folder — no install step.

### 3. Make sure the agent has AWS access

The skill uses your agent's AWS CLI (or equivalent) to discover project resources, so authenticate to the account and region where your dev project lives:

```bash
aws sts get-caller-identity   # confirm you're in the right account
```

### 4. Ask the agent to generate a manifest

For example:

- *"Generate an SMUS CI/CD manifest for my project."*
- *"Bundle my project so I can promote it to test and prod."*
- *"Scan my project and create a manifest plus an orchestration workflow for my Glue jobs."*

The agent asks for (or discovers) your domain and project, then returns a `manifest.yaml` in the chat. Save it to your app directory, review the `${VAR}` placeholders, and deploy with the CLI:

```bash
# Validate the manifest and connectivity
aws-smus-cicd-cli describe --manifest manifest.yaml --connect

# Preview the deployment (no changes made)
aws-smus-cicd-cli deploy --targets test --manifest manifest.yaml --dry-run

# Deploy to test
aws-smus-cicd-cli deploy --targets test --manifest manifest.yaml
```

> **Note:** Target projects (test, prod) must already exist in SMUS — the CLI deploys into projects, it does not create them.

## Related documentation

- **[Skill source](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/tree/main/skills/generate-bundle-manifest)** - `SKILL.md` and bundled reference manifests
- **[Application Manifest](manifest.md)** - Complete YAML configuration reference
- **[Manifest Schema](manifest-schema.md)** - YAML schema validation and structure
- **[CLI Commands](cli-commands.md)** - All available commands and options
- **[Quick Start Guide](getting-started/quickstart.md)** - Deploy your first application
