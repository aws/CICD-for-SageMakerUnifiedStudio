#!/bin/bash
# Setup integration test environment for SMUS CI/CD
# This script creates a virtual environment and configures environment variables

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$PROJECT_ROOT/venv"
ENV_FILE="$PROJECT_ROOT/.env"

echo "🚀 SMUS CI/CD Integration Test Environment Setup"
echo "================================================"
echo ""

# Check Python version
PYTHON_CMD=""
for cmd in python3.14 python3.13 python3.12 python3.11 python3.10 python3.9 python3.8 python3.7 python3 python; do
    if command -v $cmd &> /dev/null; then
        VERSION=$($cmd --version 2>&1 | grep -oE '[0-9]+\.[0-9]+' | head -1)
        MAJOR=$(echo $VERSION | cut -d. -f1)
        MINOR=$(echo $VERSION | cut -d. -f2)
        if [ "$MAJOR" -eq 3 ] && [ "$MINOR" -ge 7 ]; then
            PYTHON_CMD=$cmd
            echo "✅ Found Python $VERSION at $(which $cmd)"
            break
        fi
    fi
done

if [ -z "$PYTHON_CMD" ]; then
    echo "❌ Error: Python 3.7 or higher is required"
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo ""
    echo "📦 Creating virtual environment..."
    $PYTHON_CMD -m venv "$VENV_DIR"
    echo "✅ Virtual environment created at: $VENV_DIR"
else
    echo ""
    echo "✅ Virtual environment already exists at: $VENV_DIR"
fi

# Activate virtual environment
echo ""
echo "🔌 Activating virtual environment..."
source "$VENV_DIR/bin/activate"

# Upgrade pip
echo ""
echo "⬆️  Upgrading pip..."
pip install --upgrade pip --quiet

# Install package in development mode
echo ""
echo "📦 Installing SMUS CI/CD package in development mode..."
pip install -e "$PROJECT_ROOT[dev]" --quiet
echo "✅ Package installed"

# Get AWS configuration
echo ""
echo "🔍 Detecting AWS configuration..."
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "")
if [ -z "$AWS_ACCOUNT_ID" ]; then
    echo "⚠️  Warning: Unable to get AWS account ID. Please configure AWS credentials."
    echo "   Run: aws configure"
    AWS_ACCOUNT_ID="YOUR_ACCOUNT_ID"
fi

AWS_REGION=${AWS_DEFAULT_REGION:-us-east-1}

# Detect DataZone domain
echo ""
echo "🔍 Detecting DataZone domain..."
DOMAIN_NAME=""
DOMAIN_ID=""

# Try to find domain with tag purpose=smus-cicd-testing
if [ -n "$AWS_ACCOUNT_ID" ] && [ "$AWS_ACCOUNT_ID" != "YOUR_ACCOUNT_ID" ]; then
    DOMAINS=$(aws datazone list-domains --region $AWS_REGION --output json 2>/dev/null || echo '{"items":[]}')
    
    # Try to find domain with matching tag
    for domain_id in $(echo "$DOMAINS" | jq -r '.items[].id // empty'); do
        TAGS=$(aws datazone list-tags-for-resource --resource-arn "arn:aws:datazone:$AWS_REGION:$AWS_ACCOUNT_ID:domain/$domain_id" --region $AWS_REGION --output json 2>/dev/null || echo '{"tags":{}}')
        PURPOSE=$(echo "$TAGS" | jq -r '.tags.purpose // empty')
        
        if [ "$PURPOSE" = "smus-cicd-testing" ]; then
            DOMAIN_ID=$domain_id
            DOMAIN_NAME=$(echo "$DOMAINS" | jq -r ".items[] | select(.id==\"$domain_id\") | .name")
            echo "✅ Found domain with tag purpose=smus-cicd-testing"
            break
        fi
    done
    
    # If no tagged domain found, check if there's only one domain
    if [ -z "$DOMAIN_ID" ]; then
        DOMAIN_COUNT=$(echo "$DOMAINS" | jq '.items | length')
        if [ "$DOMAIN_COUNT" -eq 1 ]; then
            DOMAIN_ID=$(echo "$DOMAINS" | jq -r '.items[0].id')
            DOMAIN_NAME=$(echo "$DOMAINS" | jq -r '.items[0].name')
            echo "✅ Found single domain in region"
        elif [ "$DOMAIN_COUNT" -gt 1 ]; then
            echo "⚠️  Multiple domains found. Please set DATAZONE_DOMAIN_NAME manually."
            DOMAIN_NAME="YOUR_DOMAIN_NAME"
            DOMAIN_ID=""
        else
            echo "⚠️  No domains found. Please create a domain or set DATAZONE_DOMAIN_NAME manually."
            DOMAIN_NAME="YOUR_DOMAIN_NAME"
            DOMAIN_ID=""
        fi
    fi
else
    DOMAIN_NAME="YOUR_DOMAIN_NAME"
    DOMAIN_ID=""
fi

# Detect projects
DEV_PROJECT_NAME="dev-marketing"
TEST_PROJECT_NAME="test-marketing"
DEV_PROJECT_ID=""
TEST_PROJECT_ID=""

if [ -n "$DOMAIN_ID" ] && [ "$DOMAIN_ID" != "" ]; then
    echo ""
    echo "🔍 Detecting DataZone projects..."
    
    PROJECTS=$(aws datazone list-projects --domain-identifier "$DOMAIN_ID" --region $AWS_REGION --output json 2>/dev/null || echo '{"items":[]}')
    
    DEV_PROJECT_ID=$(echo "$PROJECTS" | jq -r ".items[] | select(.name==\"$DEV_PROJECT_NAME\") | .id // empty")
    TEST_PROJECT_ID=$(echo "$PROJECTS" | jq -r ".items[] | select(.name==\"$TEST_PROJECT_NAME\") | .id // empty")
    
    if [ -n "$DEV_PROJECT_ID" ]; then
        echo "✅ Found dev project: $DEV_PROJECT_NAME ($DEV_PROJECT_ID)"
    fi
    
    if [ -n "$TEST_PROJECT_ID" ]; then
        echo "✅ Found test project: $TEST_PROJECT_NAME ($TEST_PROJECT_ID)"
    fi
fi

# Create .env file
echo ""
echo "📝 Creating .env file..."
cat > "$ENV_FILE" << EOF
# SMUS CI/CD Environment Variables
# Generated on $(date)
# Source this file: source .env

# AWS Configuration
export AWS_ACCOUNT_ID=$AWS_ACCOUNT_ID
export AWS_DEFAULT_REGION=$AWS_REGION
export DEV_DOMAIN_REGION=$AWS_REGION
export TEST_DOMAIN_REGION=$AWS_REGION

# DataZone Configuration
export DATAZONE_DOMAIN_NAME=$DOMAIN_NAME
export DATAZONE_DOMAIN_ID=${DOMAIN_ID:-}
export DOMAIN_NAME=$DOMAIN_NAME

# DataZone Projects
export DATAZONE_PROJECT_NAME_DEV=$DEV_PROJECT_NAME
export DATAZONE_PROJECT_NAME_TEST=$TEST_PROJECT_NAME
export DATAZONE_PROJECT_ID_DEV=${DEV_PROJECT_ID:-}
export DATAZONE_PROJECT_ID_TEST=${TEST_PROJECT_ID:-}

# MLflow Configuration (optional)
# export MLFLOW_TRACKING_SERVER_ARN=arn:aws:sagemaker:$AWS_REGION:$AWS_ACCOUNT_ID:mlflow-tracking-server/YOUR_SERVER_NAME
export MLFLOW_TRACKING_SERVER_NAME=smus-integration-mlflow

# Optional Service Endpoints (for testing against specific endpoints)
# export DATAZONE_ENDPOINT_URL=https://datazone.$AWS_REGION.amazonaws.com
# export AWS_ENDPOINT_URL_DATAZONE=https://datazone.$AWS_REGION.amazonaws.com
# export AIRFLOW_SERVERLESS_ENDPOINT=https://airflow-serverless.$AWS_REGION.api.aws/

# Debug mode (uncomment to enable)
# export SMUS_DEBUG=1
EOF

# Source the .env file
source "$ENV_FILE"

echo "✅ Environment file created at: $ENV_FILE"

# Create activation script
ACTIVATE_SCRIPT="$PROJECT_ROOT/activate-integ-env.sh"
cat > "$ACTIVATE_SCRIPT" << 'EOF'
#!/bin/bash
# Activate SMUS CI/CD integration test environment
# Usage: source activate-integ-env.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Activate virtual environment
if [ -f "$SCRIPT_DIR/venv/bin/activate" ]; then
    source "$SCRIPT_DIR/venv/bin/activate"
    echo "✅ Virtual environment activated"
else
    echo "❌ Virtual environment not found. Run ./tests/setup-integ-env.sh first"
    return 1
fi

# Load environment variables
if [ -f "$SCRIPT_DIR/.env" ]; then
    source "$SCRIPT_DIR/.env"
    echo "✅ Environment variables loaded"
else
    echo "⚠️  .env file not found. Run ./tests/setup-integ-env.sh first"
fi

echo ""
echo "🚀 SMUS CI/CD integration test environment ready!"
echo ""
echo "Available commands:"
echo "  smus-cli --help              # Show CLI help"
echo "  pytest tests/unit/ -v        # Run unit tests"
echo "  pytest tests/integration/ -v # Run integration tests"
echo "  python tests/run_tests.py    # Run tests with parallel support"
echo ""
echo "Environment variables:"
echo "  AWS_ACCOUNT_ID: $AWS_ACCOUNT_ID"
echo "  AWS_DEFAULT_REGION: $AWS_DEFAULT_REGION"
echo "  DATAZONE_DOMAIN_NAME: $DATAZONE_DOMAIN_NAME"
echo "  DATAZONE_DOMAIN_ID: ${DATAZONE_DOMAIN_ID:-'(not set)'}"
echo ""
EOF

chmod +x "$ACTIVATE_SCRIPT"

echo ""
echo "✅ Activation script created at: $ACTIVATE_SCRIPT"

# Summary
echo ""
echo "================================================"
echo "✅ Setup Complete!"
echo "================================================"
echo ""
echo "📋 Summary:"
echo "  • Virtual environment: $VENV_DIR"
echo "  • Environment file: $ENV_FILE"
echo "  • Activation script: $ACTIVATE_SCRIPT"
echo ""
echo "🔧 Configuration:"
echo "  • AWS Account: $AWS_ACCOUNT_ID"
echo "  • AWS Region: $AWS_REGION"
echo "  • DataZone Domain: $DOMAIN_NAME"
echo "  • DataZone Domain ID: ${DOMAIN_ID:-'(not detected)'}"
echo "  • Dev Project: $DEV_PROJECT_NAME ${DEV_PROJECT_ID:+($DEV_PROJECT_ID)}"
echo "  • Test Project: $TEST_PROJECT_NAME ${TEST_PROJECT_ID:+($TEST_PROJECT_ID)}"
echo ""

if [ "$DOMAIN_NAME" = "YOUR_DOMAIN_NAME" ] || [ "$AWS_ACCOUNT_ID" = "YOUR_ACCOUNT_ID" ]; then
    echo "⚠️  Manual Configuration Required:"
    echo ""
    echo "  Edit $ENV_FILE and update:"
    if [ "$AWS_ACCOUNT_ID" = "YOUR_ACCOUNT_ID" ]; then
        echo "    • AWS_ACCOUNT_ID"
    fi
    if [ "$DOMAIN_NAME" = "YOUR_DOMAIN_NAME" ]; then
        echo "    • DATAZONE_DOMAIN_NAME"
        echo "    • DATAZONE_DOMAIN_ID (optional, can be auto-discovered)"
    fi
    echo ""
    echo "  Then run: source $ACTIVATE_SCRIPT"
    echo ""
else
    echo "📝 Next Steps:"
    echo ""
    echo "  1. Review and edit $ENV_FILE if needed"
    echo "  2. Activate the environment:"
    echo "     source activate-integ-env.sh"
    echo ""
    echo "  3. Run tests:"
    echo "     pytest tests/unit/ -v"
    echo "     pytest tests/integration/basic_app/ -v"
    echo ""
fi

echo "📚 Documentation:"
echo "  • Developer Guide: developer/developer-guide.md"
echo "  • AI Assistant Context: developer/AmazonQ.md"
echo "  • Running Tests: tests/RUNNING_TESTS.md"
echo ""
