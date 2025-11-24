#!/bin/bash
# ABOUTME: Automated deployment script for remote replication infrastructure
# ABOUTME: Builds AMI, packages Lambda, and deploys with Terraform

set -euo pipefail

# Configuration
AWS_REGION="${AWS_REGION:-us-east-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Log function
log() {
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] $*"
}

# Error handler
error() {
    log "ERROR: $*"
    exit 1
}

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."

    if ! command -v cargo &> /dev/null; then
        error "Rust/Cargo not found. Install from: https://rustup.rs/"
    fi

    if ! command -v packer &> /dev/null; then
        error "Packer not found. Install with: brew install packer"
    fi

    if ! command -v terraform &> /dev/null; then
        error "Terraform not found. Install with: brew install terraform"
    fi

    if ! command -v aws &> /dev/null; then
        error "AWS CLI not found. Install with: brew install awscli"
    fi

    if ! aws sts get-caller-identity &> /dev/null; then
        error "AWS credentials not configured. Run: aws configure"
    fi

    log "✓ All prerequisites satisfied"
}

# Build release binary
build_binary() {
    log "Building release binary..."
    cd "$PROJECT_ROOT"

    cargo build --release || error "Failed to build binary"

    local version
    version=$(./target/release/seren-replicator --version | awk '{print $2}')
    log "✓ Built binary version: $version"
}

# Build worker AMI
build_ami() {
    log "Building worker AMI (takes ~10 minutes)..."
    cd "$SCRIPT_DIR/ec2"

    # Export binary path for build script
    export BINARY_PATH="$PROJECT_ROOT/target/release/seren-replicator"

    ./build-ami.sh || error "Failed to build AMI"

    # Extract AMI ID from AWS
    AMI_ID=$(aws ec2 describe-images \
        --region "$AWS_REGION" \
        --owners self \
        --filters "Name=name,Values=seren-replicator-worker-*" \
        --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' \
        --output text)

    if [ -z "$AMI_ID" ] || [ "$AMI_ID" = "None" ]; then
        error "Could not retrieve AMI ID"
    fi

    log "✓ AMI created: $AMI_ID"
    echo "$AMI_ID" > "$SCRIPT_DIR/.ami_id"
}

# Package Lambda function
package_lambda() {
    log "Packaging Lambda function..."
    cd "$SCRIPT_DIR/lambda"

    rm -f lambda.zip
    zip -q lambda.zip handler.py requirements.txt || error "Failed to package Lambda"

    log "✓ Lambda packaged: $(du -h lambda.zip | cut -f1)"
}

# Deploy with Terraform
deploy_terraform() {
    log "Deploying infrastructure with Terraform..."
    cd "$SCRIPT_DIR/terraform"

    # Read AMI ID
    if [ ! -f "$SCRIPT_DIR/.ami_id" ]; then
        error "AMI ID file not found. Run AMI build first."
    fi
    local ami_id
    ami_id=$(cat "$SCRIPT_DIR/.ami_id")

    # Initialize if needed
    if [ ! -d .terraform ]; then
        log "Initializing Terraform..."
        terraform init || error "Terraform init failed"
    fi

    # Create terraform.tfvars if it doesn't exist
    if [ ! -f terraform.tfvars ]; then
        log "Creating terraform.tfvars..."
        cat > terraform.tfvars <<EOF
aws_region           = "$AWS_REGION"
project_name         = "seren-replication"
dynamodb_table_name  = "replication-jobs"
worker_ami_id        = "$ami_id"
worker_instance_type = "c5.2xlarge"
worker_iam_role_name = "seren-replication-worker"
EOF
    else
        # Update AMI ID in existing tfvars
        log "Updating AMI ID in terraform.tfvars..."
        if grep -q "worker_ami_id" terraform.tfvars; then
            sed -i.bak "s/worker_ami_id.*/worker_ami_id        = \"$ami_id\"/" terraform.tfvars
            rm -f terraform.tfvars.bak
        else
            echo "worker_ami_id        = \"$ami_id\"" >> terraform.tfvars
        fi
    fi

    # Plan
    log "Running terraform plan..."
    terraform plan -out=tfplan || error "Terraform plan failed"

    # Apply
    log "Applying Terraform configuration..."
    terraform apply tfplan || error "Terraform apply failed"

    # Get outputs
    API_ENDPOINT=$(terraform output -raw api_endpoint)
    DYNAMODB_TABLE=$(terraform output -raw dynamodb_table_name)
    LAMBDA_FUNCTION=$(terraform output -raw lambda_function_name)

    log "✓ Infrastructure deployed successfully"
    log ""
    log "Outputs:"
    log "  API Endpoint: $API_ENDPOINT"
    log "  DynamoDB Table: $DYNAMODB_TABLE"
    log "  Lambda Function: $LAMBDA_FUNCTION"

    # Save API endpoint
    echo "$API_ENDPOINT" > "$SCRIPT_DIR/.api_endpoint"
}

# Test API
test_api() {
    log "Testing API endpoint..."

    if [ ! -f "$SCRIPT_DIR/.api_endpoint" ]; then
        error "API endpoint file not found. Deploy infrastructure first."
    fi

    local api_endpoint
    api_endpoint=$(cat "$SCRIPT_DIR/.api_endpoint")

    log "Endpoint: $api_endpoint"

    # Test POST /jobs (will fail validation but should return 400, not 500)
    local response
    response=$(curl -s -w "\n%{http_code}" -X POST "$api_endpoint/jobs" \
        -H "Content-Type: application/json" \
        -d '{"command":"init","source_url":"test","target_url":"test"}')

    local http_code
    http_code=$(echo "$response" | tail -n1)
    local body
    body=$(echo "$response" | head -n-1)

    log "Response code: $http_code"
    log "Response body: $body"

    if [ "$http_code" = "201" ] || [ "$http_code" = "400" ]; then
        log "✓ API is responding correctly"
    else
        log "⚠ Unexpected response code: $http_code"
        log "This might be expected if validation fails"
    fi
}

# Main deployment flow
main() {
    log "Starting deployment of remote replication infrastructure"
    log "Region: $AWS_REGION"
    log ""

    check_prerequisites
    log ""

    build_binary
    log ""

    build_ami
    log ""

    package_lambda
    log ""

    deploy_terraform
    log ""

    test_api
    log ""

    log "=========================================="
    log "Deployment Complete!"
    log "=========================================="
    log ""
    log "Next steps:"
    log "  1. Export API endpoint:"
    log "     export SEREN_REMOTE_API=$(cat "$SCRIPT_DIR/.api_endpoint")"
    log ""
    log "  2. Test remote replication:"
    log "     cargo run --release -- init --remote \\"
    log "       --source \"postgresql://user:pass@host:5432/db\" \\"
    log "       --target \"postgresql://user:pass@host:5432/db\" \\"
    log "       --yes"
    log ""
    log "  3. Monitor in AWS Console:"
    log "     - EC2 instances: https://console.aws.amazon.com/ec2/home?region=$AWS_REGION#Instances:"
    log "     - DynamoDB: https://console.aws.amazon.com/dynamodbv2/home?region=$AWS_REGION#table?name=$DYNAMODB_TABLE"
    log "     - Lambda logs: https://console.aws.amazon.com/cloudwatch/home?region=$AWS_REGION#logsV2:log-groups/log-group/\$252Faws\$252Flambda\$252F$LAMBDA_FUNCTION"
    log ""
}

# Run main function
main "$@"
