#!/bin/bash
# ABOUTME: Quick deployment script for Lambda handler updates only
# ABOUTME: Use this when only handler.py changes (skips AMI rebuild)

set -euo pipefail

# Configuration
AWS_REGION="${AWS_REGION:-us-east-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Log function
log() {
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] $*"
}

# Error handler
error() {
    log "ERROR: $*"
    exit 1
}

log "Quick Lambda update (handler.py changes only)"
log "=========================================="
log ""

# Step 1: Package Lambda
log "Step 1/3: Packaging Lambda function..."
cd "$SCRIPT_DIR/lambda"

rm -f lambda.zip
zip -q lambda.zip handler.py requirements.txt || error "Failed to package Lambda"

log "✓ Lambda packaged: $(du -h lambda.zip | cut -f1)"
log ""

# Step 2: Deploy with Terraform
log "Step 2/3: Deploying with Terraform..."
cd "$SCRIPT_DIR/terraform"

if [ ! -d .terraform ]; then
    log "Initializing Terraform..."
    terraform init || error "Terraform init failed"
fi

# Apply (Terraform will detect the lambda.zip change and update the function)
log "Applying changes..."
terraform apply -auto-approve || error "Terraform apply failed"

log "✓ Lambda functions updated"
log ""

# Step 3: Verify deployment
log "Step 3/3: Verifying deployment..."

LAMBDA_FUNCTION=$(terraform output -raw lambda_function_name)
LATEST_VERSION=$(aws lambda get-function --function-name "$LAMBDA_FUNCTION" --region "$AWS_REGION" --query 'Configuration.Version' --output text)

log "✓ Lambda function: $LAMBDA_FUNCTION"
log "✓ Latest version: $LATEST_VERSION"
log ""

log "=========================================="
log "Lambda Update Complete!"
log "=========================================="
log ""
log "The SerenDB target verification is now active."
log "Remote execution is restricted to serendb.com and console.serendb.com targets."
log ""
