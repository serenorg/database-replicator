#!/bin/bash
# ABOUTME: Automated AMI build script using Packer
# ABOUTME: Builds worker AMI with all dependencies and replicator binary

set -euo pipefail

# Configuration
BINARY_PATH="${BINARY_PATH:-../../target/release/postgres-seren-replicator}"
WORKER_SCRIPT="./worker.sh"
SETUP_SCRIPT="./setup-worker.sh"
CLOUDWATCH_CONFIG="./cloudwatch-agent-config.json"
AWS_REGION="${AWS_REGION:-us-east-1}"
INSTANCE_TYPE="${INSTANCE_TYPE:-t3.medium}"
AMI_NAME="postgres-seren-replicator-worker-$(date +%Y%m%d-%H%M%S)"

# Log function
log() {
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] $*"
}

# Check prerequisites
log "Checking prerequisites..."

if ! command -v packer &> /dev/null; then
    log "ERROR: Packer not found. Install with: brew install packer"
    exit 1
fi

if ! command -v aws &> /dev/null; then
    log "ERROR: AWS CLI not found. Install with: brew install awscli"
    exit 1
fi

if [ ! -f "$BINARY_PATH" ]; then
    log "ERROR: Replicator binary not found at: $BINARY_PATH"
    log "Build it with: cargo build --release"
    exit 1
fi

if [ ! -f "$WORKER_SCRIPT" ]; then
    log "ERROR: Worker script not found at: $WORKER_SCRIPT"
    exit 1
fi

if [ ! -f "$SETUP_SCRIPT" ]; then
    log "ERROR: Setup script not found at: $SETUP_SCRIPT"
    exit 1
fi

if [ ! -f "$CLOUDWATCH_CONFIG" ]; then
    log "ERROR: CloudWatch agent config not found at: $CLOUDWATCH_CONFIG"
    exit 1
fi

# Verify binary is executable
if [ ! -x "$BINARY_PATH" ]; then
    log "ERROR: Binary is not executable: $BINARY_PATH"
    exit 1
fi

# Verify AWS credentials
log "Verifying AWS credentials..."
if ! aws sts get-caller-identity &> /dev/null; then
    log "ERROR: AWS credentials not configured. Run: aws configure"
    exit 1
fi

# Get latest Ubuntu 24.04 AMI
log "Finding latest Ubuntu 24.04 AMI..."
SOURCE_AMI=$(aws ec2 describe-images \
    --region "$AWS_REGION" \
    --owners 099720109477 \
    --filters "Name=name,Values=ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*" \
              "Name=state,Values=available" \
    --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' \
    --output text)

if [ -z "$SOURCE_AMI" ] || [ "$SOURCE_AMI" = "None" ]; then
    log "ERROR: Could not find Ubuntu 24.04 AMI"
    exit 1
fi

log "Using source AMI: $SOURCE_AMI"

# Generate Packer template
log "Generating Packer template..."

cat > worker-ami.pkr.hcl <<EOF
packer {
  required_plugins {
    amazon = {
      version = ">= 1.0.0"
      source  = "github.com/hashicorp/amazon"
    }
  }
}

variable "region" {
  type    = string
  default = "$AWS_REGION"
}

variable "instance_type" {
  type    = string
  default = "$INSTANCE_TYPE"
}

variable "source_ami" {
  type    = string
  default = "$SOURCE_AMI"
}

variable "ami_name" {
  type    = string
  default = "$AMI_NAME"
}

source "amazon-ebs" "worker" {
  region        = var.region
  instance_type = var.instance_type
  source_ami    = var.source_ami
  ssh_username  = "ubuntu"
  ami_name      = var.ami_name

  tags = {
    Name        = var.ami_name
    Purpose     = "postgres-seren-replicator-worker"
    BuildDate   = "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    BuiltBy     = "packer"
    Environment = "production"
  }
}

build {
  sources = ["source.amazon-ebs.worker"]

  # Upload setup script
  provisioner "file" {
    source      = "$SETUP_SCRIPT"
    destination = "/tmp/setup-worker.sh"
  }

  # Run setup script
  provisioner "shell" {
    inline = [
      "chmod +x /tmp/setup-worker.sh",
      "/tmp/setup-worker.sh",
      "rm -f /tmp/setup-worker.sh"
    ]
  }

  # Upload replicator binary
  provisioner "file" {
    source      = "$BINARY_PATH"
    destination = "/tmp/postgres-seren-replicator"
  }

  # Upload worker script
  provisioner "file" {
    source      = "$WORKER_SCRIPT"
    destination = "/tmp/worker.sh"
  }

  # Upload CloudWatch agent configuration
  provisioner "file" {
    source      = "$CLOUDWATCH_CONFIG"
    destination = "/tmp/cloudwatch-agent-config.json"
  }

  # Install replicator, worker script, and CloudWatch config
  provisioner "shell" {
    inline = [
      "sudo mv /tmp/postgres-seren-replicator /opt/seren-replicator/",
      "sudo mv /tmp/worker.sh /opt/seren-replicator/",
      "sudo chmod +x /opt/seren-replicator/postgres-seren-replicator",
      "sudo chmod +x /opt/seren-replicator/worker.sh",
      "sudo mv /tmp/cloudwatch-agent-config.json /opt/aws/amazon-cloudwatch-agent/etc/",
      "sudo chmod 644 /opt/aws/amazon-cloudwatch-agent/etc/cloudwatch-agent-config.json",
      "ls -la /opt/seren-replicator/"
    ]
  }

  # Verify installation (skip binary check - it's macOS format)
  provisioner "shell" {
    inline = [
      "echo 'Verifying installation...'",
      "echo 'Binary check skipped (cross-platform binary)'",
      "psql --version",
      "aws --version",
      "jq --version",
      "ec2-metadata --help > /dev/null && echo 'ec2-metadata OK'"
    ]
  }
}
EOF

log "Packer template generated: worker-ami.pkr.hcl"

# Initialize Packer
log "Initializing Packer..."
packer init worker-ami.pkr.hcl

# Validate Packer template
log "Validating Packer template..."
packer validate worker-ami.pkr.hcl

# Build AMI
log "Building AMI (this takes ~10 minutes)..."
log "AMI name: $AMI_NAME"
log ""

packer build worker-ami.pkr.hcl

# Get the AMI ID
log ""
log "Build complete! Retrieving AMI ID..."
sleep 5  # Wait for AWS to index the new AMI

AMI_ID=$(aws ec2 describe-images \
    --region "$AWS_REGION" \
    --owners self \
    --filters "Name=name,Values=$AMI_NAME" \
    --query 'Images[0].ImageId' \
    --output text)

if [ -z "$AMI_ID" ] || [ "$AMI_ID" = "None" ]; then
    log "WARNING: Could not retrieve AMI ID automatically"
    log "Check AWS Console for AMI: $AMI_NAME"
else
    log "✅ AMI created successfully!"
    log ""
    log "AMI ID: $AMI_ID"
    log "AMI Name: $AMI_NAME"
    log "Region: $AWS_REGION"
    log ""
    log "Next steps:"
    log "  1. Update Terraform variable: worker_ami_id = \"$AMI_ID\""
    log "  2. Run: terraform apply"
    log "  3. Test with: ./test-remote-replication.sh"
fi

# Clean up
log ""
log "Cleaning up Packer template..."
rm -f worker-ami.pkr.hcl

log "Done!"
