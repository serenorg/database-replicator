#!/bin/bash
# ABOUTME: AMI setup script - installs dependencies for replication worker
# ABOUTME: Run this during AMI build to install PostgreSQL tools, AWS CLI, jq, etc.

set -euo pipefail

# Log function
log() {
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] $*"
}

log "Starting worker AMI setup..."

# Update system packages
log "Updating system packages..."
sudo apt-get update -y
sudo apt-get upgrade -y

# Install PostgreSQL 17 repository
log "Adding PostgreSQL 17 repository..."
sudo apt-get install -y wget gnupg2 lsb-release unzip
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo gpg --dearmor -o /usr/share/keyrings/postgresql-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/postgresql-keyring.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list
sudo apt-get update -y

# Install PostgreSQL 17 client tools
log "Installing PostgreSQL 17 client tools..."
sudo apt-get install -y postgresql-client-17

# Verify PostgreSQL tools
log "Verifying PostgreSQL tools installation..."
psql --version
pg_dump --version
pg_dumpall --version
pg_restore --version

# Install AWS CLI v2
log "Installing AWS CLI v2..."
cd /tmp
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip -q awscliv2.zip
sudo ./aws/install
rm -rf aws awscliv2.zip

# Verify AWS CLI
log "Verifying AWS CLI installation..."
aws --version

# Install jq for JSON parsing
log "Installing jq..."
sudo apt-get install -y jq

# Verify jq
log "Verifying jq installation..."
jq --version

# Install ec2-metadata helper
log "Installing ec2-metadata..."
sudo wget -O /usr/local/bin/ec2-metadata http://s3.amazonaws.com/ec2metadata/ec2-metadata
sudo chmod +x /usr/local/bin/ec2-metadata

# Verify ec2-metadata
log "Verifying ec2-metadata installation..."
/usr/local/bin/ec2-metadata --help > /dev/null

# Create replicator directory
log "Creating /opt/seren-replicator directory..."
sudo mkdir -p /opt/seren-replicator
sudo chmod 755 /opt/seren-replicator

# Install and configure CloudWatch agent for log streaming
log "Installing CloudWatch agent..."
cd /tmp
wget -q https://s3.amazonaws.com/amazoncloudwatch-agent/ubuntu/amd64/latest/amazon-cloudwatch-agent.deb
sudo dpkg -i -E ./amazon-cloudwatch-agent.deb
rm amazon-cloudwatch-agent.deb

# Configure CloudWatch agent to ship logs
log "Configuring CloudWatch agent..."
sudo mkdir -p /opt/aws/amazon-cloudwatch-agent/etc
# Configuration will be deployed via Packer/user-data

# Clean up
log "Cleaning up..."
sudo apt-get autoremove -y
sudo apt-get clean
# Clean temp files (ignore permission errors on system directories)
rm -rf /tmp/* 2>/dev/null || true

log "Worker AMI setup complete!"
log "Summary:"
log "  - PostgreSQL 17 client tools installed"
log "  - AWS CLI v2 installed"
log "  - jq installed"
log "  - ec2-metadata installed"
log "  - CloudWatch agent installed"
log "  - /opt/seren-replicator directory created"
log ""
log "Next steps:"
log "  1. Copy postgres-seren-replicator binary to /opt/seren-replicator/"
log "  2. Copy worker.sh script to /opt/seren-replicator/"
log "  3. Set executable permissions: chmod +x /opt/seren-replicator/*"
log "  4. Create AMI snapshot"
