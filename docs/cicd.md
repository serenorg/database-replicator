# CI/CD Guide

This document describes the CI/CD setup for the seren-replicator project.

## Overview

The project uses GitHub Actions for continuous integration and provides tools for continuous deployment. The CI/CD pipeline is split into two phases:

- **Phase 1 (Current):** Automated build, test, lint + Manual deployment with parameterized environments
- **Phase 2 (Future):** Fully automated deployment pipelines per environment

## Current CI Pipeline

### GitHub Actions Workflows

Located in `.github/workflows/ci.yml`:

**Jobs:**
1. **Test** - Runs on Ubuntu and macOS
   - Unit tests: `cargo test`
   - Doc tests: `cargo test --doc`
   - Caches Cargo dependencies for speed

2. **Lint** - Code quality checks
   - Format check: `cargo fmt --check`
   - Clippy lints: `cargo clippy -- -D warnings`

3. **Security** - Dependency audit
   - Runs `cargo audit` to detect vulnerabilities
   - Automated security scanning with rustsec

4. **Build** - Multi-platform releases
   - Linux x64, macOS x64, macOS ARM64
   - Uploads artifacts for each platform
   - Used by release workflow

**Triggers:**
- Every push to `main` branch
- Every pull request to `main` branch

### Integration Tests

Integration tests are **not** run in CI by default. See [Integration Testing Guide](./integration-testing.md) for details.

**Why not in CI?**
- Require real PostgreSQL databases
- Some tests are destructive
- Variable test duration
- Cost considerations

**How to run locally:**
```bash
# Setup test databases with Docker
docker run -d --name pg-source -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:17
docker run -d --name pg-target -e POSTGRES_PASSWORD=postgres -p 5433:5432 postgres:17

# Run integration tests
export TEST_SOURCE_URL="postgresql://postgres:postgres@localhost:5432/postgres"
export TEST_TARGET_URL="postgresql://postgres:postgres@localhost:5433/postgres"
cargo test --test integration_test -- --ignored

# Cleanup
docker stop pg-source pg-target && docker rm pg-source pg-target
```

## Deployment

### Environment Configuration

Three environments supported via Terraform variables:

| Environment | Config File | Purpose |
|-------------|-------------|---------|
| Development | `environments/dev.tfvars` | Local testing, smaller instances |
| Staging | `environments/staging.tfvars` | Pre-production validation |
| Production | `environments/prod.tfvars` | Live customer traffic |

**Example deployment:**
```bash
cd aws/terraform

# Development
terraform apply -var-file=environments/dev.tfvars -var="worker_ami_id=ami-xxxxx"

# Staging
terraform apply -var-file=environments/staging.tfvars -var="worker_ami_id=ami-xxxxx"

# Production
terraform apply -var-file=environments/prod.tfvars -var="worker_ami_id=ami-xxxxx"
```

### Deployment Process

#### 1. Build Release Binary
```bash
cargo build --release
```

#### 2. Build Worker AMI
```bash
cd aws/ec2
./build-ami.sh
```

This creates an AMI with:
- PostgreSQL 17 client tools
- AWS CLI v2
- CloudWatch agent
- Worker script and replicator binary

**Output:** AMI ID (e.g., `ami-0abc123def456`)

#### 3. Package Lambda Functions
```bash
cd aws/lambda
./package.sh
```

Creates `lambda.zip` with both coordinator and provisioner functions.

#### 4. Deploy Infrastructure
```bash
cd aws/terraform

# First time: Initialize
terraform init

# Review changes
terraform plan -var-file=environments/prod.tfvars -var="worker_ami_id=ami-xxxxx"

# Apply changes
terraform apply -var-file=environments/prod.tfvars -var="worker_ami_id=ami-xxxxx"
```

**Outputs:**
- `api_endpoint` - API Gateway URL
- `api_key` - API key for authentication (sensitive)
- Other resource identifiers

#### 5. Run Smoke Tests
```bash
cd aws

# Get outputs from Terraform
API_ENDPOINT=$(terraform output -raw api_endpoint)
API_KEY=$(terraform output -raw api_key)

# Run smoke tests
./test-smoke.sh
```

### Smoke Tests

Located at `aws/test-smoke.sh` - validates deployed API:

**Tests:**
1. API health check (404 for nonexistent job)
2. Invalid payload rejection (400 error)
3. Valid job submission (201 created)
4. Job status polling (GET /jobs/{id})

**Usage:**
```bash
API_ENDPOINT=https://xxx.execute-api.us-east-1.amazonaws.com \
API_KEY=your-api-key \
./aws/test-smoke.sh
```

**Configuration:**
- `MAX_POLL_ATTEMPTS` - Number of status checks (default: 60)
- `POLL_INTERVAL` - Seconds between checks (default: 5)

### Terraform State Management

**Local State (Default):**
- State stored in `terraform.tfstate` file
- Simple, works for single-user development
- Not suitable for teams

**Remote State (Recommended for Production):**

1. **Create S3 bucket for state:**
   ```bash
   aws s3 mb s3://your-terraform-state-bucket --region us-east-1
   aws s3api put-bucket-versioning \
     --bucket your-terraform-state-bucket \
     --versioning-configuration Status=Enabled
   ```

2. **Create DynamoDB table for locking:**
   ```bash
   aws dynamodb create-table \
     --table-name terraform-state-lock \
     --attribute-definitions AttributeName=LockID,AttributeType=S \
     --key-schema AttributeName=LockID,KeyType=HASH \
     --billing-mode PAY_PER_REQUEST \
     --region us-east-1
   ```

3. **Enable remote backend:**
   ```bash
   cd aws/terraform
   cp backend.tf.example backend.tf
   # Edit backend.tf with your bucket/table names
   terraform init -migrate-state
   ```

**Benefits:**
- Team collaboration (shared state)
- State locking (prevents concurrent modifications)
- State versioning (can recover from mistakes)
- Encryption at rest

## Deployment Checklist

### Pre-Deployment
- [ ] All tests pass locally: `cargo test`
- [ ] Linting passes: `cargo fmt -- --check && cargo clippy`
- [ ] Integration tests pass (if applicable)
- [ ] Changes reviewed and approved
- [ ] Release notes prepared

### Deployment
- [ ] Build release binary: `cargo build --release`
- [ ] Build worker AMI: `cd aws/ec2 && ./build-ami.sh`
- [ ] Note AMI ID from output
- [ ] Package Lambda functions: `cd aws/lambda && ./package.sh`
- [ ] Review Terraform plan: `terraform plan -var-file=environments/prod.tfvars -var="worker_ami_id=ami-xxxxx"`
- [ ] Apply Terraform: `terraform apply -var-file=environments/prod.tfvars -var="worker_ami_id=ami-xxxxx"`
- [ ] Note API endpoint and key from outputs

### Post-Deployment
- [ ] Run smoke tests: `./aws/test-smoke.sh`
- [ ] Check CloudWatch logs for errors
- [ ] Verify CloudWatch metrics are being emitted
- [ ] Test one real replication job (small database)
- [ ] Monitor for 15-30 minutes
- [ ] Update documentation if needed
- [ ] Tag release: `git tag vX.Y.Z && git push --tags`

## Phase 2: Automated CI/CD (Future)

### Planned Enhancements

**Automated Deployments:**
- [ ] GitOps workflow: PR → Review → Merge → Auto-deploy
- [ ] Environment promotion: Dev → Staging → Prod
- [ ] Rollback automation
- [ ] Deployment approval gates

**Enhanced Testing:**
- [ ] Integration tests in CI with Docker PostgreSQL
- [ ] End-to-end API tests in CI
- [ ] Performance benchmarks
- [ ] Load testing in staging

**Infrastructure as Code:**
- [ ] Packer templates in CI
- [ ] Multi-region AMI copying
- [ ] Automated AMI cleanup (old versions)
- [ ] Terraform workspaces per environment

**Monitoring & Alerts:**
- [ ] Deployment notifications (Slack/email)
- [ ] Automated health checks post-deployment
- [ ] Canary deployments
- [ ] Blue/green deployments

### Proposed Workflows

**1. Continuous Integration (ci.yml)** - Already exists
- Runs on every PR
- Tests, lint, build
- Security audit

**2. Continuous Deployment (deploy.yml)** - Future
```yaml
on:
  push:
    branches: [main]
  workflow_dispatch:

jobs:
  deploy-dev:
    # Auto-deploy to dev on main push

  deploy-staging:
    # Requires dev success

  deploy-prod:
    # Requires manual approval
```

**3. Release Workflow (release.yml)** - Exists
- Triggered by Git tags
- Builds binaries for all platforms
- Creates GitHub release with artifacts

**4. Integration Tests (integration.yml)** - Future
- Manual trigger or scheduled
- Runs full integration test suite
- Uses Docker PostgreSQL instances

## Troubleshooting

### Smoke Test Failures

**401 Unauthorized:**
- Check `API_KEY` environment variable
- Verify API key in Terraform outputs: `terraform output api_key`

**Connection Refused:**
- Check `API_ENDPOINT` is correct
- Verify API Gateway is deployed: `terraform state show aws_apigatewayv2_api.api`
- Check AWS region matches

**Job Never Reaches Terminal State:**
- This is acceptable - job is processing
- Check CloudWatch logs: Use `log_url` from status response
- Verify worker provisioned: Check EC2 console

### Terraform Issues

**State Lock:**
```
Error: Error locking state: ConditionalCheckFailedException
```
- Another operation is in progress
- Wait for it to complete or manually release lock:
  ```bash
  terraform force-unlock LOCK_ID
  ```

**AMI Not Found:**
```
Error: Error launching source instance: InvalidAMIID.NotFound
```
- Verify AMI exists: `aws ec2 describe-images --image-ids ami-xxxxx`
- Check region matches
- Rebuild AMI if deleted: `cd aws/ec2 && ./build-ami.sh`

**Lambda Package Too Large:**
```
Error: Code size exceeds maximum
```
- Lambda functions must be < 50MB zipped
- Check `lambda.zip` size: `ls -lh aws/lambda/lambda.zip`
- Dependencies may need optimization

## Related Documentation

- [Integration Testing Guide](./integration-testing.md)
- [Deployment Guide](../aws/README.md)
- [CI Workflow](../.github/workflows/ci.yml)
- [Release Workflow](../.github/workflows/release.yml)
- [CLAUDE.md](../CLAUDE.md) - Development practices
