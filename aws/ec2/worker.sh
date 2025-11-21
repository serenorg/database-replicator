#!/bin/bash
# ABOUTME: EC2 worker bootstrap script for remote replication jobs
# ABOUTME: Fetches encrypted credentials from DynamoDB, decrypts them, and executes replication

set -euo pipefail

# Configuration
REPLICATOR_BIN="/opt/seren-replicator/postgres-seren-replicator"
DYNAMODB_TABLE="${DYNAMODB_TABLE:-replication-jobs}"
AWS_REGION="${AWS_REGION:-us-east-1}"

# Parse arguments
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <job_id>"
    exit 1
fi

JOB_ID="$1"

# Log function
log() {
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] $*"
}

# Redact credentials from URL for logging
redact_url() {
    local url="$1"
    # Extract everything before @ and after the last @ for safe logging
    if echo "$url" | grep -q '@'; then
        local scheme_user=$(echo "$url" | cut -d'@' -f1)
        local host_path=$(echo "$url" | cut -d'@' -f2-)
        # Show only scheme, hide user:pass
        local scheme=$(echo "$scheme_user" | cut -d':' -f1)
        echo "${scheme}://***@${host_path}"
    else
        echo "$url"
    fi
}

# Update DynamoDB job status
update_job_status() {
    local status="$1"
    local error_msg="${2:-}"

    log "Updating job status to: $status"

    if [ -n "$error_msg" ]; then
        aws dynamodb update-item \
            --region "$AWS_REGION" \
            --table-name "$DYNAMODB_TABLE" \
            --key "{\"job_id\": {\"S\": \"$JOB_ID\"}}" \
            --update-expression "SET #status = :status, error = :error" \
            --expression-attribute-names '{"#status": "status"}' \
            --expression-attribute-values "{\":status\": {\"S\": \"$status\"}, \":error\": {\"S\": \"$error_msg\"}}"
    else
        aws dynamodb update-item \
            --region "$AWS_REGION" \
            --table-name "$DYNAMODB_TABLE" \
            --key "{\"job_id\": {\"S\": \"$JOB_ID\"}}" \
            --update-expression "SET #status = :status, ${status}_at = :timestamp" \
            --expression-attribute-names '{"#status": "status"}' \
            --expression-attribute-values "{\":status\": {\"S\": \"$status\"}, \":timestamp\": {\"S\": \"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\"}}"
    fi
}

# Update progress in DynamoDB
update_progress() {
    local current_db="$1"
    local completed="$2"
    local total="$3"

    local progress_json="{\"current_database\": \"$current_db\", \"databases_completed\": $completed, \"databases_total\": $total}"

    aws dynamodb update-item \
        --region "$AWS_REGION" \
        --table-name "$DYNAMODB_TABLE" \
        --key "{\"job_id\": {\"S\": \"$JOB_ID\"}}" \
        --update-expression "SET progress = :progress" \
        --expression-attribute-values "{\":progress\": {\"S\": \"$progress_json\"}}"
}

# Terminate this instance
terminate_self() {
    log "Self-terminating instance..."

    # Get instance ID from metadata service
    INSTANCE_ID=$(ec2-metadata --instance-id | cut -d " " -f 2)

    if [ -n "$INSTANCE_ID" ]; then
        aws ec2 terminate-instances \
            --region "$AWS_REGION" \
            --instance-ids "$INSTANCE_ID"
        log "Termination initiated for instance $INSTANCE_ID"
    else
        log "ERROR: Could not determine instance ID from metadata"
    fi
}

# Decrypt encrypted string using KMS
decrypt_value() {
    local encrypted_b64="$1"

    # Decrypt using AWS KMS
    # The encrypted value is base64-encoded ciphertext from KMS
    echo "$encrypted_b64" | base64 -d | aws kms decrypt \
        --region "$AWS_REGION" \
        --ciphertext-blob fileb:///dev/stdin \
        --query Plaintext \
        --output text | base64 -d
}

# Trap errors and update status
trap 'update_job_status "failed" "Script error at line $LINENO"; terminate_self' ERR

# Main execution
main() {
    log "Starting replication job: $JOB_ID"

    # Fetch job details from DynamoDB
    log "Fetching job details from DynamoDB..."
    JOB_ITEM=$(aws dynamodb get-item \
        --region "$AWS_REGION" \
        --table-name "$DYNAMODB_TABLE" \
        --key "{\"job_id\": {\"S\": \"$JOB_ID\"}}" \
        --output json)

    if [ -z "$JOB_ITEM" ] || [ "$(echo "$JOB_ITEM" | jq -r '.Item')" = "null" ]; then
        log "ERROR: Job not found in DynamoDB: $JOB_ID"
        terminate_self
        exit 1
    fi

    # Parse job specification
    log "Parsing job specification..."
    COMMAND=$(echo "$JOB_ITEM" | jq -r '.Item.command.S')
    ENCRYPTED_SOURCE=$(echo "$JOB_ITEM" | jq -r '.Item.source_url_encrypted.S')
    ENCRYPTED_TARGET=$(echo "$JOB_ITEM" | jq -r '.Item.target_url_encrypted.S')
    FILTER_JSON=$(echo "$JOB_ITEM" | jq -r '.Item.filter.S // "{}"')
    OPTIONS_JSON=$(echo "$JOB_ITEM" | jq -r '.Item.options.S // "{}"')

    log "Command: $COMMAND"

    # Get job timeout (default: 28800 seconds = 8 hours)
    JOB_TIMEOUT=$(echo "$OPTIONS_JSON" | jq -r '.job_timeout // 28800')
    log "Job timeout: ${JOB_TIMEOUT}s"

    # Start timeout watchdog in background
    (
        sleep "$JOB_TIMEOUT"
        log "Job timeout exceeded ($JOB_TIMEOUT seconds)"
        update_job_status "timeout" "Job exceeded maximum duration of $JOB_TIMEOUT seconds"
        # Kill the replicator process and terminate instance
        pkill -f postgres-seren-replicator || true
        terminate_self
    ) &
    WATCHDOG_PID=$!

    # Decrypt credentials
    log "Decrypting credentials..."
    SOURCE_URL=$(decrypt_value "$ENCRYPTED_SOURCE")
    TARGET_URL=$(decrypt_value "$ENCRYPTED_TARGET")

    log "Source: $(redact_url "$SOURCE_URL")"
    log "Target: $(redact_url "$TARGET_URL")"

    # Update status to running
    update_job_status "running"

    # Build replicator command
    CMD=("$REPLICATOR_BIN" "$COMMAND" "--source" "$SOURCE_URL" "--target" "$TARGET_URL" "--yes")

    # Add filter options
    INCLUDE_DATABASES=$(echo "$FILTER_JSON" | jq -r '.include_databases // empty | .[]')
    if [ -n "$INCLUDE_DATABASES" ]; then
        while IFS= read -r db; do
            CMD+=("--include-databases" "$db")
        done <<< "$INCLUDE_DATABASES"
    fi

    EXCLUDE_TABLES=$(echo "$FILTER_JSON" | jq -r '.exclude_tables // empty | .[]')
    if [ -n "$EXCLUDE_TABLES" ]; then
        while IFS= read -r table; do
            CMD+=("--exclude-tables" "$table")
        done <<< "$EXCLUDE_TABLES"
    fi

    # Add options from job spec
    DROP_EXISTING=$(echo "$OPTIONS_JSON" | jq -r '.drop_existing // "false"')
    if [ "$DROP_EXISTING" = "true" ]; then
        CMD+=("--drop-existing")
    fi

    NO_SYNC=$(echo "$OPTIONS_JSON" | jq -r '.no_sync // "false"')
    if [ "$NO_SYNC" = "true" ]; then
        CMD+=("--no-sync")
    fi

    # Execute replication
    log "Executing replication command..."
    # Log command with redacted credentials
    CMD_DISPLAY=("$REPLICATOR_BIN" "$COMMAND" "--source" "$(redact_url "$SOURCE_URL")" "--target" "$(redact_url "$TARGET_URL")" "--yes")
    log "Command: ${CMD_DISPLAY[*]}"

    if "${CMD[@]}"; then
        log "Replication completed successfully"
        # Kill the timeout watchdog since job completed
        kill "$WATCHDOG_PID" 2>/dev/null || true
        update_job_status "completed"
    else
        EXIT_CODE=$?
        log "Replication failed with exit code: $EXIT_CODE"
        # Kill the timeout watchdog since job completed (failed)
        kill "$WATCHDOG_PID" 2>/dev/null || true
        update_job_status "failed" "Replication command failed with exit code $EXIT_CODE"
    fi

    # Self-terminate
    terminate_self
}

# Run main function
main "$@"
