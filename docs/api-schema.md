# API Schema and Validation

This document describes the job specification schema, validation rules, and versioning strategy for the remote replication API.

## Schema Versioning

### Current Version

- **Current Schema Version:** `1.0`
- **Supported Versions:** `["1.0"]`

### Version Format

Schema versions follow semantic versioning: `MAJOR.MINOR`

- **MAJOR:** Incremented for incompatible changes that break existing clients
- **MINOR:** Incremented for backward-compatible additions

### Backward Compatibility

The API maintains backward compatibility within a major version. When adding new features:
- New optional fields can be added without incrementing the major version
- Required fields cannot be removed without a major version bump
- Field semantics cannot change without a major version bump

## Job Specification Schema (v1.0)

### Request Format

```json
{
  "schema_version": "1.0",
  "command": "init",
  "source_url": "postgresql://user:pass@source.example.com:5432/mydb",
  "target_url": "postgresql://user:pass@target.example.com:5432/mydb",
  "filter": {
    "include_databases": ["db1", "db2"],
    "exclude_tables": ["db1.logs", "db2.cache"]
  },
  "options": {
    "drop_existing": false,
    "enable_sync": true,
    "estimated_size_bytes": 1073741824
  }
}
```

### Field Descriptions

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `schema_version` | string | Yes | API schema version (must be `"1.0"`) |
| `command` | string | Yes | Replication command (allowed: `init`, `validate`, `sync`, `status`, `verify`) |
| `source_url` | string | Yes | PostgreSQL connection URL for source database |
| `target_url` | string | Yes | PostgreSQL connection URL for target database |
| `filter` | object | No | Optional filtering configuration |
| `options` | object | No | Optional command-specific options |

### Filter Object

| Field | Type | Description |
|-------|------|-------------|
| `include_databases` | string[] | List of databases to replicate (mutually exclusive with `exclude_databases`) |
| `exclude_tables` | string[] | List of tables to exclude in format `"database.table"` |

### Options Object

| Field | Type | Description |
|-------|------|-------------|
| `drop_existing` | boolean | Whether to drop existing target database before init |
| `enable_sync` | boolean | Whether to set up continuous logical replication after init |
| `estimated_size_bytes` | number | Estimated total size of databases to replicate (used for instance sizing) |

## Validation Rules

### Size Limits

- **Maximum job spec size:** 15KB (15,360 bytes)
  - Reason: EC2 user-data limit is 16KB, leaving 1KB buffer
- **Maximum URL length:** 2,048 characters
- **Maximum command length:** 50 characters

### Command Validation

**Allowed commands:** `init`, `validate`, `sync`, `status`, `verify`

Commands are validated case-insensitively and must be one of the allowed values.

### URL Validation

PostgreSQL connection URLs must:
- Use `postgresql://` or `postgres://` scheme
- Include a hostname (cannot be empty)
- Have a valid hostname format (alphanumeric, hyphens, dots)
- Have a valid port (1-65535) if specified
- Have a valid database name (alphanumeric, underscores, hyphens) if specified

**Security checks:**
- URLs are scanned for command injection patterns:
  - Command chaining: `;`
  - Command substitution: `$(`, `` ` ``
  - Boolean operators: `||`, `&&`
- Multiple `@` signs are rejected (malformed URL)
- Invalid port ranges are rejected

### Options Validation

- `drop_existing`: Must be boolean
- `enable_sync`: Must be boolean
- `estimated_size_bytes`: Must be a non-negative number
- Unknown option keys are rejected

### Type Validation

All required fields must:
- Be present in the request
- Be of the correct type (string, object, etc.)
- Not be empty strings

## Response Format

### Success Response (201 Created)

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "trace_id": "660e8400-e29b-41d4-a716-446655440000",
  "status": "provisioning"
}
```

### Error Response (400 Bad Request)

```json
{
  "error": "Missing required field: schema_version"
}
```

Common validation errors:
- `"Missing required field: schema_version"`
- `"Unsupported schema version: X.X (supported: 1.0)"`
- `"Invalid command: X (allowed: init, validate, sync, status, verify)"`
- `"Invalid source_url: URL contains potentially dangerous characters"`
- `"Job spec too large: X bytes (max: 15360)"`
- `"Field 'options' must be an object"`
- `"Unknown option: X"`

## Credential Security

### Encryption at Rest

- Source and target URLs (containing credentials) are encrypted using AWS KMS before storage in DynamoDB
- Encrypted as `source_url_encrypted` and `target_url_encrypted` attributes
- Credentials are never stored in plaintext

### Credential Protection in Transit

- URLs are redacted in all log output
- Example: `postgresql://user:pass@host/db` → `postgresql://***@host/db`
- Credentials never appear in CloudWatch logs or EC2 console

### User-Data Security

**Critical:** Credentials are NOT passed via EC2 user-data.

User-data contains only:
```bash
#!/bin/bash
/opt/seren-replicator/worker.sh "job-id-uuid"
```

The worker:
1. Fetches the job record from DynamoDB using the job ID
2. Retrieves the encrypted credentials
3. Decrypts them using AWS KMS
4. Uses them for replication

This ensures credentials never appear in:
- EC2 console (user-data is visible)
- CloudWatch Logs (user-data logs)
- EC2 metadata service

## Migration Guide

### Upgrading to v1.0 (Current)

If you have existing code using the old format without `schema_version`:

**Before:**
```json
{
  "command": "init",
  "source_url": "postgresql://...",
  "target_url": "postgresql://..."
}
```

**After:**
```json
{
  "schema_version": "1.0",
  "command": "init",
  "source_url": "postgresql://...",
  "target_url": "postgresql://..."
}
```

### Future Versions

When new schema versions are released:
1. The API will support both old and new versions
2. Clients can continue using older versions within the same major version
3. Deprecation warnings will be issued before removing support
4. A migration period of at least 6 months will be provided

## Testing

Comprehensive validation tests are available in `aws/lambda/test_handler.py`:

```bash
cd aws/lambda
python3 test_handler.py -v
```

Test coverage includes:
- Valid and invalid job specs
- Schema version validation
- Required field validation
- Command whitelist validation
- URL format and security validation
- Options type validation
- Size limit validation

## Related Documentation

- [Deployment Guide](../aws/README.md) - API deployment and configuration
- [CI/CD Guide](./cicd.md) - Testing and deployment pipelines
- [Integration Testing](./integration-testing.md) - End-to-end testing strategies
