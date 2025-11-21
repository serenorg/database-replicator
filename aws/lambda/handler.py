"""
ABOUTME: AWS Lambda function for remote replication job orchestration
ABOUTME: Handles POST /jobs (submit) and GET /jobs/{id} (status) requests with security features
"""

import json
import uuid
import time
import boto3
import os
import base64
from datetime import datetime
from urllib.parse import urlparse, urlunparse

# AWS clients
dynamodb = boto3.client('dynamodb')
ec2 = boto3.client('ec2')
ssm = boto3.client('ssm')
kms = boto3.client('kms')

# Configuration from environment variables
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE', 'replication-jobs')
WORKER_AMI_ID = os.environ.get('WORKER_AMI_ID', 'ami-xxxxxxxxx')
WORKER_INSTANCE_TYPE = os.environ.get('WORKER_INSTANCE_TYPE', 'c5.2xlarge')
WORKER_IAM_ROLE = os.environ.get('WORKER_IAM_ROLE', 'seren-replication-worker')
KMS_KEY_ID = os.environ.get('KMS_KEY_ID')
API_KEY_PARAMETER_NAME = os.environ.get('API_KEY_PARAMETER_NAME')

# Cache for API key (loaded once per Lambda container lifecycle)
_api_key_cache = None


def get_api_key():
    """Retrieve API key from SSM Parameter Store (cached)"""
    global _api_key_cache

    if _api_key_cache is not None:
        return _api_key_cache

    if not API_KEY_PARAMETER_NAME:
        raise ValueError("API_KEY_PARAMETER_NAME environment variable not set")

    try:
        response = ssm.get_parameter(
            Name=API_KEY_PARAMETER_NAME,
            WithDecryption=True
        )
        _api_key_cache = response['Parameter']['Value']
        return _api_key_cache
    except Exception as e:
        print(f"Failed to retrieve API key: {e}")
        raise


def validate_api_key(event):
    """Validate API key from request headers"""
    headers = event.get('headers', {})

    # Headers are case-insensitive, normalize to lowercase
    headers_lower = {k.lower(): v for k, v in headers.items()}

    provided_key = headers_lower.get('x-api-key')

    if not provided_key:
        return False, "Missing x-api-key header"

    expected_key = get_api_key()

    if provided_key != expected_key:
        return False, "Invalid API key"

    return True, None


def encrypt_data(plaintext):
    """Encrypt data using KMS"""
    if not KMS_KEY_ID:
        raise ValueError("KMS_KEY_ID environment variable not set")

    try:
        response = kms.encrypt(
            KeyId=KMS_KEY_ID,
            Plaintext=plaintext.encode('utf-8')
        )
        # Base64 encode the ciphertext for storage
        return base64.b64encode(response['CiphertextBlob']).decode('utf-8')
    except Exception as e:
        print(f"Encryption failed: {e}")
        raise


def decrypt_data(ciphertext_b64):
    """Decrypt data using KMS"""
    try:
        # Base64 decode the ciphertext
        ciphertext = base64.b64decode(ciphertext_b64)

        response = kms.decrypt(
            CiphertextBlob=ciphertext
        )
        return response['Plaintext'].decode('utf-8')
    except Exception as e:
        print(f"Decryption failed: {e}")
        raise


def redact_url(url):
    """Redact credentials from connection URL for logging"""
    if not url:
        return url

    try:
        parsed = urlparse(url)
        if parsed.username or parsed.password:
            # Reconstruct URL without credentials
            netloc = parsed.hostname
            if parsed.port:
                netloc = f"{netloc}:{parsed.port}"

            redacted = urlunparse((
                parsed.scheme,
                netloc,
                parsed.path,
                parsed.params,
                parsed.query,
                parsed.fragment
            ))
            return f"{redacted} (credentials redacted)"
        return url
    except:
        return "[invalid URL]"


def lambda_handler(event, context):
    """Main Lambda handler - routes requests to appropriate handler"""

    http_method = event.get('httpMethod', '')
    path = event.get('path', '')

    print(f"Request: {http_method} {path}")

    # Validate API key for all requests
    is_valid, error_msg = validate_api_key(event)
    if not is_valid:
        print(f"Authentication failed: {error_msg}")
        return {
            'statusCode': 401,
            'body': json.dumps({'error': 'Unauthorized'})
        }

    try:
        if http_method == 'POST' and path == '/jobs':
            return handle_submit_job(event)
        elif http_method == 'GET' and path.startswith('/jobs/'):
            job_id = path.split('/')[-1]
            return handle_get_job(job_id)
        else:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Not found'})
            }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'})
        }


def handle_submit_job(event):
    """Handle POST /jobs - submit new replication job"""

    # Parse request body
    try:
        body = json.loads(event['body'])
    except:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Invalid JSON'})
        }

    # Validate required fields
    required_fields = ['command', 'source_url', 'target_url']
    for field in required_fields:
        if field not in body:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': f'Missing required field: {field}'})
            }

    # Generate job ID
    job_id = str(uuid.uuid4())

    # Encrypt sensitive credentials
    try:
        encrypted_source = encrypt_data(body['source_url'])
        encrypted_target = encrypt_data(body['target_url'])
    except Exception as e:
        print(f"Encryption failed: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Failed to encrypt credentials'})
        }

    # Log with redacted URLs
    print(f"Job {job_id}: {body['command']} from {redact_url(body['source_url'])} to {redact_url(body['target_url'])}")

    # Create job record in DynamoDB with encrypted credentials
    now = datetime.utcnow().isoformat() + 'Z'
    ttl = int(time.time()) + (30 * 86400)  # 30 days

    try:
        dynamodb.put_item(
            TableName=DYNAMODB_TABLE,
            Item={
                'job_id': {'S': job_id},
                'status': {'S': 'provisioning'},
                'command': {'S': body['command']},
                'source_url_encrypted': {'S': encrypted_source},
                'target_url_encrypted': {'S': encrypted_target},
                'filter': {'S': json.dumps(body.get('filter', {}))},
                'options': {'S': json.dumps(body.get('options', {}))},
                'created_at': {'S': now},
                'ttl': {'N': str(ttl)},
            }
        )
    except Exception as e:
        print(f"DynamoDB error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Failed to create job record'})
        }

    # Provision EC2 instance (passes only job_id, not credentials)
    try:
        instance_id = provision_worker(job_id)

        # Update job with instance ID
        dynamodb.update_item(
            TableName=DYNAMODB_TABLE,
            Key={'job_id': {'S': job_id}},
            UpdateExpression='SET instance_id = :iid',
            ExpressionAttributeValues={':iid': {'S': instance_id}}
        )

        print(f"Job {job_id} submitted, instance {instance_id} provisioning")

    except Exception as e:
        print(f"Failed to provision instance: {e}")
        # Update job status to failed
        dynamodb.update_item(
            TableName=DYNAMODB_TABLE,
            Key={'job_id': {'S': job_id}},
            UpdateExpression='SET #status = :status, error = :error',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={
                ':status': {'S': 'failed'},
                ':error': {'S': 'Provisioning failed'}
            }
        )
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Provisioning failed'})
        }

    return {
        'statusCode': 201,
        'body': json.dumps({
            'job_id': job_id,
            'status': 'provisioning'
        })
    }


def provision_worker(job_id):
    """Provision EC2 instance to run replication job

    Security: Only passes job_id to worker, not credentials.
    Worker fetches and decrypts credentials from DynamoDB.
    """

    # Build user data script - only passes job_id
    user_data = f"""#!/bin/bash
set -euo pipefail

# Execute worker script with job ID
# Worker will fetch credentials from DynamoDB and decrypt them
/opt/seren-replicator/worker.sh "{job_id}"
"""

    # Launch instance
    response = ec2.run_instances(
        ImageId=WORKER_AMI_ID,
        InstanceType=WORKER_INSTANCE_TYPE,
        MinCount=1,
        MaxCount=1,
        IamInstanceProfile={'Name': WORKER_IAM_ROLE},
        UserData=user_data,
        TagSpecifications=[{
            'ResourceType': 'instance',
            'Tags': [
                {'Key': 'Name', 'Value': f'seren-replication-{job_id}'},
                {'Key': 'JobId', 'Value': job_id},
                {'Key': 'ManagedBy', 'Value': 'seren-replication-system'}
            ]
        }],
        InstanceInitiatedShutdownBehavior='terminate',
    )

    instance_id = response['Instances'][0]['InstanceId']
    return instance_id


def handle_get_job(job_id):
    """Handle GET /jobs/{job_id} - get job status"""

    try:
        response = dynamodb.get_item(
            TableName=DYNAMODB_TABLE,
            Key={'job_id': {'S': job_id}}
        )
    except Exception as e:
        print(f"DynamoDB error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Database error'})
        }

    if 'Item' not in response:
        return {
            'statusCode': 404,
            'body': json.dumps({'error': 'Job not found'})
        }

    item = response['Item']

    # Convert DynamoDB item to JSON (exclude encrypted credentials from response)
    job_status = {
        'job_id': item['job_id']['S'],
        'status': item['status']['S'],
        'created_at': item.get('created_at', {}).get('S'),
        'started_at': item.get('started_at', {}).get('S'),
        'completed_at': item.get('completed_at', {}).get('S'),
        'error': item.get('error', {}).get('S'),
    }

    # Parse progress if present
    if 'progress' in item:
        try:
            job_status['progress'] = json.loads(item['progress']['S'])
        except:
            pass

    return {
        'statusCode': 200,
        'body': json.dumps(job_status)
    }
