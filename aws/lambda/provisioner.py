"""
ABOUTME: Lambda function for processing SQS queue and provisioning EC2 workers
ABOUTME: Decouples job submission from EC2 provisioning for better burst handling
"""

import json
import time
import boto3
import os
from botocore.exceptions import ClientError

# AWS clients
dynamodb = boto3.client('dynamodb')
ec2 = boto3.client('ec2')
kms = boto3.client('kms')
cloudwatch = boto3.client('cloudwatch')

# Configuration from environment variables
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE', 'replication-jobs')
WORKER_AMI_ID = os.environ.get('WORKER_AMI_ID', 'ami-xxxxxxxxx')
WORKER_INSTANCE_TYPE = os.environ.get('WORKER_INSTANCE_TYPE', 'c5.2xlarge')
WORKER_IAM_ROLE = os.environ.get('WORKER_IAM_ROLE', 'seren-replication-worker')
KMS_KEY_ID = os.environ.get('KMS_KEY_ID')
MAX_CONCURRENT_JOBS = int(os.environ.get('MAX_CONCURRENT_JOBS', '10'))
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
WORKER_LOG_GROUP = '/aws/ec2/seren-replication-worker'


def choose_instance_type(estimated_size_bytes):
    """Choose EC2 instance type based on database size

    Cost optimization for SerenAI-managed infrastructure:
    - Small (<10GB): t3.medium (~$0.04/hr) - 2 vCPU, 4GB RAM
    - Medium (10-100GB): c5.large (~$0.085/hr) - 2 vCPU, 4GB RAM, compute-optimized
    - Large (100GB-1TB): c5.2xlarge (~$0.34/hr) - 8 vCPU, 16GB RAM
    - Very large (>1TB): c5.4xlarge (~$0.68/hr) - 16 vCPU, 32GB RAM

    Args:
        estimated_size_bytes: Total size of databases to replicate in bytes

    Returns:
        EC2 instance type string (e.g., 't3.medium', 'c5.2xlarge')
    """
    size_gb = estimated_size_bytes / (1024**3)

    if size_gb < 10:
        return 't3.medium'
    elif size_gb < 100:
        return 'c5.large'
    elif size_gb < 1024:
        return 'c5.2xlarge'
    else:
        return 'c5.4xlarge'


def put_metric(metric_name, value=1.0, unit='Count', dimensions=None):
    """
    Put custom CloudWatch metric for job tracking and monitoring

    Args:
        metric_name: Name of the metric (e.g., 'JobProvisioned', 'ProvisioningFailed')
        value: Metric value (default: 1.0)
        unit: Metric unit (default: 'Count')
        dimensions: Optional list of dimension dicts [{'Name': 'InstanceType', 'Value': 't3.medium'}]
    """
    try:
        from datetime import datetime
        metric_data = {
            'MetricName': metric_name,
            'Value': value,
            'Unit': unit,
            'Timestamp': datetime.utcnow()
        }

        if dimensions:
            metric_data['Dimensions'] = dimensions

        cloudwatch.put_metric_data(
            Namespace='SerenReplication',
            MetricData=[metric_data]
        )
    except Exception as e:
        # Don't fail the request if metrics fail
        print(f"Failed to put metric {metric_name}: {e}")


def count_active_jobs():
    """Count jobs in provisioning or running state"""
    try:
        response = dynamodb.scan(
            TableName=DYNAMODB_TABLE,
            FilterExpression='#status IN (:provisioning, :running)',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={
                ':provisioning': {'S': 'provisioning'},
                ':running': {'S': 'running'}
            },
            Select='COUNT'
        )
        return response['Count']
    except Exception as e:
        print(f"Failed to count active jobs: {e}")
        # Return 0 on error to allow job submission (fail open)
        return 0


def retry_with_backoff(func, max_retries=3, initial_delay=1):
    """Retry a function with exponential backoff

    Args:
        func: Callable to retry
        max_retries: Maximum number of retry attempts (default: 3)
        initial_delay: Initial delay in seconds (default: 1)

    Returns:
        Result of func()

    Raises:
        Last exception if all retries fail
    """
    delay = initial_delay
    last_exception = None

    for attempt in range(max_retries):
        try:
            return func()
        except ClientError as e:
            last_exception = e
            error_code = e.response.get('Error', {}).get('Code', '')

            # Only retry on transient errors
            retryable_errors = [
                'RequestLimitExceeded',
                'InsufficientInstanceCapacity',
                'InternalError',
                'ServiceUnavailable',
                'Throttling'
            ]

            if error_code not in retryable_errors:
                # Not a transient error, raise immediately
                raise

            if attempt < max_retries - 1:
                print(f"Retry attempt {attempt + 1}/{max_retries} after {delay}s (error: {error_code})")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                print(f"All {max_retries} retries exhausted")

    # All retries failed
    raise last_exception


def provision_worker(job_id, options=None):
    """Provision EC2 instance to run replication job

    Security: Only passes job_id to worker, not credentials.
    Worker fetches and decrypts credentials from DynamoDB.
    """
    if options is None:
        options = {}

    # Automatically choose instance type based on database size
    estimated_size = options.get('estimated_size_bytes', 0)
    if estimated_size > 0:
        instance_type = choose_instance_type(estimated_size)
        size_gb = estimated_size / (1024**3)
        print(f"Database size: {size_gb:.1f} GB, automatically selected instance type: {instance_type}")
    else:
        # No size estimate provided, fall back to environment variable default
        instance_type = WORKER_INSTANCE_TYPE
        print(f"No size estimate provided, using default instance type: {instance_type}")

    print(f"Provisioning {instance_type} instance for job {job_id}")

    # Build user data script - only passes job_id
    user_data = f"""#!/bin/bash
set -euo pipefail

# Execute worker script with job ID
# Worker will fetch credentials from DynamoDB and decrypt them
/opt/seren-replicator/worker.sh "{job_id}"
"""

    # Launch instance with retry logic for transient failures
    def launch_instance():
        return ec2.run_instances(
            ImageId=WORKER_AMI_ID,
            InstanceType=instance_type,
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

    response = retry_with_backoff(launch_instance, max_retries=3, initial_delay=2)
    instance_id = response['Instances'][0]['InstanceId']
    return instance_id


def lambda_handler(event, context):
    """Process SQS messages and provision EC2 workers"""

    # Process each SQS message
    for record in event['Records']:
        trace_id = None
        start_time = time.time()

        try:
            # Parse SQS message body
            message_body = json.loads(record['body'])
            job_id = message_body['job_id']
            trace_id = message_body.get('trace_id')
            options = message_body.get('options', {})

            print(f"[TRACE:{trace_id}] Processing job {job_id} from queue")

            # Check concurrent job limit before provisioning
            active_jobs = count_active_jobs()
            if active_jobs >= MAX_CONCURRENT_JOBS:
                print(f"[TRACE:{trace_id}] Job {job_id} deferred: {active_jobs} active jobs (limit: {MAX_CONCURRENT_JOBS})")
                # Raise exception to return message to queue for retry
                raise Exception(f"Max concurrent jobs limit reached ({MAX_CONCURRENT_JOBS})")

            # Provision EC2 instance
            instance_id = provision_worker(job_id, options)

            # Calculate provisioning duration
            provisioning_duration = time.time() - start_time

            # Update job with instance ID and log information
            log_stream = instance_id  # EC2 instance ID becomes the log stream name
            dynamodb.update_item(
                TableName=DYNAMODB_TABLE,
                Key={'job_id': {'S': job_id}},
                UpdateExpression='SET instance_id = :iid, log_group = :lg, log_stream = :ls',
                ExpressionAttributeValues={
                    ':iid': {'S': instance_id},
                    ':lg': {'S': WORKER_LOG_GROUP},
                    ':ls': {'S': log_stream}
                }
            )

            print(f"[TRACE:{trace_id}] Job {job_id} provisioned successfully, instance {instance_id}")

            # Emit success metrics
            put_metric('JobProvisioned', dimensions=[
                {'Name': 'InstanceType', 'Value': options.get('instance_type', 'unknown')}
            ])
            put_metric('ProvisioningDuration', value=provisioning_duration, unit='Seconds')

        except Exception as e:
            print(f"[TRACE:{trace_id}] Failed to process job: {e}")

            # Emit failure metric
            put_metric('ProvisioningFailed', dimensions=[
                {'Name': 'ErrorType', 'Value': type(e).__name__}
            ])

            # Update job status to failed
            try:
                dynamodb.update_item(
                    TableName=DYNAMODB_TABLE,
                    Key={'job_id': {'S': job_id}},
                    UpdateExpression='SET #status = :status, error = :error',
                    ExpressionAttributeNames={'#status': 'status'},
                    ExpressionAttributeValues={
                        ':status': {'S': 'failed'},
                        ':error': {'S': f'Provisioning failed: {str(e)}'}
                    }
                )
            except Exception as update_error:
                print(f"[TRACE:{trace_id}] Failed to update job status: {update_error}")

            # Re-raise to trigger SQS retry mechanism
            raise

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Provisioning complete'})
    }
