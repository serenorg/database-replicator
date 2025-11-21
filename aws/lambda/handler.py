"""
ABOUTME: AWS Lambda function for remote replication job orchestration
ABOUTME: Handles POST /jobs (submit) and GET /jobs/{id} (status) requests
"""

import json
import uuid
import time
import boto3
import os
from datetime import datetime

# AWS clients
dynamodb = boto3.client('dynamodb')
ec2 = boto3.client('ec2')

# Configuration from environment variables
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE', 'replication-jobs')
WORKER_AMI_ID = os.environ.get('WORKER_AMI_ID', 'ami-xxxxxxxxx')
WORKER_INSTANCE_TYPE = os.environ.get('WORKER_INSTANCE_TYPE', 'c5.2xlarge')
WORKER_IAM_ROLE = os.environ.get('WORKER_IAM_ROLE', 'seren-replication-worker')


def lambda_handler(event, context):
    """Main Lambda handler - routes requests to appropriate handler"""

    http_method = event.get('httpMethod', '')
    path = event.get('path', '')

    print(f"Request: {http_method} {path}")

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
            'body': json.dumps({'error': str(e)})
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

    # Create job record in DynamoDB
    now = datetime.utcnow().isoformat() + 'Z'
    ttl = int(time.time()) + (30 * 86400)  # 30 days

    dynamodb.put_item(
        TableName=DYNAMODB_TABLE,
        Item={
            'job_id': {'S': job_id},
            'status': {'S': 'provisioning'},
            'command': {'S': body['command']},
            'source_url': {'S': body['source_url']},
            'target_url': {'S': body['target_url']},
            'filter': {'S': json.dumps(body.get('filter', {}))},
            'options': {'S': json.dumps(body.get('options', {}))},
            'created_at': {'S': now},
            'ttl': {'N': str(ttl)},
        }
    )

    # Provision EC2 instance
    try:
        instance_id = provision_worker(job_id, body)

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
                ':error': {'S': f'Provisioning failed: {str(e)}'}
            }
        )
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Provisioning failed: {str(e)}'})
        }

    return {
        'statusCode': 201,
        'body': json.dumps({
            'job_id': job_id,
            'status': 'provisioning'
        })
    }


def provision_worker(job_id, job_spec):
    """Provision EC2 instance to run replication job"""

    # Build user data script
    user_data = f"""#!/bin/bash
set -euo pipefail

# Write job spec to file
cat > /tmp/job_spec.json <<'EOF'
{json.dumps(job_spec)}
EOF

# Execute worker script
/opt/seren-replicator/worker.sh "{job_id}" /tmp/job_spec.json
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

    # Convert DynamoDB item to JSON
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
