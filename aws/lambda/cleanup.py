#!/usr/bin/env python3
# ABOUTME: Lambda function to detect and clean up stuck replication jobs
# ABOUTME: Runs periodically via EventBridge to mark stale jobs as failed

import json
import os
from datetime import datetime, timedelta, timezone

import boto3

# AWS clients
dynamodb = boto3.client('dynamodb')
ec2 = boto3.client('ec2')
cloudwatch = boto3.client('cloudwatch')

# Configuration
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE', 'replication-jobs')
MAX_RUNNING_HOURS = int(os.environ.get('MAX_RUNNING_HOURS', '12'))  # Jobs running longer than this are considered stuck
MAX_PENDING_HOURS = int(os.environ.get('MAX_PENDING_HOURS', '1'))   # Jobs pending longer than this are considered stuck


def parse_timestamp(ts_str):
    """Parse ISO timestamp string to datetime object"""
    if not ts_str:
        return None
    try:
        return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        return None


def is_instance_terminated(instance_id):
    """Check if EC2 instance is terminated or terminating"""
    try:
        response = ec2.describe_instances(InstanceIds=[instance_id])
        if response['Reservations']:
            state = response['Reservations'][0]['Instances'][0]['State']['Name']
            return state in ['terminated', 'terminating', 'stopping', 'stopped']
        return True  # Instance doesn't exist
    except ec2.exceptions.ClientError:
        return True  # Instance not found


def mark_job_failed(job_id, reason):
    """Mark a job as failed with the given reason"""
    print(f"Marking job {job_id} as failed: {reason}")

    try:
        dynamodb.update_item(
            TableName=DYNAMODB_TABLE,
            Key={'job_id': {'S': job_id}},
            UpdateExpression='SET #status = :status, #error = :error, updated_at = :timestamp',
            ExpressionAttributeNames={
                '#status': 'status',
                '#error': 'error'
            },
            ExpressionAttributeValues={
                ':status': {'S': 'failed'},
                ':error': {'S': reason},
                ':timestamp': {'S': datetime.now(timezone.utc).isoformat()}
            }
        )

        # Emit metric
        cloudwatch.put_metric_data(
            Namespace='SerenReplication',
            MetricData=[{
                'MetricName': 'StuckJobCleaned',
                'Value': 1,
                'Unit': 'Count',
                'Dimensions': [{'Name': 'JobId', 'Value': job_id}]
            }]
        )

        return True
    except Exception as e:
        print(f"Failed to mark job {job_id} as failed: {str(e)}")
        return False


def cleanup_stuck_jobs():
    """Find and clean up stuck jobs"""
    now = datetime.now(timezone.utc)
    cleaned_count = 0
    scanned_count = 0

    # Scan for jobs in non-terminal states
    try:
        paginator = dynamodb.get_paginator('scan')
        pages = paginator.paginate(
            TableName=DYNAMODB_TABLE,
            FilterExpression='#status IN (:pending, :running)',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={
                ':pending': {'S': 'pending'},
                ':running': {'S': 'running'}
            }
        )

        for page in pages:
            for item in page.get('Items', []):
                scanned_count += 1
                job_id = item['job_id']['S']
                status = item['status']['S']
                created_at = parse_timestamp(item.get('created_at', {}).get('S'))
                instance_id = item.get('instance_id', {}).get('S')

                if not created_at:
                    print(f"⚠ Job {job_id} has no created_at timestamp, skipping")
                    continue

                age_hours = (now - created_at).total_seconds() / 3600

                # Check for stuck pending jobs
                if status == 'pending' and age_hours > MAX_PENDING_HOURS:
                    reason = f"Job stuck in pending state for {age_hours:.1f} hours (max: {MAX_PENDING_HOURS})"
                    if mark_job_failed(job_id, reason):
                        cleaned_count += 1

                # Check for stuck running jobs
                elif status == 'running' and age_hours > MAX_RUNNING_HOURS:
                    # Additional check: is the instance still running?
                    if instance_id:
                        instance_terminated = is_instance_terminated(instance_id)
                        if instance_terminated:
                            reason = f"Job instance {instance_id} terminated but job still marked as running"
                        else:
                            reason = f"Job running for {age_hours:.1f} hours (max: {MAX_RUNNING_HOURS}), assuming stuck"
                    else:
                        reason = f"Job stuck in running state for {age_hours:.1f} hours with no instance"

                    if mark_job_failed(job_id, reason):
                        cleaned_count += 1

        print(f"✓ Cleanup complete: scanned {scanned_count} jobs, cleaned {cleaned_count} stuck jobs")

        # Emit summary metrics
        cloudwatch.put_metric_data(
            Namespace='SerenReplication',
            MetricData=[
                {
                    'MetricName': 'StuckJobsFound',
                    'Value': cleaned_count,
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'JobsScanned',
                    'Value': scanned_count,
                    'Unit': 'Count'
                }
            ]
        )

        return {'cleaned': cleaned_count, 'scanned': scanned_count}

    except Exception as e:
        print(f"ERROR: Failed to scan DynamoDB: {str(e)}")
        raise


def lambda_handler(event, context):
    """Lambda handler for scheduled cleanup"""
    print(f"Starting stuck job cleanup - Max running hours: {MAX_RUNNING_HOURS}, Max pending hours: {MAX_PENDING_HOURS}")

    result = cleanup_stuck_jobs()

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Cleanup complete',
            'jobs_cleaned': result['cleaned'],
            'jobs_scanned': result['scanned']
        })
    }
