# Lambda Function for Remote Replication

This Lambda function orchestrates remote replication jobs.

## Environment Variables

- `DYNAMODB_TABLE`: DynamoDB table name (default: replication-jobs)
- `WORKER_AMI_ID`: AMI ID for worker instances (required)
- `WORKER_INSTANCE_TYPE`: EC2 instance type (default: c5.2xlarge)
- `WORKER_IAM_ROLE`: IAM role name for workers (default: seren-replication-worker)

## Deployment

```bash
# Package Lambda
cd aws/lambda
zip -r lambda.zip handler.py

# Upload to AWS (replace with your function name)
aws lambda update-function-code \
  --function-name seren-replication-coordinator \
  --zip-file fileb://lambda.zip
```

## Testing Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run tests (TBD)
python -m pytest
```
