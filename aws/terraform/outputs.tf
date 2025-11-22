output "api_endpoint" {
  description = "API Gateway endpoint URL for remote replication"
  value       = aws_apigatewayv2_api.api.api_endpoint
}

output "dynamodb_table_name" {
  description = "DynamoDB table name for job state"
  value       = aws_dynamodb_table.replication_jobs.name
}

output "lambda_function_name" {
  description = "Lambda function name"
  value       = aws_lambda_function.coordinator.function_name
}

output "worker_iam_role_name" {
  description = "IAM role name for worker instances"
  value       = aws_iam_role.worker_role.name
}

output "worker_instance_profile_name" {
  description = "IAM instance profile name for worker instances"
  value       = aws_iam_instance_profile.worker_profile.name
}

output "api_key" {
  description = "API key for authenticating requests (store securely)"
  value       = random_password.api_key.result
  sensitive   = true
}

output "kms_key_id" {
  description = "KMS key ID for encrypting sensitive data"
  value       = aws_kms_key.replication_data.key_id
}

output "provisioning_queue_url" {
  description = "SQS queue URL for job provisioning"
  value       = aws_sqs_queue.provisioning_queue.url
}

output "provisioning_dlq_url" {
  description = "SQS dead letter queue URL for failed provisions"
  value       = aws_sqs_queue.provisioning_dlq.url
}
