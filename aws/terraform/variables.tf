variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "seren-replication"
}

variable "dynamodb_table_name" {
  description = "DynamoDB table name for job state"
  type        = string
  default     = "replication-jobs"
}

variable "worker_ami_id" {
  description = "AMI ID for worker EC2 instances (must have postgres-seren-replicator installed)"
  type        = string
}

variable "worker_instance_type" {
  description = "EC2 instance type for worker instances"
  type        = string
  default     = "c5.2xlarge"
}

variable "worker_iam_role_name" {
  description = "IAM role name for worker instances"
  type        = string
  default     = "seren-replication-worker"
}

variable "max_concurrent_jobs" {
  description = "Maximum number of concurrent replication jobs (cost control)"
  type        = number
  default     = 10
}
