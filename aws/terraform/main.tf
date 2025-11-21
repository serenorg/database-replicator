terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# KMS key for encrypting sensitive data at rest
resource "aws_kms_key" "replication_data" {
  description             = "KMS key for encrypting replication job credentials and sensitive data"
  deletion_window_in_days = 10
  enable_key_rotation     = true

  tags = {
    Name      = "${var.project_name}-data-key"
    ManagedBy = "terraform"
    Project   = var.project_name
  }
}

# KMS key alias for easier reference
resource "aws_kms_alias" "replication_data" {
  name          = "alias/${var.project_name}-data"
  target_key_id = aws_kms_key.replication_data.key_id
}

# DynamoDB table for job state
resource "aws_dynamodb_table" "replication_jobs" {
  name         = var.dynamodb_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "job_id"

  attribute {
    name = "job_id"
    type = "S"
  }

  attribute {
    name = "status"
    type = "S"
  }

  attribute {
    name = "created_at"
    type = "S"
  }

  # GSI for querying by status
  global_secondary_index {
    name            = "status-created-index"
    hash_key        = "status"
    range_key       = "created_at"
    projection_type = "ALL"
  }

  # TTL for automatic cleanup (30 days)
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  # Enable encryption at rest with KMS
  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.replication_data.arn
  }

  # Enable point-in-time recovery
  point_in_time_recovery {
    enabled = true
  }

  tags = {
    Name        = "${var.project_name}-jobs"
    ManagedBy   = "terraform"
    Project     = var.project_name
  }
}

# IAM role for Lambda execution
resource "aws_iam_role" "lambda_execution" {
  name = "${var.project_name}-lambda-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name      = "${var.project_name}-lambda-execution"
    ManagedBy = "terraform"
    Project   = var.project_name
  }
}

# IAM policy for Lambda to access DynamoDB and EC2
resource "aws_iam_role_policy" "lambda_policy" {
  name = "${var.project_name}-lambda-policy"
  role = aws_iam_role.lambda_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:GetItem",
          "dynamodb:UpdateItem",
          "dynamodb:Query"
        ]
        Resource = [
          aws_dynamodb_table.replication_jobs.arn,
          "${aws_dynamodb_table.replication_jobs.arn}/index/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "ec2:RunInstances",
          "ec2:DescribeInstances",
          "ec2:CreateTags",
          "ec2:TerminateInstances"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "iam:PassRole"
        ]
        Resource = aws_iam_role.worker_role.arn
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = aws_kms_key.replication_data.arn
      },
      {
        Effect = "Allow"
        Action = [
          "ssm:GetParameter"
        ]
        Resource = "arn:aws:ssm:${var.aws_region}:*:parameter/${var.project_name}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# IAM role for EC2 worker instances
resource "aws_iam_role" "worker_role" {
  name = var.worker_iam_role_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name      = "${var.project_name}-worker"
    ManagedBy = "terraform"
    Project   = var.project_name
  }
}

# IAM policy for worker instances
resource "aws_iam_role_policy" "worker_policy" {
  name = "${var.project_name}-worker-policy"
  role = aws_iam_role.worker_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:UpdateItem",
          "dynamodb:GetItem"
        ]
        Resource = aws_dynamodb_table.replication_jobs.arn
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey"
        ]
        Resource = aws_kms_key.replication_data.arn
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# Instance profile for worker instances
resource "aws_iam_instance_profile" "worker_profile" {
  name = "${var.project_name}-worker-profile"
  role = aws_iam_role.worker_role.name

  tags = {
    Name      = "${var.project_name}-worker-profile"
    ManagedBy = "terraform"
    Project   = var.project_name
  }
}

# Lambda function
resource "aws_lambda_function" "coordinator" {
  filename      = "${path.module}/../lambda/lambda.zip"
  function_name = "${var.project_name}-coordinator"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.11"
  timeout       = 30
  memory_size   = 256

  environment {
    variables = {
      DYNAMODB_TABLE         = aws_dynamodb_table.replication_jobs.name
      WORKER_AMI_ID          = var.worker_ami_id
      WORKER_INSTANCE_TYPE   = var.worker_instance_type
      WORKER_IAM_ROLE        = aws_iam_instance_profile.worker_profile.name
      KMS_KEY_ID             = aws_kms_key.replication_data.key_id
      API_KEY_PARAMETER_NAME = aws_ssm_parameter.api_key.name
      MAX_CONCURRENT_JOBS    = var.max_concurrent_jobs
    }
  }

  tags = {
    Name      = "${var.project_name}-coordinator"
    ManagedBy = "terraform"
    Project   = var.project_name
  }
}

# CloudWatch Log Group for Lambda
resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${aws_lambda_function.coordinator.function_name}"
  retention_in_days = 7

  tags = {
    Name      = "${var.project_name}-lambda-logs"
    ManagedBy = "terraform"
    Project   = var.project_name
  }
}

# API Gateway (HTTP API)
resource "aws_apigatewayv2_api" "api" {
  name          = "${var.project_name}-api"
  protocol_type = "HTTP"

  cors_configuration {
    allow_origins = ["*"]
    allow_methods = ["GET", "POST", "OPTIONS"]
    allow_headers = ["content-type"]
  }

  tags = {
    Name      = "${var.project_name}-api"
    ManagedBy = "terraform"
    Project   = var.project_name
  }
}

# API Gateway integration with Lambda
resource "aws_apigatewayv2_integration" "lambda_integration" {
  api_id           = aws_apigatewayv2_api.api.id
  integration_type = "AWS_PROXY"
  integration_uri  = aws_lambda_function.coordinator.invoke_arn
}

# API Gateway route for POST /jobs
resource "aws_apigatewayv2_route" "post_jobs" {
  api_id    = aws_apigatewayv2_api.api.id
  route_key = "POST /jobs"
  target    = "integrations/${aws_apigatewayv2_integration.lambda_integration.id}"
}

# API Gateway route for GET /jobs/{id}
resource "aws_apigatewayv2_route" "get_job" {
  api_id    = aws_apigatewayv2_api.api.id
  route_key = "GET /jobs/{id}"
  target    = "integrations/${aws_apigatewayv2_integration.lambda_integration.id}"
}

# API Gateway stage
resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.api.id
  name        = "$default"
  auto_deploy = true

  # Enable access logging
  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api_logs.arn
    format = jsonencode({
      requestId      = "$context.requestId"
      ip             = "$context.identity.sourceIp"
      requestTime    = "$context.requestTime"
      httpMethod     = "$context.httpMethod"
      routeKey       = "$context.routeKey"
      status         = "$context.status"
      protocol       = "$context.protocol"
      responseLength = "$context.responseLength"
    })
  }

  # Configure throttling at stage level (rate limiting)
  default_route_settings {
    throttling_burst_limit = 100
    throttling_rate_limit  = 50
  }

  tags = {
    Name      = "${var.project_name}-api-stage"
    ManagedBy = "terraform"
    Project   = var.project_name
  }
}

# CloudWatch Log Group for API Gateway logs
resource "aws_cloudwatch_log_group" "api_logs" {
  name              = "/aws/apigateway/${aws_apigatewayv2_api.api.name}"
  retention_in_days = 7

  tags = {
    Name      = "${var.project_name}-api-logs"
    ManagedBy = "terraform"
    Project   = var.project_name
  }
}

# Generate random API key for authentication
resource "random_password" "api_key" {
  length  = 32
  special = false
}

# Store API key in SSM Parameter Store (encrypted)
resource "aws_ssm_parameter" "api_key" {
  name        = "/${var.project_name}/api-key"
  description = "API key for replication service authentication"
  type        = "SecureString"
  value       = random_password.api_key.result
  key_id      = aws_kms_key.replication_data.id

  tags = {
    Name      = "${var.project_name}-api-key"
    ManagedBy = "terraform"
    Project   = var.project_name
  }
}

# Lambda permission for API Gateway
resource "aws_lambda_permission" "api_gateway" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.coordinator.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.api.execution_arn}/*/*"
}
