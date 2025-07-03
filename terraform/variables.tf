variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-2"
}

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "chess-glicko"
}

variable "notification_email" {
  description = "Email for pipeline notifications"
  type        = string
  default     = ""
}

variable "schedule_expression" {
  description = "EventBridge schedule expression for monthly runs"
  type        = string
  default     = "cron(0 0 1 * ? *)"  # 1st of each month at 00:00 UTC
}

variable "task_cpu" {
  description = "CPU units for ECS task (1024 = 1 vCPU)"
  type        = string
  default     = "1024"
}

variable "task_memory" {
  description = "Memory for ECS task in MiB"
  type        = string
  default     = "2048"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "prod"
}

# Outputs
output "ecr_repository_url" {
  description = "ECR repository URL"
  value       = aws_ecr_repository.chess_pipeline.repository_url
}

output "s3_bucket_name" {
  description = "S3 bucket name"
  value       = aws_s3_bucket.chess_data.id
}

output "ecs_cluster_name" {
  description = "ECS cluster name"
  value       = aws_ecs_cluster.main.name
}

output "task_definition_arn" {
  description = "ECS task definition ARN"
  value       = aws_ecs_task_definition.chess_pipeline.arn
}

output "sns_topic_arn" {
  description = "SNS topic ARN for notifications"
  value       = aws_sns_topic.notifications.arn
}

output "manual_run_command" {
  description = "Command to manually run the pipeline"
  value = <<EOF
aws ecs run-task \
  --cluster ${aws_ecs_cluster.main.name} \
  --task-definition ${aws_ecs_task_definition.chess_pipeline.family} \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[${join(",", data.aws_subnets.default.ids)}],assignPublicIp=ENABLED,securityGroups=[${aws_security_group.ecs_tasks.id}]}" \
  --overrides '{"containerOverrides":[{"name":"chess-glicko","environment":[{"name":"PROCESS_MONTH","value":"2024-01"}]}]}'
EOF
}