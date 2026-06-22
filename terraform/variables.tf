variable "aws_region" {
    type        = string
    default     = "us-east-1"
    description = "The tarfet AWS region for deployment"
}

variable "project_name" {
    type        = string
    default     = "customer-feedback-pipeline"
    description = "Project prefix applied to resource naming keys"
}

variable "environment" {
    type        = string
    default     = "dev"
    description = "Deployment workspace deployment identifier"
}

variable "db_password" {
    type        = string
    sensitive   = true
    description = "The root master password for the RDS PostgreSQL instance"
}
