output "s3_bucket_name" {
    value       = aws_s3_bucket.data_lake.id
    description = "Target bucket string name for streaming config"
}

output "rds_endpoint" {
    value       = aws_db_instance.serving_sink.endpoint
    description = "Database connection hostname connection string"
}

output "emr_master_security_group_id" {
  value       = aws_security_group.emr_master.id  # Replace with your actual resource identifier
  description = "The security group ID assigned to the EMR Primary node"
}

output "emr_slave_security_group_id" {
  value       = aws_security_group.emr_slave.id   # Replace with your actual resource identifier
  description = "The security group ID assigned to EMR Core and Task nodes"
}