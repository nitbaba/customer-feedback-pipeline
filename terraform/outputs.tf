output "s3_bucket_name" {
    value       = aws_s3_bucket.data_lake.id
    description = "Target bucket string name for streaming config"
}

output "rds_endpoint" {
    value       = aws_db_instance.serving_sink.endpoint
    description = "Database connection hostname connection string"
}
