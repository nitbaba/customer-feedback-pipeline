provider "aws" {
    region = var.aws_region
}

#===============================================
# 1. DATA LAKE STORAGE TIER (AWS S3)
#===============================================
resource "aws_s3_bucket" "data_lake" {
    bucket          = "${var.project_name}-${var.environment}-lake"
    force_destroy   = true #Allows clean teardown of data blocks during testing

    tags = {
        Name        = "Medallion Data Lake"
        Environment = var.environment
    }
}

#Generate folder hierarchy prefixes inside the bucket
resource "aws_s3_object" "lake_tiers" {
    for_each = toset(["landing/", "bronze/", "silver/", "gold/", "checkpoints/bronze/", "checkpoints/silver/"])
    bucket   = aws_s3_bucket.data_lake.id
    key      = each.value
}

#===============================================
# 2. NETWORKING & SECURITY GROUPS
#===============================================
data "aws_vpc" "default" {
    default = true
}

resource "aws_security_group" "rds_sg" {
    name        = "${var.project_name}-${var.environment}-rds-sg"
    description = "Controls inbound database traffic from local engines"
    vpc_id      = data.aws_vpc.default.id

    #Inbound rule allowing PostgreSQL access from specific IP or security group
    ingress {
        from_port   = 5432
        to_port     = 5432
        protocol    = "tcp"
        cidr_blocks = ["0.0.0.0/0"] #TODO open for local testing, tighten when deployed
    }

    egress {
        from_port   = 0
        to_port     = 0
        protocol    = "-1"
        cidr_blocks = ["0.0.0.0/0"] #TODO open for local testing, tighten when deployed
    }
}

resource "aws_secretsmanager_secret" "db_secret" {
    name                        = "${var.project_name}-${var.environment}-db-credentials"
    description                 = "Managed relational credentials for feddback analytics"
    recovery_window_in_days   = 0 #Immediate deletion if stack is destroyed during testing
}

#Sensitive config keys as encrypted JSON payload
resource "aws_secretsmanager_secret_version" "db_secret_val" {
    secret_id       = aws_secretsmanager_secret.db_secret.id
    secret_string   = jsonencode({
        username = aws_db_instance.serving_sink.username
        password = var.db_password
        endpoint = aws_db_instance.serving_sink.endpoint
    })
}

#===============================================
# 3. ANALYTICS SERVING SINK (AWS RDS PostgreSQL)
#===============================================
resource "aws_db_instance" "serving_sink" {
    identifier              = "${var.project_name}-${var.environment}-sink"
    engine                  = "postgres"
    engine_version          = "15.7"
    instance_class          = "db.t4g.micro" #cost-efficient, lighweight
    allocated_storage       = 20
    max_allocated_storage   = 100
    db_name                 = "feedback_analytics"
    username                = "pipeline_admin"
    password                = var.db_password
    parameter_group_name    = "default.postgres15"
    vpc_security_group_ids  = [aws_security_group.rds_sg.id]
    skip_final_snapshot     = true
    publicly_accessible     = true #TODO opened for WSL2 reasons

    tags = {
        Name        = "Analytical Gold Serving Sink"
        Environment = var.environment
    }
}