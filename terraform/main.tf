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

resource "aws_s3_bucket_lifecycle_configuration" "lake_ttl_policies" {
    bucket = aws_s3_bucket.data_lake.id

    #Rule 1: Auto-purge temporary streaming checkpoints and raw landing drops after 7 days
    rule {
        id      = "purge-ephemeral-staging-data"
        status  = "Enabled"

        filter {
            prefix = "checkpoints/"
        }

        #expiration {
        #    days = 7
        #}

        abort_incomplete_multipart_upload {
            days_after_initiation = 7
        }
    }

    #Rule 2: Move historical Bronze data to cold-storage after 30 days
    rule {
        id      = "tier-bronze-cold-storage"
        status  = "Enabled"

        filter {            
            prefix = "bronze/"            
        }

        transition {
            days            = 30
            storage_class   = "INTELLIGENT_TIERING"
        }
    }

    #Rule 3: Move historical Silver data to cold-storage after 30 days
    rule {
        id      = "tier-silver-cold-storage"
        status  = "Enabled"

        filter {
            prefix = "silver/"
        }

        transition {
            days            = 30
            storage_class   = "INTELLIGENT_TIERING"
        }
    }
}

#===============================================
# 2. NETWORKING & SECURITY GROUPS
#===============================================
data "aws_vpc" "default" {
    default = true
}

resource "aws_security_group" "airflow_runned_sg" {
    name        = "${var.project_name}-${var.environment}-airflow-sg"
    description = "Security group for Airflow engine to access relational data sinks"
    vpc_id      = data.aws_vpc.default.id

    egress {
        from_port   = 0
        to_port     = 0
        protocol    = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }

    tags = {
        Name        = "${var.project_name}-${var.environment}-airflow-sg"
        Environment = var.environment
    }
}

resource "aws_security_group" "emr_engine_sg" {
    name        = "${var.project_name}-${var.environment}-emr-sg"
    description = "Security group for transient compute cluster nodes"
    vpc_id      = data.aws_vpc.default.id

    egress {
        from_port   = 0
        to_port     = 0
        protocol    = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }

    tags = {
        Name        = "${var.project_name}-${var.environment}-emr-sg"
        Environment = var.environment
    }
}

resource "aws_security_group" "rds_sg" {
    name        = "${var.project_name}-${var.environment}-rds-sg"
    description = "Controls inbound database traffic from local engines"
    vpc_id      = data.aws_vpc.default.id

    #Inbound rule allowing PostgreSQL access from specific IP or security group
    ingress {
        description = "Allow bounded PostgreSQL traffic from desingate compute"
        from_port   = 5432
        to_port     = 5432
        protocol    = "tcp"

        security_groups = [
            aws_security_group.airflow_runned_sg.id,
            aws_security_group.emr_engine_sg.id
        ]
    }
    
    egress {
        from_port   = 0
        to_port     = 0
        protocol    = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }

    tags = {
        Environment = "Development"
        Pipeline    = "CustomerFeedbackAnalytics"
    }
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

#===============================================
# 4. DASHBOARD CREDENTIALS MANAGEMENT
#===============================================
resource "random_password" "dashboard_reader_pwd" {
  length           = 24
  special          = true
  override_special = "!#$%&*()-_=+[]{}?:"
}

resource "aws_secretsmanager_secret" "dashboard_secret" {
    name                    = "${var.project_name}-${var.environment}-dashboard-credentials"
    description             = "Read only db credentials for BI visualization tools"
    recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "dashboard_secret_val" {
    secret_id       = aws_secretsmanager_secret.dashboard_secret.id
    secret_string   = jsonencode({
        username = "dashboard_reader"
        password = random_password.dashboard_reader_pwd.result
        endpoint = aws_db_instance.serving_sink.endpoint
        database = "feedback_analytics"
    })
}