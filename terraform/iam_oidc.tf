# 1. CREATE the OIDC Identity Provider instead of reading it as data
resource "aws_iam_openid_connect_provider" "github" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"] # GitHub's verified OIDC thumbprint
}

# 2. Define the IAM Trust Policy referencing our managed resource
data "aws_iam_policy_document" "github_allow" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn] # Pointing to the resource block
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:nitbaba/customer-feedback-pipeline:ref:refs/heads/main"]
    }
  }
}

# 3. Create the deployment IAM Role
resource "aws_iam_role" "github_actions_deploy" {
  name               = "${var.project_name}-${var.environment}-gh-deploy"
  assume_role_policy = data.aws_iam_policy_document.github_allow.json
}

# 4. Attach a policy allowing this role to sync to your S3 buckets
resource "aws_iam_policy" "s3_deploy_policy" {
  name        = "${var.project_name}-${var.environment}-s3-deploy"
  description = "Allows GitHub Actions to sync artifacts to S3"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Resource = [
          "arn:aws:s3:::customer-feedback-pipeline-dev-lake",
          "arn:aws:s3:::customer-feedback-pipeline-dev-lake/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "s3_sync_attach" {
  role       = aws_iam_role.github_actions_deploy.name
  policy_arn = aws_iam_policy.s3_deploy_policy.arn
}

# 5. Output the Role ARN
output "github_actions_role_arn" {
  value       = aws_iam_role.github_actions_deploy.arn
  description = "Value for GitHub Secret: AWS_ROLE_TO_ASSUME"
}