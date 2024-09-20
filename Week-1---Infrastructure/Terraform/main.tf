terraform {
  required_version = ">= 0.13"

}


provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_requesting_account_id  = true

  endpoints {
    apigateway       = "http://localhost:4566"
    cloudformation   = "http://localhost:4566"
    cloudwatch       = "http://localhost:4566"
    dynamodb         = "http://localhost:4566"
    ec2              = "http://localhost:4566"
    es               = "http://localhost:4566"
    firehose         = "http://localhost:4566"
    iam              = "http://localhost:4566"
    kinesis          = "http://localhost:4566"
    lambda           = "http://localhost:4566"
    route53          = "http://localhost:4566"
    redshift         = "http://localhost:4566"
    s3               = "http://localhost:4566"
    secretsmanager   = "http://localhost:4566"
    ses              = "http://localhost:4566"
    sns              = "http://localhost:4566"
    sqs              = "http://localhost:4566"
    ssm              = "http://localhost:4566"
    stepfunctions    = "http://localhost:4566"
    sts              = "http://localhost:4566"
    cloudwatchlogs   = "http://localhost:4566"
    cloudwatchevents = "http://localhost:4566"
  }
}


# data "aws_caller_identity" "current_identity" {}
# data "aws_region" "current_region" {}


locals {
  tags = {
    Environment  = var.environment
    Group        = "DE Camping"
    OwnerEmail   = "data.sfoundation@gmail.com"
    PipelineRepo = "https://github.com/longbuivan/dotfile.git"
    Workload     = "Mini Project"
  }
  # account_id = data.aws_caller_identity.current_identity.account_id
  # region     = data.aws_region.current_region.name

  table_catalog = {
    Source = "Localstack"
    Pipeline = "Data Engineering Pipeline"
  }


}


resource "aws_iam_policy" "lambda_execution_policy" {
  name = "lambda-execution-policy"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "s3:ListBucket",
        "s3:GetObject",
        "s3:CopyObject",
        "s3:HeadObject"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::${var.environment}-web-raw-data-s3.id}",
        "arn:aws:s3:::${var.environment}-web-raw-data-s3.id}/*"
      ]
    },
    {
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Effect": "Allow",
      "Resource": "*"
    },
    {
      "Action": [
        "firehose:PutRecord",
        "firehose:PutRecordBatch"
      ],
      "Effect": "Allow",
      "Resource": "*"
    }
  ]
}
EOF

}

resource "aws_kinesis_stream" "web_raw_streaming" {
  name             = "web_raw_streaming"
  shard_count      = 1
  retention_period = 24

  shard_level_metrics = [
    "IncomingBytes",
    "OutgoingBytes",
    "OutgoingRecords",
    "IncomingRecords"
  ]


  tags = {
    Environment = var.environment,
    Workload    = "Data Camping"
  }
}

data "archive_file" "flatting_data" {
  type = "zip"
  source {
    content  = file("../src/flatting_data.py")
    filename = "src/flatting_data.py"
  }

  output_path = "./flatting_data.zip"

}

data "archive_file" "web_ingesting" {
  type = "zip"
  source {
    content  = file("../src/web_ingesting.py")
    filename = "src/web_ingesting.py"
  }

  output_path = "./web_ingesting.zip"

}

resource "aws_lambda_function" "web_raw_ingesting_lambda" {
  filename         = data.archive_file.web_ingesting.output_path
  function_name    = "web_raw_ingesting_lambda"
  source_code_hash = data.archive_file.web_ingesting.output_base64sha256
  role             = aws_iam_role.lambda_execution_role.arn
  handler          = "src/web_ingesting.lambda_handler"
  runtime          = "python3.8"
  timeout          = 60
  memory_size      = 128

  environment {
    variables = {
      WEB_RAW_KINESIS = aws_kinesis_stream.web_raw_streaming.name
      WEB_ENDPOINT    = var.web_data_endpoint
    }
  }
  depends_on = [
    aws_iam_role.lambda_execution_role,
    aws_iam_role_policy_attachment.lambda_iam_policy_basic_execution,
    aws_iam_policy.lambda_execution_policy,
    # aws_lambda_permission.allow_cloudwatch_to_invoke_lambda_permission,
  ]
  tags = local.tags
}


resource "aws_lambda_function" "flatting_data_lambda" {
  function_name    = "flatting_data_lambda"
  filename         = data.archive_file.flatting_data.output_path
  runtime          = "python3.8"
  handler          = "src/flatting_data.lambda_handler"
  timeout          = 60
  memory_size      = 128
  source_code_hash = data.archive_file.flatting_data.output_base64sha256
  role             = aws_iam_role.lambda_execution_role.arn
  depends_on = [
    aws_iam_role.lambda_execution_role,
    aws_iam_role_policy_attachment.lambda_iam_policy_basic_execution,
    aws_iam_policy.lambda_execution_policy,
    # aws_lambda_permission.allow_cloudwatch_to_invoke_lambda_permission,
  ]
  tags = local.tags
}
