data "aws_caller_identity" "current" {}


data "aws_region" "current" {}


data "archive_file" "extraction_zip" {
    type        = "zip"
    source_file = "${path.module}/../src/extraction.py"
    output_path = "${path.module}/../data/extraction.zip"
}


data "archive_file" "transform_zip" {
    type        = "zip"
    source_file = "${path.module}/../src/transform.py"
    output_path = "${path.module}/../data/transform.zip"
}


data "archive_file" "load_zip" {
    type        = "zip"
    source_file = "${path.module}/../src/load.py"
    output_path = "${path.module}/../data/load.zip"
}


data "aws_iam_policy_document" "read_from_s3_document" {
    statement {
        actions = [
            "s3:GetObject"
        ]

        resources = [
            "${aws_s3_bucket.code_bucket.arn}/*"
        ]
    }
}


# probably need write to s3 document in future - placeholder here


data "aws_iam_policy_document" "log_to_cloudwatch_document" {
    statement {
        actions = [
            "logs:CreateLogGroup"
        ]

        resources = [
            "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
        ]
    }

    statement {
        actions = [
            "logs:CreateLogStream", 
            "logs:PutLogEvents"
        ]

        resources = [
            "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.extraction_lambda_name}:*",
            "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.transform_lambda_name}:*",
            "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.load_lambda_name}:*"
        ]
    }
}
