data "aws_caller_identity" "current" {}


data "aws_region" "current" {}


data "archive_file" "src_zip" {
    type        = "zip"
    source_dir = "${path.module}/../src"
    output_path = "${path.module}/../data/src.zip"
    excludes = [
        "__pycache__"
    ]
}


data "archive_file" "extract_zip" {
    type        = "zip"
    source_dir = var.extract_archive_source_path
    output_path = var.extract_archive_output_path
    depends_on = [
        null_resource.install_dependencies,
        null_resource.copy_src
    ]
}


data "archive_file" "transform_zip" {
    type        = "zip"
    source_dir = var.transform_archive_source_path
    output_path = var.transform_archive_output_path
    depends_on = [
        null_resource.install_dependencies,
        null_resource.copy_src
    ]
}


data "archive_file" "load_zip" {
    type        = "zip"
    source_dir = var.load_archive_source_path
    output_path = var.load_archive_output_path
    depends_on = [
        null_resource.install_dependencies,
        null_resource.copy_src
    ]
}


data "aws_iam_policy_document" "read_from_s3_document" {
    statement {
        actions = [
            "s3:GetObject"
        ]

        resources = [
            "${aws_s3_bucket.ingested_data_bucket.arn}/*",
            "${aws_s3_bucket.processed_data_bucket.arn}/*"
        ]
    }
}


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
            "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.extract_lambda_name}:*",
            "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.transform_lambda_name}:*",
            "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.load_lambda_name}:*"
        ]
    }
}


data "aws_iam_policy_document" "access_secretsmanager_document" {
    statement {
        actions = [
            "secretsmanager:GetSecretValue"
        ]

        resources = [
            "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
        ]
    }
}


data "aws_iam_policy_document" "allow_all_s3_actions_document" {
    statement {
        actions = [
            "s3:*"
        ]

        resources = [
            "${aws_s3_bucket.ingested_data_bucket.arn}/*",
            "${aws_s3_bucket.processed_data_bucket.arn}/*"
        ]
    }
}


data "aws_iam_policy_document" "list_all_buckets_document" {
    statement {
        actions = [
            "s3:ListAllMyBuckets"
        ]

        resources = [
            "arn:aws:s3:::*"
        ]
    }
}


data "aws_iam_policy_document" "list_root_objects_document" {
    statement {
        actions = [
            "s3:ListBucket"
        ]

        resources = [
            "${aws_s3_bucket.ingested_data_bucket.arn}",
            "${aws_s3_bucket.processed_data_bucket.arn}"
        ]
    }
}
