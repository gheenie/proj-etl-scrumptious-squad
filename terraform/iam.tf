resource "aws_iam_role" "lambda_role" {
    name_prefix        = "lambda-role-"
    assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}


resource "aws_iam_policy" "read_from_s3_policy" {
    name_prefix = "read-from-s3-policy-"
    policy      = data.aws_iam_policy_document.read_from_s3_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
    role       = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.read_from_s3_policy.arn
}


resource "aws_iam_policy" "log_to_cloudwatch_policy" {
    name_prefix = "log-to-cloudwatch-policy-"
    policy      = data.aws_iam_policy_document.log_to_cloudwatch_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_cloudwatch_policy_attachment" {
    role       = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.log_to_cloudwatch_policy.arn
}


resource "aws_iam_policy" "access_secretsmanager_policy" {
    name_prefix = "access-secretsmanager-policy-"
    policy      = data.aws_iam_policy_document.access_secretsmanager_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_secretsmanager_policy_attachment" {
    role       = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.access_secretsmanager_policy.arn
}



resource "aws_iam_policy" "allow_all_s3_actions_policy" {
    name_prefix = "allow-all-s3-actions-policy-"
    policy      = data.aws_iam_policy_document.allow_all_s3_actions_document.json
}

# This will allow the lambda to upload .parquet files to both our buckets,
# but it's not following the principle of least privileges.
resource "aws_iam_role_policy_attachment" "lambda_allow_all_s3_actions_policy_attachment" {
    role       = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.allow_all_s3_actions_policy.arn
}


resource "aws_iam_policy" "list_all_buckets_policy" {
    name_prefix = "list-all-buckets-policy-"
    policy      = data.aws_iam_policy_document.list_all_buckets_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_list_all_buckets_policy_attachment" {
    role       = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.list_all_buckets_policy.arn
}


resource "aws_iam_policy" "list_root_objects_policy" {
    name_prefix = "list-root-objects-policy-"
    policy      = data.aws_iam_policy_document.list_root_objects_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_list_root_objects_policy_attachment" {
    role       = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.list_root_objects_policy.arn
}




