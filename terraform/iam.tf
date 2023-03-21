resource "aws_iam_role" "lambda_role" {
    name_prefix = "lambda-role-"

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
    policy = data.aws_iam_policy_document.read_from_s3_document.json
}


# probably need write to s3 policy in future - placeholder here


resource "aws_iam_policy" "log_to_cloudwatch_policy" {
    name_prefix = "log-to-cloudwatch-policy-"
    policy = data.aws_iam_policy_document.log_to_cloudwatch_document.json
}


resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
    role = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.read_from_s3_policy.arn
}


# probably need write to s3 attachment in future - placeholder here


resource "aws_iam_role_policy_attachment" "lambda_cloudwatch_policy_attachment" {
    role = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.log_to_cloudwatch_policy.arn
}