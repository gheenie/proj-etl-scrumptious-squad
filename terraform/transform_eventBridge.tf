# resource "aws_lambda_function" "transform_lambda_function" {
#     function_name = "transform-test"
#     role = aws_iam_role.lambda_role.arn
#     handler = "var.transform_lambda_name.lambda_handler"
#     runtime = "python3.9"
#     s3_bucket = aws_s3_bucket.code_bucket.bucket
#     s3_key = aws_s3_object.transform_code.key
# }

resource "aws_cloudwatch_event_rule" "scheduler" {
    name_prefix = "transform-scheduler-"
    schedule_expression = "rate(1 minute)"
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.scheduler.name
  arn       = aws_lambda_function.transform_lambda_function.arn
}
resource "aws_lambda_permission" "allow_scheduler" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.transform_lambda_function.function_name
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}