resource "aws_cloudwatch_event_rule" "transform_scheduler" {
    name_prefix = "transform-scheduler-"
    schedule_expression = "rate(1 minute)"
}

resource "aws_cloudwatch_event_target" "transform_lambda_target" {
    rule = aws_cloudwatch_event_rule.transform_scheduler.name
    arn  = aws_lambda_function.transform_lambda.arn
}

resource "aws_lambda_permission" "allow_transform_scheduler" {
    action = "lambda:InvokeFunction"
    principal = "events.amazonaws.com"
    source_arn = aws_cloudwatch_event_rule.transform_scheduler.arn
    function_name = aws_lambda_function.transform_lambda.function_name
    source_account = data.aws_caller_identity.current.account_id
}
