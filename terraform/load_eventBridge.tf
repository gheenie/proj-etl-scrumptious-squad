resource "aws_cloudwatch_event_rule" "load_scheduler" {
    name_prefix = "load-scheduler-"
    schedule_expression = "rate(1 minute)"
}

resource "aws_cloudwatch_event_target" "load_lambda_target" {
    rule = aws_cloudwatch_event_rule.load_scheduler.name
    arn  = aws_lambda_function.load_lambda.arn
}

resource "aws_lambda_permission" "allow_load_scheduler" {
    action = "lambda:InvokeFunction"
    principal = "events.amazonaws.com"
    source_arn = aws_cloudwatch_event_rule.load_scheduler.arn
    function_name = aws_lambda_function.load_lambda.function_name
    source_account = data.aws_caller_identity.current.account_id
}