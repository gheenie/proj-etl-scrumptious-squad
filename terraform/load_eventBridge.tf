resource "aws_cloudwatch_event_rule" "load_scheduler" {
    name_prefix         = "load-scheduler-"
    schedule_expression = "rate(3 minutes)"
}

resource "aws_cloudwatch_event_target" "load_lambda_target" {
    rule  = aws_cloudwatch_event_rule.load_scheduler.name
    arn   = aws_lambda_function.load_lambda.arn
    input = jsonencode({
        "secret_id": "cred_DW",
        "bucket_prefix": "scrumptious-squad-pr-data-"
    })
}

resource "aws_lambda_permission" "allow_load_scheduler" {
    action         = "lambda:InvokeFunction"
    principal      = "events.amazonaws.com"
    source_arn     = aws_cloudwatch_event_rule.load_scheduler.arn
    function_name  = aws_lambda_function.load_lambda.function_name
    source_account = data.aws_caller_identity.current.account_id
}