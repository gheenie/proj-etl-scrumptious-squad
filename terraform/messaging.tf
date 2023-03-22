resource "aws_sns_topic" "error_alerts" {
    name = "error-alerts"
}

resource "aws_sns_topic_subscription" "error_alerts_email_target" {
    topic_arn              = aws_sns_topic.error_alerts.arn
    protocol               = "email"
    endpoint               = var.sns_group_email
}
