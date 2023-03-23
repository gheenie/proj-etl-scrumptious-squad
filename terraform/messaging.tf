resource "aws_sns_topic" "error_notification" {
    name = "error-alerts"
}

resource "aws_sns_topic_subscription" "error_notification_email_target" {
    topic_arn              = aws_sns_topic.error_notification.arn
    protocol               = "email"
    endpoint               = var.sns_group_email
}
