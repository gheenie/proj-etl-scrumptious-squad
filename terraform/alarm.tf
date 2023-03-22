# Runtime for all functions
resource "aws_cloudwatch_log_metric_filter" "runtime_error" {
  name = "runtime-error"
  pattern = "Runtime Error"
  log_group_name = "integration-group"

  metric_transformation {
    name = "Exceeded_Allowed_Runtime"
    namespace = "scrumptious-space"
    value = "1"
  }
  depends_on = [aws_cloudwatch_log_group.integration-group]
}
resource "aws_cloudwatch_metric_alarm" "alert_runtime_errors" {
  alarm_name = "alert_runtime_errors"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods = "1"
  metric_name = "Multiple_Of_Three"
  namespace = "scrumptious-space"
  period = "60"
  statistic = "Sum"
  threshold = "1"
  alarm_actions = ["arn:aws:sns:us-east-1:${var.subscription_arn}:test-error-alerts"]
  alarm_description = "Oh no! We've passed our max runtime!"
}
resource "aws_cloudwatch_metric_alarm" "alert_nearing_max_runtime" {
  alarm_name = "alert_nearing_max_runtime"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods = "1"
  metric_name = "Duration"
  namespace = "AWS/Lambda"
  period = "60"
  statistic = "Maximum"
  threshold = "600"
  alarm_actions = ["arn:aws:sns:us-east-1:${var.subscription_arn}:test-error-alerts"]
  alarm_description = "Uh-oh! We're nearing a runtime error!"
}


# Data Integrity

resource "aws_cloudwatch_log_metric_filter" "data-integrity-metric-filter" {
  name           = "data-integrity-metric-filter"
  log_group_name = "extraction-group"
  pattern        = "Data Integrity Violation"
  metric_transformation {
    name      = "data-integrity-metric"
    namespace = "LogMetrics"
    value     = "1"
  }
  depends_on = [aws_cloudwatch_log_group.extraction-group]
}
resource "aws_cloudwatch_metric_alarm" "data-integrity-alarm" {
  alarm_name          = "data-integrity-alarm"
  metric_name         = "data-integrity-metric"
  threshold           = "0"
  statistic           = "Sum"
  comparison_operator = "GreaterThanThreshold"
  datapoints_to_alarm = "1"
  evaluation_periods  = "1"
  period              = "60"
  namespace           = "LogMetrics"
  alarm_description   = "Data integrity violation detected"
  alarm_actions       = ["arn:aws:sns:us-east-1:${var.subscription_arn}:test-error-alerts"]
}


# Validation

resource "aws_cloudwatch_log_metric_filter" "data-validation-metric-filter" {
  name           = "data-validation-metric-filter"
  log_group_name = "extraction-group"
  pattern        = "Data Validation Failed"
  metric_transformation {
    name      = "data-validation-metric"
    namespace = "LogMetrics"
    value     = "1"
  }
  depends_on = [aws_cloudwatch_log_group.extraction-group]
}
resource "aws_cloudwatch_metric_alarm" "data-validation-alarm" {
  alarm_name          = "data-validation-alarm"
  metric_name         = "data-validation-metric"
  threshold           = "0"
  statistic           = "Sum"
  comparison_operator = "GreaterThanThreshold"
  datapoints_to_alarm = "1"
  evaluation_periods  = "1"
  period              = "60"
  namespace           = "LogMetrics"
  alarm_description   = "Data validation failed"
  alarm_actions       = ["arn:aws:sns:us-east-1:${var.subscription_arn}:test-error-alerts"]
}


# Transformation - -> parquet

resource "aws_cloudwatch_log_metric_filter" "transformation-error-metric-filter" {
  name           = "transformation-error-metric-filter"
  log_group_name = "transformation-group"
  pattern        = "Transformation Error"
  metric_transformation {
    name      = "transformation-error-metric"
    namespace = "LogMetrics"
    value     = "1"
  }
  depends_on = [aws_cloudwatch_log_group.transformation-group]
}
resource "aws_cloudwatch_metric_alarm" "transformation-error-alarm" {
  alarm_name          = "transformation-error-alarm"
  metric_name         = "transformation-error-metric"
  namespace           = "LogMetrics"
  statistic           = "Sum"
  period              = "60"
  evaluation_periods  = "1"
  threshold           = "0"
  comparison_operator = "GreaterThanThreshold"
  alarm_actions       = ["arn:aws:sns:us-east-1:${var.subscription_arn}:test-error-alerts"]
}


# Total runtime - from inital s3 bucket to data warehouse

resource "aws_cloudwatch_log_metric_filter" "total-runtime-metric-filter" {
  name           = "total-runtime-metric-filter"
  log_group_name = "integration-group"
  pattern        = "Total Runtime Exceeded"
  metric_transformation {
    name      = "total-runtime-metric"
    namespace = "LogMetrics"
    value     = "900"
    }
  }
resource "aws_cloudwatch_metric_alarm" "total-runtime-alarm" {
  alarm_name          = "total-runtime-alarm"
  metric_name         = "total-runtime-metric"
  threshold           = "900"
  statistic           = "Maximum"
  comparison_operator = "GreaterThanThreshold"
  datapoints_to_alarm = "1"
  evaluation_periods  = "1"
  period              = "60"
  namespace           = "LogMetrics"
  alarm_description   = "Total runtime exceeded threshold of 15 minutes"
  alarm_actions       = ["arn:aws:sns:us-east-1:${var.subscription_arn}:test-error-alerts"]
}
