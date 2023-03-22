# Monitor execution time for all functions.

resource "aws_cloudwatch_log_metric_filter" "runtime_error" {
  log_group_name = aws_cloudwatch_log_group.integration_group.name
  depends_on     = [aws_cloudwatch_log_group.integration_group]

  name           = "runtime-error-filter"
  pattern        = "RuntimeError"
  
  metric_transformation {
    name      = "Exceeded_Allowed_Runtime_Count"
    namespace = "scrumptious-space"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "alert_runtime_errors" {
  metric_name         = aws_cloudwatch_log_metric_filter.runtime_error.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.runtime_error.metric_transformation[0].namespace

  alarm_name          = "alert_runtime_errors"
  evaluation_periods  = "1"
  period              = "60"
  statistic           = "Sum"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = "1"
  alarm_actions       = [aws_sns_topic.error_alerts.arn]
  alarm_description   = "Oh no! We've passed our max runtime!"
}

resource "aws_cloudwatch_metric_alarm" "alert_nearing_max_runtime" {
  dimensions = {
        "FunctionName" = aws_lambda_function.extract_lambda.function_name
        # Will inserting multiple functions here work?
    }
  
  metric_name         = "Duration"
  namespace           = "AWS/Lambda"

  alarm_name          = "alert_nearing_max_runtime"
  evaluation_periods  = "1"
  period              = "60"
  statistic           = "Maximum"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = "600"
  alarm_actions       = [aws_sns_topic.error_alerts.arn]
  alarm_description   = "Uh-oh! We're nearing a runtime error!"
}


# Monitor for Data Integrity Violation appearing in log text during extraction.

resource "aws_cloudwatch_log_metric_filter" "data_integrity_metric_filter" {
  log_group_name = aws_cloudwatch_log_group.extraction_group.name
  depends_on     = [aws_cloudwatch_log_group.extraction_group]

  name           = "data-integrity-metric-filter"
  pattern        = "Data Integrity Violation"

  metric_transformation {
    name      = "Data_Integrity_Violated_Count"
    namespace = "LogMetrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "data_integrity_alarm" {
  metric_name         = aws_cloudwatch_log_metric_filter.data_integrity_metric_filter.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.data_integrity_metric_filter.metric_transformation[0].namespace

  alarm_name          = "data-integrity-alarm"
  evaluation_periods  = "1"
  period              = "60"
  datapoints_to_alarm = "1"
  statistic           = "Sum"
  comparison_operator = "GreaterThanThreshold"
  threshold           = "0"
  alarm_description   = "Data integrity violation detected"
  alarm_actions       = [aws_sns_topic.error_alerts.arn]
}


# Monitor for Data Validation Failed appearing in log text during extraction.

resource "aws_cloudwatch_log_metric_filter" "data_validation_metric_filter" {
  log_group_name = aws_cloudwatch_log_group.extraction_group.name
  depends_on     = [aws_cloudwatch_log_group.extraction_group]

  name           = "data-validation-metric-filter"
  pattern        = "Data Validation Failed"

  metric_transformation {
    name      = "Data_Validation_Failed_Count"
    namespace = "LogMetrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "data_validation_alarm" {
  metric_name         = aws_cloudwatch_log_metric_filter.data_validation_metric_filter.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.data_validation_metric_filter.metric_transformation[0].namespace

  alarm_name          = "data-validation-alarm"
  evaluation_periods  = "1"
  period              = "60"
  datapoints_to_alarm = "1"
  statistic           = "Sum"
  comparison_operator = "GreaterThanThreshold"
  threshold           = "0"
  alarm_description   = "Data validation failed"
  alarm_actions       = [aws_sns_topic.error_alerts.arn]
}


# Monitor for Transformation Error appearing in log text during transformation.

resource "aws_cloudwatch_log_metric_filter" "transformation_error_metric_filter" {
  log_group_name = aws_cloudwatch_log_group.transformation_group.name
  depends_on     = [aws_cloudwatch_log_group.transformation_group]

  name           = "transformation-error-metric-filter"
  pattern        = "Transformation Error"

  metric_transformation {
    name      = "Transformation_Error_Count"
    namespace = "LogMetrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "transformation_error_alarm" {
  metric_name         = aws_cloudwatch_log_metric_filter.transformation_error_metric_filter.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.transformation_error_metric_filter.metric_transformation[0].namespace

  alarm_name          = "transformation-error-alarm"
  evaluation_periods  = "1"
  period              = "60"
  statistic           = "Sum"
  comparison_operator = "GreaterThanThreshold"
  threshold           = "0"
  alarm_actions       = [aws_sns_topic.error_alerts.arn]
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
  alarm_actions       = [aws_sns_topic.error_alerts.arn]
}
