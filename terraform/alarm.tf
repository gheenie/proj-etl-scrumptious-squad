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

resource "aws_cloudwatch_metric_alarm" "runtime_error_alarm" {
  metric_name         = aws_cloudwatch_log_metric_filter.runtime_error.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.runtime_error.metric_transformation[0].namespace

  alarm_name          = "runtime-error-alarm"
  evaluation_periods  = "1"
  period              = "60"
  statistic           = "Sum"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = "1"
  alarm_actions       = [aws_sns_topic.error_notification.arn]
  alarm_description   = "Oh no! We've passed our max runtime!"
}

resource "aws_cloudwatch_metric_alarm" "nearing_max_runtime_alarm" {
  dimensions = {
        "FunctionName" = aws_lambda_function.extract_lambda.function_name
        # Will inserting multiple functions here work?
    }
  
  metric_name         = "Duration"
  namespace           = "AWS/Lambda"

  alarm_name          = "nearing-max-runtime-alarm"
  evaluation_periods  = "1"
  period              = "60"
  statistic           = "Maximum"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = "600"
  alarm_actions       = [aws_sns_topic.error_notification.arn]
  alarm_description   = "Uh-oh! We're nearing a runtime error!"
}


# Monitor for Data Integrity Violation appearing in log text during extraction.

resource "aws_cloudwatch_log_metric_filter" "data_integrity_metric_filter" {
  log_group_name = aws_cloudwatch_log_group.extraction_group.name

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
  alarm_actions       = [aws_sns_topic.error_notification.arn]
}


# Monitor for Data Validation Failed appearing in log text during extraction.

resource "aws_cloudwatch_log_metric_filter" "data_validation_metric_filter" {
  log_group_name = aws_cloudwatch_log_group.extraction_group.name

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
  alarm_actions       = [aws_sns_topic.error_notification.arn]
}


# Monitor for Transformation Error appearing in log text during transformation.

resource "aws_cloudwatch_log_metric_filter" "transformation_error_metric_filter" {
  log_group_name = aws_cloudwatch_log_group.transformation_group.name

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
  alarm_actions       = [aws_sns_topic.error_notification.arn]
}


resource "aws_cloudwatch_log_metric_filter" "any_error_in_loading_phase" {
  log_group_name = aws_cloudwatch_log_group.loading_group.name

  name           = "any-error-filter"
  pattern        = "Error"
  
  metric_transformation {
    name      = "Error_Count"
    namespace = "scrumptious-space"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "any_error_in_loading_phase_alarm" {
  metric_name         = aws_cloudwatch_log_metric_filter.any_error_in_loading_phase.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.any_error_in_loading_phase.metric_transformation[0].namespace

  alarm_name          = "error-in-loading-phase-alarm"
  evaluation_periods  = "1"
  period              = "60"
  statistic           = "Sum"
  comparison_operator = "GreaterThanThreshold"
  threshold           = "0"
  alarm_actions       = [aws_sns_topic.error_notification.arn]
}
