resource "aws_lambda_function" "extraction_lambda" {
    function_name = "${var.extraction_lambda_name}"
    runtime = "python3.9"
    role = aws_iam_role.lambda_role.arn
    # Name of the .py file with handler in goes below
    handler = "${path.module}/../src/extract.py"
    # Links to a zip file, not a bucket & object
    filename = "${path.module}/../data/extraction.zip"
    source_code_hash = data.archive_file.extraction_zip.output_base64sha256
}

resource "aws_cloudwatch_log_group" "extraction-group" {
  name = "extraction-group"
}

resource "aws_cloudwatch_log_group" "transformation-group" {
  name = "transformation-group"
}

resource "aws_cloudwatch_log_group" "loading-group" {
  name = "loading-group"
}

resource "aws_cloudwatch_log_group" "integration-group" {
  name = "integration-group"
}

resource "aws_lambda_function" "extract_lambda" {
    function_name = var.extract_lambda_name
    runtime = "python3.9"
    role = aws_iam_role.lambda_role.arn
    # Name of the .py file with handler in goes below 
    handler = "extract.someting"
    # Links to a zip file, not a bucket & object
    filename = "${path.module}/../data/extract.zip"
    source_code_hash = data.archive_file.extraction_zip.output_base64sha256
}

resource "aws_lambda_function" "transform_lambda" {
    function_name = var.transform_lambda_name
    runtime = "python3.9"
    role = aws_iam_role.lambda_role.arn
    # Name of the .py file with handler in goes below 
    handler = "transform.something"
    # Links to a zip file, not a bucket & object
    filename = "${path.module}/../data/transform.zip"
    source_code_hash = data.archive_file.transform_zip.output_base64sha256
}

resource "aws_lambda_function" "load_lambda" {
    function_name = var.load_lambda_name
    runtime = "python3.9"
    role = aws_iam_role.lambda_role.arn
    # Name of the .py file with handler in goes below 
    handler = "load.something"
    # Links to a zip file, not a bucket & object
    filename = "${path.module}/../data/load.zip"
    source_code_hash = data.archive_file.load_zip.output_base64sha256

}