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


resource "aws_cloudwatch_log_group" "extraction_group" {
    # A log group with this name will be auto-generated when the corresponding 
    # lambda is executed the first time, so we can just use that instead of 
    # making duplicate log groups. But we need to explicitly create one at the 
    # point of terraform plan to attach metric filters properly because 
    # the auto-generation doesn't happen immediately.
    name = "/aws/lambda/${var.extract_lambda_name}"
}


resource "aws_cloudwatch_log_group" "transformation_group" {
    # See aws_cloudwatch_log_group.extraction_group
    name = "/aws/lambda/${var.transform_lambda_name}"
}


resource "aws_cloudwatch_log_group" "loading_group" {
    # See aws_cloudwatch_log_group.extraction_group
    name = "/aws/lambda/${var.load_lambda_name}"
}


resource "aws_cloudwatch_log_group" "integration_group" {
    name = "integration-group"
}
