resource "aws_lambda_function" "extraction_lambda" {
    function_name = "extraction_lambda"
    runtime = "python3.9"
    role = aws_iam_role.lambda_role.arn
    # Name of the .py file with handler in goes below 
    handler = "${path.module}/../src/extraction.py"
    # Links to a zip file, not a bucket & object
    filename = "${path.module}/../data/extraction.zip"
    source_code_hash = data.archive_file.lambda.output-base64sha256
}