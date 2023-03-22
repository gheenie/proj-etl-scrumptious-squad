resource "aws_lambda_function" "extraction_lambda" {
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
    # function_name = var.extraction_lambda_name
    # runtime = "python3.9"
    # role = aws_iam_role.lambda_role.arn
    # # Name of the .py file with handler in goes below 
    # handler = "extraction.someting"
    # # Links to a zip file, not a bucket & object
    # filename = "${path.module}/../data/extraction.zip"
    # source_code_hash = data.archive_file.extraction_zip.output_base64sha256
}

resource "aws_lambda_function" "load_lambda" {
    # function_name = var.extraction_lambda_name
    # runtime = "python3.9"
    # role = aws_iam_role.lambda_role.arn
    # # Name of the .py file with handler in goes below 
    # handler = "extraction.someting"
    # # Links to a zip file, not a bucket & object
    # filename = "${path.module}/../data/extraction.zip"
    # source_code_hash = data.archive_file.extraction_zip.output_base64sha256
}