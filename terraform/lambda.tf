resource "aws_lambda_function" "extraction_lambda" {
    function_name = "extraction_lambda"
    runtime = "python3.9"
    # Name of role for this function goes below
    role = aws_iam_role.___.arn
    # Name of the .py file with hndler in goes below
    handler = "example.py"
    # Links to a zip file

    # source_code_hash = data.archive_file.lambda.output-
    
    # Link to wherever the deployement package for this lambda is kept (the code-bucket)
    s3_bucket = .
    s3_key = .
}