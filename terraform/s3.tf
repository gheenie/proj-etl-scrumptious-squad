resource "aws_s3_bucket" "ingested_data_bucket" {
 bucket_prefix = var.code_bucket_prefix
}

resource "aws_s3_bucket" "code_bucket" {
bucket_prefix = var.code_bucket_prefix
}

resource "aws_s3_bucket" "processed_data_bucket" {
bucket_prefix = var.code_bucket_prefix
}