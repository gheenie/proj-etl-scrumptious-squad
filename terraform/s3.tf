resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "scrumptious-squad-code-bucket-"
}
# resource "aws_s3_object" "extract_code" {
#   bucket = aws_s3_bucket.code_bucket.bucket
#   key = "extraction.zip"
#   source = "${path.module}/../data/extraction.zip"
#   source_hash = data.archive_file.extraction_zip.output_md5
# }

# resource "aws_s3_object" "transform_code" {
#   bucket = aws_s3_bucket.code_bucket.bucket
#   key = "transform.zip"
#   source = "${path.module}/../data/transform.zip"
#   source_hash = data.archive_file.transform_zip.output_md5   
# }