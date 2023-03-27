resource "local_file" "ingested_data_bucket_name" {
  content  = aws_s3_bucket.ingested_data_bucket.bucket
  filename = "${path.module}/../config/.ingested_data_bucket_name"
}


resource "local_file" "processed_data_bucket_name" {
  content  = aws_s3_bucket.processed_data_bucket.bucket
  filename = "${path.module}/../config/.processed_data_bucket_name"
}
