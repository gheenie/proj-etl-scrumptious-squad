resource "aws_s3_bucket" "ingested_data_bucket" {
    # bucket_prefix = var.ingested_data_bucket_prefix
    bucket = "scrumptious-squad-in-data-20230323093358275600000004"
}

resource "aws_s3_bucket" "processed_data_bucket" {
    # bucket_prefix = var.processed_data_bucket_prefix
    bucket = "scrumptious-squad-pr-data-20230323093358336100000005"
}
