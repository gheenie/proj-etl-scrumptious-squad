variable "sns_group_email" {
    type    = string
    default = "scrumptious23@yahoo.com"
}


variable "extract_archive_source_path" {
    type    = string
    default = "./../data/src_extract"
}


variable "extract_archive_output_path" {
    type    = string
    default = "./../data/extract.zip"
}


variable "transform_archive_source_path" {
    type    = string
    default = "./../data/src_transform"
}


variable "transform_archive_output_path" {
    type    = string
    default = "./../data/transform.zip"
}


variable "load_archive_source_path" {
    type    = string
    default = "./../data/src_load"
}


variable "load_archive_output_path" {
    type    = string
    default = "./../data/load.zip"
}


variable "extract_lambda_name" {
    type    = string
    default = "extract-lambda"
}


variable "transform_lambda_name" {
    type    = string
    default = "transform-lambda"
}


variable "load_lambda_name" {
    type    = string
    default = "load-lambda"
}


variable "ingested_data_bucket_prefix" {
    type    = string
    default = "scrumptious-squad-in-data-"
}


variable "processed_data_bucket_prefix" {
    type    = string
    default = "scrumptious-squad-pr-data-"
}
