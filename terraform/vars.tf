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


variable "sns_group_email" {
    type    = string
    default = "scrumptious23@yahoo.com"
}


variable "subscription_arn" {
    type = number
    default = 962685537040
}
