variable "extraction_lambda_name" {
    type = string
    default = "extraction-lambda"
}


variable "transform_lambda_name" {
    type = string
    default = "transform-lambda"
}


variable "load_lambda_name" {
    type = string
    default = "load-lambda"
}


variable "ingested_data_bucket_prefix" {
    type = string
    default = "scrumptious-squad-in-data-"
}

variable "code_bucket_prefix" {
    type = string
    default = "scrumptious-squad-co-"
}

variable "processed_data_bucket_prefix" {
    type = string
    default = "scrumptious-squad-pr-data-"
}