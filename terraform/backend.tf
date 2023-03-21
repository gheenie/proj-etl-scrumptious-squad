terraform {
    backend "s3" {
        # Use the same name as ./tfstate_bucket/s3.tf secrets_bucket
        bucket = "scrumptious-squad-secrets-bucket-123"
        key = "terraform.tfstate"
        region = "us-east-1"
    }
}