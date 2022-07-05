terraform {
  backend "s3" {
    bucket = "bucket-backend-tweets"
    key    = "resouces/terraform.tfstate"
    region = "us-east-1"
  }
}