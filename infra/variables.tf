variable "buckets_name" {
  type = list(string)
  default = [
    "athena-logs-vini",
    "emr-logs-vini",
    "bootstrap-scripts-vini",
    "bucket-delivery-tweets",
    "bucket-landing-tweets",
    "emr-codes-vini"
  ]
}