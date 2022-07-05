variable "buckets_name" {
  type = list(string)
  default = [
    "athena-logs-vini",
    "emr-logs-vini-serasa",
    "bootstrap-scripts-vini",
    "bucket-delivery-tweets",
    "bucket-landing-tweets",
    "emr-codes-vini"
  ]
}