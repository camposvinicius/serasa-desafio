resource "aws_s3_bucket" "buckets" {
  for_each      = toset(var.buckets_name)
  bucket        = each.key
  force_destroy = true

  tags = {
    Name        = upper(each.key)
    Environment = "Serasa-Data-Engineer"
  }
}

resource "aws_s3_object" "emr_codes" {
  for_each = fileset("../emr_codes/", "*")

  bucket        = element(var.buckets_name, length(var.buckets_name) - 1)
  key           = each.key
  source        = "../emr_codes/${each.key}"
  force_destroy = true

  depends_on = [
    aws_s3_bucket.buckets
  ]
}

resource "aws_s3_object" "bootstrap_emr" {
  for_each = fileset("../emr_codes/bootstrap_code/", "*")

  bucket        = element(var.buckets_name, 2)
  key           = each.key
  source        = "../emr_codes/bootstrap_code/${each.key}"
  force_destroy = true

  depends_on = [
    aws_s3_bucket.buckets
  ]
}

