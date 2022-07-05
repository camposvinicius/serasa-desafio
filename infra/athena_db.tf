resource "aws_athena_database" "athena_db" {
  name          = "db_serasa_tweets"
  bucket        = element(var.buckets_name, 0)
  force_destroy = true

  depends_on = [
    aws_s3_bucket.buckets
  ]

}