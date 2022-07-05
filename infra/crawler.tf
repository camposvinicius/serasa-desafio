resource "aws_glue_crawler" "crawler_tweets" {
  database_name = aws_athena_database.athena_db.name
  name          = "Crawler_Serasa_Tweets"
  role          = "GlueServiceRoleSerasaTweets"

  configuration = jsonencode(
    {
      Grouping = {
        TableGroupingPolicy = "CombineCompatibleSchemas"
      }
      CrawlerOutput = {
        Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
      }
      Version = 1
    }
  )

  s3_target {
    path = "s3://bucket-delivery-tweets/delivery/"
  }

  depends_on = [
    aws_athena_database.athena_db
  ]
}