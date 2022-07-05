########################################## BIBLIOTECAS ###########################################################

import sys
import logging
import tweepy as tw
import boto3
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import (
    StructField, 
    StructType, 
    StringType, 
    IntegerType,
    LongType,
    DateType
)

########################################## VARIÁVEIS ###########################################################

PATH_TO_SAVE_TWEETS_S3 = 's3://bucket-landing-tweets/tweets/'
REGION = 'us-east-1'
SECRET_NAME = 'ViniTwitterKeys'

########################################## APLICAÇÃO ###########################################################

class SerasaTweetsDesafio:

    def __init__(self, spark, path, keyword, last_year) -> None:

        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=REGION
        )

        secrets = client.get_secret_value(
            SecretId=SECRET_NAME
        )

        if 'SecretString' in secrets:
            return_secrets = json.loads(secrets['SecretString'])
            (
                CONSUMER_KEY,
                CONSUMER_SECRET,
                ACCESS_TOKEN,
                ACCESS_TOKEN_SECRET
            ) = (
                return_secrets['CONSUMER_KEY'],
                return_secrets['CONSUMER_SECRET'],
                return_secrets['ACCESS_TOKEN'],
                return_secrets['ACCESS_TOKEN_SECRET']  
            )
                
        self.spark = spark
        self.path = path               
        self.keyword = keyword
        self.last_year = last_year
        self.consumer_key = CONSUMER_KEY
        self.consumer_secret = CONSUMER_SECRET
        self.access_token = ACCESS_TOKEN
        self.access_token_secret = ACCESS_TOKEN_SECRET

    def run(self) -> None:
        self.create_logger()
        self.get_tweets_write_parquet()

    def create_logger(self):
        logging.basicConfig(format='%(name)s - %(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', stream=sys.stdout)
        self.logger = logging.getLogger('DATAENGINEER_VINI_SERASA')
        self.logger.setLevel(logging.INFO)                

    def get_tweets_write_parquet(self):

        self.logger.info("\n    Iniciando a aplicação... ")
        auth = tw.OAuthHandler(self.consumer_key, self.consumer_secret)
        auth.set_access_token(self.access_token, self.access_token_secret)
        api = tw.API(auth, wait_on_rate_limit=True)

        cols = [
            'ID_USUARIO',
            'TWEET',
            'DATA_CRIACAO',
            'USUARIO',
            'DESCRICAO_DO_USUARIO',
            'LOCALIZACAO_DO_USUARIO',
            'SEGUINDO',
            'SEGUIDORES',
            'NUMERO_TOTAL_DE_TWEETS'
        ]

        schema = StructType([
            StructField('ID_USUARIO', LongType(), True),
            StructField('TWEET', StringType(), True),
            StructField('DATA_CRIACAO', DateType(), True),
            StructField('USUARIO', StringType(), True),
            StructField('DESCRICAO_DO_USUARIO', StringType(), True),
            StructField('LOCALIZACAO_DO_USUARIO', StringType(), True),
            StructField('SEGUINDO', IntegerType(), True),
            StructField('SEGUIDORES', IntegerType(), True),
            StructField('NUMERO_TOTAL_DE_TWEETS', IntegerType(), True)
        ])
            
        data = []

        self.logger.info(f"\n   Iniciando a coleta de tweets para a palavra chave {self.keyword}... ")

        for tweet in tw.Cursor(api.search_tweets, q=f'#' + str(self.keyword), lang="pt", tweet_mode='extended', since_id=self.last_year).items(10000):
            data.append([
                tweet.user.id,
                tweet.full_text,
                tweet.created_at,
                tweet.user.screen_name,
                tweet.user.description,
                tweet.user.location,
                tweet.user.friends_count,
                tweet.user.followers_count,
                tweet.user.statuses_count
            ])

        df = (
            self.spark.createDataFrame(self.spark.sparkContext.parallelize(data), schema)
            .withColumn('HASHTAG_PESQUISADA', lit(f'{self.keyword}'.upper()))
        )

        del(data)
        
        (
            df.repartition(5)
            .write.format("parquet")
            .mode("append")
            .save(self.path)
        )

        self.logger.info(f"\n   Dataframe criado e dados salvos. Aplicação sendo encerrada ...")

if __name__ == '__main__':
        
    spark = (
        SparkSession.builder.appName('DATA_ENGINEER_VINI_SERASA')
        .enableHiveSupport()
        .config('spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version', '2')
        .config('spark.speculation', 'false')
        .config('spark.sql.adaptive.enabled', 'true')
        .config('spark.shuffle.service.enabled', 'true')
        .config('spark.dynamicAllocation.enabled', 'true')
        .config('spark.sql.adaptive.coalescePartitions.enabled', 'true')
        .config('spark.sql.adaptive.coalescePartitions.minPartitionSize', '1')
        .config('spark.sql.adaptive.coalescePartitions.minPartitionSize', '10')
        .config('spark.sql.adaptive.advisoryPartitionSizeInBytes', '134217728')
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
        .config('spark.dynamicAllocation.minExecutors', "5")
        .config('spark.dynamicAllocation.maxExecutors', "30")
        .config('spark.dynamicAllocation.initialExecutors', "10")
        .config('spark.sql.debug.maxToStringFields', '300')
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    
    run = SerasaTweetsDesafio(
        spark, 
        PATH_TO_SAVE_TWEETS_S3,        
        sys.argv[1], 
        sys.argv[2]
    )
    
    run.run()
 
