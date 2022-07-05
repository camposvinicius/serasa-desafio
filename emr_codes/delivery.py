########################################## BIBLIOTECAS ###########################################################

import sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    year,
    month,
    dayofmonth
)

########################################## VARIÁVEIS ###########################################################

PATH_TWEETS_S3 = 's3://bucket-landing-tweets/tweets/'
PATH_TO_DELIVERY = 's3://bucket-delivery-tweets/delivery/'

########################################## APLICAÇÃO ###########################################################


class DeliveryTweetsSerasa:
    
    def __init__(self, spark, path_landing, path_delivery) -> None:
        self.spark = spark
        self.path_landing = path_landing
        self.path_delivery = path_delivery

    def run(self) -> None:
        self.create_logger()
        self.delivery_data()

    def create_logger(self):
        logging.basicConfig(format='%(name)s - %(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', stream=sys.stdout)
        self.logger = logging.getLogger('DATAENGINEER_VINI_SERASA')
        self.logger.setLevel(logging.INFO)

    def delivery_data(self):

        self.logger.info(f"\n    Iniciando a aplicação e leitura do path {self.path_landing}... ")

        df = (
            spark.read.format("parquet")
            .load(self.path_landing)
            .withColumn('ANO_DO_TWEET', year('DATA_CRIACAO'))
            .withColumn('MES_DO_TWEET', month('DATA_CRIACAO'))
            .withColumn('DIA_DO_TWEET', dayofmonth('DATA_CRIACAO'))
        )

        self.logger.info(f"\n    Escrevendo os dados particionados por HASHTAG... ")
        
        (
            df.repartition(5)
            .write
            .format("parquet")
            .partitionBy('HASHTAG_PESQUISADA')
            .mode("append")
            .save(self.path_delivery)
        )

        self.logger.info(f"\n   Aplicação concluída, encerrando ...")

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

    run = DeliveryTweetsSerasa(
        spark,
        PATH_TWEETS_S3,
        PATH_TO_DELIVERY
    )

    run.run()