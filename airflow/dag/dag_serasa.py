################################################################# BIBLIOTECAS #################################################################################

from datetime import timedelta, datetime

from airflow import DAG

from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator

from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator

################################################################# VARIÁVEIS #################################################################################

SCRIPT_GET_TWEETS = 's3://emr-codes-vini/collect_tweets.py'
SCRIPT_CURATED = 's3://emr-codes-vini/delivery.py'

HASHTAG_LIST = [
    'covid19',
    'pneumonia',
    'gripe',
    'sinusite',
    'asma'
]

DATE='{{ ds }}'

EMR_CONFIG = {
    'Name': f'AWS-SERASA-DATA-ENGINEER-{DATE}',
    "ReleaseLabel": "emr-6.7.0",
    "LogUri": "s3://emr-logs-vini-serasa/",
    "Applications": [
        {
            "Name": "Hadoop"
        },
        {
            "Name": "Hive",
        },
        {
            "Name": "Ganglia",
        },
        {
            "Name": "Spark",
        }
    ],
    'VisibleToAllUsers': False,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'AutoScalingRole': 'EMR_AutoScaling_DefaultRole',
    'StepConcurrencyLevel': 5, 
    'BootstrapActions': [
        {
            'Name': 'Pip3 Install Extra Libraries',
            'ScriptBootstrapAction': {
                'Path': 's3://bootstrap-scripts-vini/pip_install_libraries.sh'
            }
        }          
    ],    
    'Tags': [
        {
            "Key": "Serasa",
            "Value": "DataEngineer"
        },
        {
            "Key": "Vini",
            "Value": "Desafio"
        }
    ],  
    "Configurations": [
        {
            "Classification": "hive-site",
            "Properties": {
                "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            }
        },
        {
            "Classification": "spark-hive-site",
            "Properties": {
                "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            }
        },
        {
            "Classification": "spark",
            "Properties": {
                "maximizeResourceAllocation": "true"
            }
        }      
    ],    
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'MASTER_NODES',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                "Name": "CORE_NODES",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "TASK_NODES",
                "Market": "SPOT",
                "BidPrice": "0.088",
                "InstanceRole": "TASK",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
                "AutoScalingPolicy":
                    {
                        "Constraints":
                    {
                        "MinCapacity": 1,
                        "MaxCapacity": 2
                    },
                    "Rules":
                        [
                    {
                    "Name": "Scale Up",
                    "Action":{
                        "SimpleScalingPolicyConfiguration":{
                        "AdjustmentType": "CHANGE_IN_CAPACITY",
                        "ScalingAdjustment": 1,
                        "CoolDown": 120
                        }
                    },
                    "Trigger":{
                        "CloudWatchAlarmDefinition":{
                        "ComparisonOperator": "GREATER_THAN_OR_EQUAL",
                        "EvaluationPeriods": 1,
                        "MetricName": "Scale Up",
                        "Period": 60,
                        "Threshold": 15,
                        "Statistic": "AVERAGE",
                        "Threshold": 75
                        }
                    }
                    }
                    ]
                }
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
        'Ec2SubnetId': 'SUBNET',
        'Ec2KeyName': 'ViniSerasaKeyPair'
    }
}

########################################################## DAG ###############################################################################################

default_args = {
    'owner': 'AWS-SERASA-DATAENGINEER',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=30)
}

with DAG(
    dag_id="DATA-ENGINEER-SERASA-VINI",
    tags=['serasa', 'aws', 'dataengineer'],
    default_args=default_args,
    start_date=datetime.now() - timedelta(days=365),
    schedule_interval='@monthly',
    concurrency=5,
    max_active_runs=12,
    max_active_tasks=5,
    catchup=True
) as dag:

################################################################# CRIA CLUSTER E OBSERVA A CRIAÇÃO #################################################################################

    def create_emr_cluster():
        create_emr_cluster = EmrCreateJobFlowOperator(
            task_id="create_emr_cluster",
            job_flow_overrides=EMR_CONFIG,
            aws_conn_id="aws",
            emr_conn_id="emr",
            region_name='us-east-1'
        )

        emr_create_sensor = EmrJobFlowSensor(
            task_id='monitoring_emr_cluster_creation',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
            target_states=['WAITING'],
            failed_states=['TERMINATED', 'TERMINATED_WITH_ERRORS'],
            aws_conn_id="aws"
        )

        return (
            create_emr_cluster >> emr_create_sensor
        )

################################################################ RODA SCRIPT E OBSERVA STATUS #################################################################################

    def run_spark_submit(step_name, path_script, hashtag=None):
        steps = [{   
            "Name": f"{step_name}",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    'spark-submit', 
                    '--deploy-mode', 'cluster',
                    '--conf', 'spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2',
                    '--conf', 'spark.speculation=false',
                    '--conf', 'spark.sql.adaptive.enabled=true',
                    '--conf', 'spark.sql.adaptive.coalescePartitions.enabled=true',
                    '--conf', 'spark.sql.adaptive.coalescePartitions.minPartitionNum=1',
                    '--conf', 'spark.sql.adaptive.coalescePartitions.initialPartitionNum=10',
                    '--conf', 'spark.sql.adaptive.advisoryPartitionSizeInBytes=134217728',
                    '--conf', 'spark.serializer=org.apache.spark.serializer.KryoSerializer',
                    '--conf', 'spark.dynamicAllocation.minExecutors=5',
                    '--conf', 'spark.dynamicAllocation.maxExecutors=30',
                    '--conf', 'spark.dynamicAllocation.initialExecutors=10', 
                    f'{path_script}',
                    f'{hashtag}',
                    DATE             
                ]
            }
        }] 

        task = EmrAddStepsOperator(
            task_id=f'task_{step_name}',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
            steps=steps,
            aws_conn_id='aws',
            dag=dag
        )

        step_check_task = EmrStepSensor(
            task_id=f'watch_task_{step_name}',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
            step_id=f"{{{{ task_instance.xcom_pull(task_ids='task_{step_name}', key='return_value')[0] }}}}",
            target_states=['COMPLETED'],
            failed_states=['CANCELLED', 'FAILED', 'INTERRUPTED'],
            aws_conn_id="aws",
            dag=dag
        )
        
        return (
            task, step_check_task
        )

################################################################# DESTRÓI CLUSTER #################################################################################

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id='terminate_emr_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        trigger_rule="all_done",
        aws_conn_id='aws'
    )

################################################################# TRIGGA CRAWLER #################################################################################

    glue_crawler = GlueCrawlerOperator(
        task_id='Crawler_Serasa_Tweets',
        config={"Name": "Crawler_Serasa_Tweets"},
        aws_conn_id='aws',
        wait_for_completion=True,
        poll_interval=10
    )

############################################################### DEFINIÇÃO DE SEQUÊNCIA DAS TASKS DA DAG ################################################################

    task_create_emr = create_emr_cluster()
    task_delivery_data, task_sensor_delivery_data = run_spark_submit('delivery_data', SCRIPT_CURATED)

    for hashtag in HASHTAG_LIST:
        task_get_tweets, task_sensor_get_tweets = run_spark_submit(f'get_tweets_{hashtag}', SCRIPT_GET_TWEETS, hashtag)

        task_create_emr >> task_get_tweets >> task_sensor_get_tweets >> task_delivery_data
    

    task_delivery_data >> task_sensor_delivery_data >> [terminate_emr_cluster, glue_crawler]