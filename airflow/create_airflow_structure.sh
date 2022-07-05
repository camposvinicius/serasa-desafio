#!/bin/bash

mkdir airflow &&
cd airflow &&
mkdir ./dags ./plugins ./logs &&
echo AIRFLOW_UID=50000 >> .env &&
touch docker-compose.yaml