import datetime
import sys
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.contrib.sensors.file_sensor import FileSensor


base_dir = '{{ dag_run.conf["base_dir"] if dag_run else "" }}'
env_vars = {'PYSPARK_PYTHON' : 'dsenv'}

with DAG(
    dag_id='thekirillisupov_dag',
    schedule_interval=None,
    catchup=False,
    start_date = datetime.datetime(22, 5, 1)
) as dag:
    feature_eng_task =  BashOperator(task_id='feature_eng_task', bash_command=f'PYSPARK_PYTHON=/opt/conda/envs/dsenv/bin/python /usr/bin/spark-submit --master yarn --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/opt/conda/envs/dsenv/bin/python --name arrow-spark --queue default {base_dir}/preprocessing.py')
    
    download_train_task = BashOperator(ask_id='train_download_task', bash_command=f'hdfs dfs -get thekirillisupov_train_out/*.json {base_dir}/thekirillisupov_train_out_local')
    
    train_model = BashOperator(task_id='train_task', bash_command=f'/opt/conda/envs/dsenv/bin/python {base_dir}/train.py {base_dir}/thekirillisupov_train_out_local {base_dir}/6.joblib')
    
    model_sensor = FileSensor(task_id = 'model_sensor', poke_interval=5,  filepath = f'{base_dir}/6.joblib')
    
    predict_task = BashOperator(task_id='predict_task', bash_command=f'PYSPARK_PYTHON=/opt/conda/envs/dsenv/bin/python /usr/bin/spark-submit --master yarn --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/opt/conda/envs/dsenv/bin/python --name arrow-spark --queue default {base_dir}/predict.py')

    feature_eng_task >> download_train_task >> train_model >> model_sensor >> predict_task
