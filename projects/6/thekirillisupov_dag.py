from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.bash import BashSensor
from datetime import datetime
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.contrib.sensors.file_sensor import FileSensor

base_dir = '{{ dag_run.conf["base_dir"] if dag_run else "" }}'

with DAG(
    "thekirillisupov_dag",
    schedule_interval=None,
    catchup=False,
    start_date=datetime(2022, 5, 3)
) as dag:

    feature_eng_task = SparkSubmitOperator(
	task_id="feuture_eng_task",
	application="{}/preprocessing.py".format(base_dir),
	env_vars={"PYSPARK_PYTHON": "opt/conda/envs/dsenv"}
)

    download_train_task = BashOperator(
	task_id="download_train_task",
	bash_command="hdfs dfs -get thekirillisupov_train_out/*.json {}thekirillisupov_train_out_local".format(base_dir)
)
    train_task = BashOperator(
	task_id = "train_task",
	bash_command="python3 {}train.py {}thekirillisupov_train_out_local {}6.joblib".format(base_dir, base_dir, base_dir)
)

    model_sensor = FileSensor(
	task_id = "model_sensor",
	poke_interval=5,
	filepath="{}6.joblib".format(base_dir)
)

    predict_task = SparkSubmitOperator(
	task_id = "predict_task",
	application =  "{}predict.py".format(base_dir),
	env_vars={"PYSPARK_PYTHON": "opt/conda/envs/dsenv"}
)

    feature_eng_task >> download_train_task >> train_task >> model_sensor >> predict_task
