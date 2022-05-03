from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.bash import BashSensor

base_dir = '{{ dag_run.conf["base_dir"] if dag_run else "" }}'

with DAG(
    "thekirillisupov_dag",
    schedule_interval=None,
    catchup=False
) as dag:


    feature_eng_train_task = BashOperator(
	task_id="feuture_eng_train_task",
	bash_command="python {}preprocessing.py /datasets/amazon/all_reviews_5_core_train_extra_small_sentiment.json /datasets/amazon/all_reviews_5_core_test_extra_small_features.json".format(base_dir)
    )

    download_train_task = BashOperator(
	task_id="download_train_task",
	bash_command="hdfs dfs -copyFromRemote thekirillisupov_train_out {}thekirillisupov_train_out_local".format(base_dir)

    train_task = BashOperator(
	task_id = "train_task",
	bash_command="python {}train.py {}thekirillisupov_train_out {}6.joblib".format(base_dir, base_dir, base_dir)
)

    model_sensor = BashSensor(
	task_id = "model_sensor",
	bash_command="hdfs dfs test -e /user/thekirillisupov/6.joblib"
)

    predict_task = BashOperator(
	task_id = "predict_task",
	bash_command =  "python predict.py /user/thekirillisupov/6.joblib thekirillisupov_train_out /user/thekirillisupov/thekirillisupov_hw6_prediction"
)

    feature_eng_task >> train_download_task >> train_task >> model_sensor >> predict_task
