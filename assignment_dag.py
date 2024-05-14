from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "Selase",
    "start_date": days_ago(0),
    "email": "email@email.com",
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=0)
}

dag = DAG(
    dag_id= "ETL_toll_data",
    default_args= default_args,
    description= "Apache Airflow Final Assignment",
    schedule_interval= timedelta(days=1)
)

# tasks

# task 1 unzip data
unzip_data = BashOperator(
    task_id="unzip_data",
    bash_command="tar -xzvf /home/project/airflow/dags/finalassignment/tolldata.tgz",
    dag=dag
)

# task 2 extract data from csv
extract_data_from_csv = BashOperator(
    task_id = "extract_data_from_csv",
    bash_command = "cut -d',' -f1-4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv",
    dag=dag
)

# task 3 extract data from tsv
extract_data_from_tsv = BashOperator(
    task_id="extract_data_from_tsv",
    bash_command="cut -d',' -f5-7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/tsv_data.csv",
    dag=dag
)

# task 4 extract data from fixed width
extract_data_from_fixed_width = BashOperator(
    task_id="extract_data_from_fixed_width",
    bash_command="cut -d',' -f6,7 /home/project/airflow/dags/finalassignment/payment-data.txt > /home/project/airflow/dags/finalassignment/fixed_width_data.csv",
    dag=dag
)

# task 5 consolidate data
consolidate_data = BashOperator(
    task_id="consolidate_data",
    bash_command="paste -d',' /home/project/airflow/dags/finalassignment/csv_data.csv /home/project/airflow/dags/finalassignment/tsv_data.csv /home/project/airflow/dags/finalassignment/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/extracted_data.csv",
    dag=dag
)

# task 6 transform vehicle type field
transform_data = BashOperator(
    task_id = "transform_data",
    bash_command="tr '[a-z] '[A-Z]' < /home/project/airflow/dags/finalassignment/extracted-data.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv",
    dag=dag
)

# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data