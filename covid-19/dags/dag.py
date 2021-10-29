import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

import pymysql.cursors
import requests


class Config:
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
    MYSQL_DB = os.getenv("MYSQL_DB")
    MYSQL_CHARSET = os.getenv("MYSQL_CHARSET")
    GET_LINK_URL = os.getenv("GET_LINK_URL")


def get_data_from_api(url):
    respone = requests.get(url)
    data = respone.json()
    with open("data.json", "w") as f:
        json.dump(data, f)
    return data


def save_data_to_db():
    connection = pymysql.connect(
        host=Config.MYSQL_HOST,
        port=Config.MYSQL_PORT,
        user=Config.MYSQL_USER,
        password=Config.MYSQL_PASSWORD,
        database=Config.MYSQL_DB,
        charset=Config.MYSQL_CHARSET,
        cursorclass=pymysql.cursors.DictCursor,
    )

    with open("data.json") as f:
        data = json.load(f)

    with connection.cursor() as inst:
        insert_data = """
            INSERT INTO `covid19_report_by_time` (
                `date`, `new_case`, `total_case`, `update_date`
                ) VALUES (%s, %s, %s, %s)
        """
        for i in range(len(data)):
            inst.execute(
                insert_data,
                (
                    data[i]["txn_date"],
                    data[i]["new_case"],
                    data[i]["total_case"],
                    data[i]["update_date"],
                ),
            )

    connection.commit()


default_arguments = {
    "owner": "pichai_pipeline",
    "start_date": days_ago(2),
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "Covid19_Report_By_Time",
    schedule_interval="@daily",
    default_args=default_arguments,
    description="Test pipeline for COVID-19.",
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="get_data_from_api",
        python_callable=get_data_from_api,
        op_kwargs={
            "url": "https://covid19.ddc.moph.go.th/api/Cases/today-cases-by-provinces"
        },
        dag=dag,
    )

    t2 = PythonOperator(
        task_id="save_data_to_db", 
        python_callable=save_data_to_db, 
        dag=dag
    )

    t1 >> t2