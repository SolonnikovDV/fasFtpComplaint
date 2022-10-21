from datetime import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator

# local import
from ftp_API.ftp_get_data import get_ftp_data_list, diff_list, download_data
# from ftp_API.import_data_mongodb import data_import, collection_filter

with DAG(
    dag_id='download_ftp_fas',
    schedule_interval='@monthly',
    start_date=datetime(2022, 10, 18),
    catchup=False,
) as dag:

    task_get_ftp_data_list = PythonOperator(
        task_id='get_ftp_data_list',
        python_callable=get_ftp_data_list,
    )

    task_diff_list = PythonOperator(
        task_id='diff_list',
        python_callable=diff_list,
    )

    task_download_data = PythonOperator(
        task_id='download_data',
        python_callable=download_data,
    )

    # task_data_import = PythonOperator(
    #     task_id='data_import',
    #     python_callable=data_import,
    # )
    #
    # task_collection_filter = PythonOperator(
    #     task_id='collection_filter',
    #     python_callable=collection_filter,
    # )

# task lunch order
task_get_ftp_data_list >> task_diff_list >> task_download_data\
# >> task_data_import >> task_collection_filter