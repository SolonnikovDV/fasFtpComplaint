from airflow.example_dags.example_bash_operator import dag
from airflow.models.baseoperator import chain
import pendulum
from airflow.operators.empty import EmptyOperator

'''
optimize period for schedule is @monthly
in a project for thr demonstration were used schedule=@daily 
'''


# create download dag
@dag('download_data_dag', start_date=pendulum.datetime(2022, 10, 28, tz='UTC'), schedule='@daily', catchup=False)
def download_dag():
    return EmptyOperator(task_id='ftp_get_data')


# create data importing dag
@dag('import_data_to_db', start_date=pendulum.datetime(2021, 10, 28, tz='UTC'), schedule='@daily', catchup=False)
def import_data_dag():
    return EmptyOperator(task_id='import_data_mongodb')


# init dags
download_dag = download_dag()
import_data_dag = import_data_dag()

# run dags chain
chain(download_dag, import_data_dag)
