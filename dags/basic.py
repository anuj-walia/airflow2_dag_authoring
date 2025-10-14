from airflow.decorators import dag, task
from datetime import datetime
import logging

logging.basicConfig(level=logging.DEBUG)
@dag(schedule=None,start_date=datetime(2025,10,1),catchup=False)
def dag_basic_with_decorators():


    @task()
    def start():
        print("Starting DAG")

    @task()
    def work():
        print("Executing work")
        logging.debug("**************debug work*****************")

    @task()
    def stop():
        print("Finishing DAG")


    start()>>work()>>stop()

dag_basic_with_decorators()
