import logging
import pendulum
from datetime import date, timedelta, datetime

from airflow.decorators import dag, task
from airflow.models import Variable

from lib.pg import ConnectionBuilderPg
from lib.vertica import ConnectionBuilderVertica
from lib.stg_loader import StgLoader
from lib.dwh_loader import DwhLoader


log = logging.getLogger(__name__)

@dag(
    schedule_interval='0 12 * * *',  # Ежеднвно в 12 часов.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  
    catchup=False, 
    tags=['final-sprint', 'vertica', 'postgres', 'stg'], 
    is_paused_upon_creation=True 
)
def loader_dag():
    # Создание подключения к базам
    def create_connections():
        pg_conn = ConnectionBuilderPg.pg_conn("PG_CONNECTION")
        vertica_conn = ConnectionBuilderVertica.pg_conn("VERTICA_CONNECTION")
        return pg_conn, vertica_conn

    # Параметр для расчета
    def get_sent_dttm():
        sent_dttm = Variable.get('calculate_date')
        if sent_dttm == '-':
            sent_dttm = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        return sent_dttm

    @task(task_id="stg_loader")
    def load_stg(pg_conn, vertica_conn, sent_dttm):
        loader = StgLoader(pg_conn, vertica_conn, log)
        loader.load_to_stg(sent_dttm)

    @task(task_id="dwh_loader")
    def load_dwh(vertica_conn, sent_dttm):
        loader = DwhLoader(vertica_conn, log)
        loader.load_to_dwh(sent_dttm)

    pg_conn, vertica_conn = create_connections()
    sent_dttm = get_sent_dttm()
    stg = load_stg(pg_conn, vertica_conn, sent_dttm)
    dwh = load_dwh(vertica_conn, sent_dttm)
    
    stg >> dwh 

loader_dag = loader_dag()
