from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ingestao_dados_exame',
    default_args=default_args,
    description='Copiar dados de exames, fazer a normalizacao e inserir no StorageAccount',
    schedule_interval="0 */2 * * *",
)

carregar_dados_camada_raw = BashOperator(
    task_id='carregar_dados_camada_raw', 
    bash_command='python3 /home/azureuser/airflow/scripts/carregar_dados_camada_raw.py',
    dag=dag,
)

normalizar_dados = BashOperator(
    task_id='normalizar_dados',  
    bash_command='python3 /home/azureuser/airflow/scripts/normalizar_dados.py',
    dag=dag,
)

carregar_dados_consume_zone = BashOperator(
    task_id='carregar_dados_consume_zone',   
    bash_command='python3 /home/azureuser/airflow/scripts/carregar_dados_consume_zone.py',
    dag=dag,
)

inicio = DummyOperator(task_id="inicio", dag=dag)
fim = DummyOperator(task_id="fim", dag=dag)

inicio >> carregar_dados_camada_raw >> normalizar_dados >> carregar_dados_consume_zone >> fim