from airflow import DAG
from airflow.models import Connection
from airflow import settings
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

def create_connections():
    session = settings.Session()

    connections = [
        Connection(
            conn_id='novadrive-principal',
            conn_type='postgres',
            host='159.223.187.110',
            login='etlreadonly',
            password='novadrive376A@',
            port=5432,
            schema='novadrive'
        ),
        Connection(
            conn_id='novadrive-stage',
            conn_type='postgres',
            host='host.docker.internal',
            login='postgres',  # Substitua pelo seu usuário
            password='senha',  # Substitua pela sua senha
            port=5432,
            schema='novadrive-stage.public'
        ),
        Connection(
            conn_id='postgres-default',
            conn_type='postgres',
            host='airflow-postgres-1',
            login='airflow', 
            password='airflow',  
            port=5432,
            schema='postgres'
        )


    ] 

    for conn in connections:
        existing_conn = session.query(Connection).filter_by(conn_id=conn.conn_id).first()
        if existing_conn:
            print(f"Conexão {conn.conn_id} já existe")
        else:
            session.add(conn)
            print(f"Adicionando: {conn.conn_id}")
    
    session.commit()
    print("Conexões adicionadas com sucesso!")

with DAG(
    dag_id='create_airflow_connections',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='DAG para criar conexões no Airflow',
) as dag:

    create_connections_task = PythonOperator(
        task_id='create_connections_task',
        python_callable=create_connections,
    )
