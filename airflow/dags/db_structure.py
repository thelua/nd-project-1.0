from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import psycopg2


def create_db():
    connection = psycopg2.connect(
        host="postgres",
        port=5432,
        user="airflow",
        password="airflow",
        dbname="postgres"  
    )
    connection.autocommit = True
    cursor = connection.cursor()

    cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'novadrive_stage'")
    exists = cursor.fetchone()
    if not exists:
        cursor.execute("CREATE DATABASE novadrive_stage OWNER airflow;")
        print("Banco de dados criado")
    else:
        print("Banco de dados já existe")

    cursor.close()
    connection.close()


with DAG(
    "database_structure",
    default_args={"retries": 1},
    description="Cria nosso banco de dados e suas tabelas",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    create_db_task = PythonOperator(
        task_id="create_database",
        python_callable=create_db,
    )

 
    create_table_task = PostgresOperator(
        task_id="create_users_table",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS estados (
                id_estados SERIAL PRIMARY KEY,
                estado VARCHAR(100) NOT NULL,
                sigla CHAR(2) NOT NULL,
                data_inclusao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_atualizacao TIMESTAMP
            );

        CREATE TABLE IF NOT EXISTS cidades (
                id_cidades SERIAL PRIMARY KEY,
                cidade VARCHAR(255) NOT NULL,
                id_estados INTEGER NOT NULL REFERENCES estados(id_estados) ON DELETE CASCADE,
                data_inclusao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_atualizacao TIMESTAMP
            );

        CREATE TABLE IF NOT EXISTS veiculos (
                id_veiculos SERIAL PRIMARY KEY,
                nome VARCHAR(255) NOT NULL,
                tipo VARCHAR(100) NOT NULL,
                valor DECIMAL(10, 2) NOT NULL,
                data_atualizacao TIMESTAMP,
                data_inclusao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );



        CREATE TABLE IF NOT EXISTS concessionarias (
                id_concessionarias SERIAL PRIMARY KEY,
                concessionaria VARCHAR(255) NOT NULL,
                id_cidades INTEGER NOT NULL REFERENCES cidades(id_cidades) ON DELETE CASCADE,
                data_inclusao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_atualizacao TIMESTAMP
            );

        CREATE TABLE IF NOT EXISTS vendedores (
                id_vendedores SERIAL PRIMARY KEY,
                nome VARCHAR(255) NOT NULL,
                id_concessionarias INTEGER NOT NULL REFERENCES concessionarias(id_concessionarias) ON DELETE CASCADE,
                data_inclusao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_atualizacao TIMESTAMP
            );

        CREATE TABLE IF NOT EXISTS clientes (
                id_clientes SERIAL PRIMARY KEY,
                cliente VARCHAR(255) NOT NULL,
                endereco TEXT NOT NULL,
                id_concessionarias INTEGER NOT NULL REFERENCES concessionarias(id_concessionarias) ON DELETE CASCADE,
                data_inclusao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_atualizacao TIMESTAMP
            );

         CREATE TABLE IF NOT EXISTS vendas (
                id_vendas SERIAL PRIMARY KEY,
                id_veiculos INTEGER NOT NULL REFERENCES veiculos(id_veiculos) ON DELETE CASCADE,
                id_concessionarias INTEGER NOT NULL REFERENCES concessionarias(id_concessionarias) ON DELETE CASCADE,
                id_vendedores INTEGER NOT NULL REFERENCES vendedores(id_vendedores) ON DELETE CASCADE,
                id_clientes INTEGER NOT NULL REFERENCES clientes(id_clientes) ON DELETE CASCADE,
                valor_pago DECIMAL(10, 2) NOT NULL,
                data_venda TIMESTAMP,
                data_inclusao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_atualizacao TIMESTAMP
            );




        """,
        database="novadrive_stage", 
    )

    create_db_task >> create_table_task
