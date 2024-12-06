from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,  
    'start_date': datetime(2024, 1, 1), 
    'retries': 0, 
    'retry_delay': timedelta(minutes=1),  
}


with DAG(
    dag_id='database_structure', 
    default_args=default_args,  
    description='Checa se o nosso banco de dados existe e a estrutura está correta',
    schedule_interval=None, 
    catchup=False 
) as dag:
    
    def create_db():
        postgres_hook = PostgresHook(postgres_conn_id='novadrive-stage')  
        conn = postgres_hook.get_conn()
        conn.autocommit = True  # Necessário para executar CREATE DATABASE fora de uma transação

        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'novadrive_stage'")
            if not cursor.fetchone(): 
                cursor.execute("CREATE DATABASE novadrive_stage")
                print("Banco de dados criado")
            else:
                print("Banco de dados já existe")


    def create_tables():
        
        table_schemas = {
        'veiculos': """
            CREATE TABLE IF NOT EXISTS veiculos (
                id_veiculos SERIAL PRIMARY KEY,
                nome VARCHAR(255) NOT NULL,
                tipo VARCHAR(100) NOT NULL,
                valor DECIMAL(10, 2) NOT NULL,
                data_atualizacao TIMESTAMP,
                data_inclusao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
        'estados': """
            CREATE TABLE IF NOT EXISTS estados (
                id_estados SERIAL PRIMARY KEY,
                estado VARCHAR(100) NOT NULL,
                sigla CHAR(2) NOT NULL,
                data_inclusao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_atualizacao TIMESTAMP
            )
        """,
        'cidades': """
            CREATE TABLE IF NOT EXISTS cidades (
                id_cidades SERIAL PRIMARY KEY,
                cidade VARCHAR(255) NOT NULL,
                id_estados INTEGER NOT NULL REFERENCES estados(id_estados) ON DELETE CASCADE,
                data_inclusao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_atualizacao TIMESTAMP
            )
        """,
        'concessionarias': """
            CREATE TABLE IF NOT EXISTS concessionarias (
                id_concessionarias SERIAL PRIMARY KEY,
                concessionaria VARCHAR(255) NOT NULL,
                id_cidades INTEGER NOT NULL REFERENCES cidades(id_cidades) ON DELETE CASCADE,
                data_inclusao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_atualizacao TIMESTAMP
            )
        """,
        'vendedores': """
            CREATE TABLE IF NOT EXISTS vendedores (
                id_vendedores SERIAL PRIMARY KEY,
                nome VARCHAR(255) NOT NULL,
                id_concessionarias INTEGER NOT NULL REFERENCES concessionarias(id_concessionarias) ON DELETE CASCADE,
                data_inclusao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_atualizacao TIMESTAMP
            )
        """,
        'clientes': """
            CREATE TABLE IF NOT EXISTS clientes (
                id_clientes SERIAL PRIMARY KEY,
                cliente VARCHAR(255) NOT NULL,
                endereco TEXT NOT NULL,
                id_concessionarias INTEGER NOT NULL REFERENCES concessionarias(id_concessionarias) ON DELETE CASCADE,
                data_inclusao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_atualizacao TIMESTAMP
            )
        """,
        'vendas': """
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
            )
        """
    }

        
        postgres_hook = PostgresHook(postgres_conn_id='novadrive-stage')

       
        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                for table_name, create_query in table_schemas.items():
                    cursor.execute(create_query) 
                    print(f"Tabela '{table_name}' criada")

            conn.commit()


    create_db_task = PythonOperator(
        task_id='create_database_if_not_exists', 
        python_callable=create_db  
    )

  
    create_tables_task = PythonOperator(
        task_id='create_tables_if_not_exists',  
        python_callable=create_tables 
    )

   

    create_db_task >> create_tables_task
