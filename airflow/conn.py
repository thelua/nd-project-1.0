from airflow.models import Connection
from airflow import settings

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

        #conexao do stage criado localmente
        Connection(
            conn_id='novadrive-stage',
            conn_type='postgres',
            host='host.docker.internal',
            login='postgres', ##Coloque seu  usuario aqui 
            password='senha', ##Coloque sua senha aqui 
            port=5432,
            schema='novadrive-stage.public'
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
    print("Adicionado")

if __name__ == "__main__":
    create_connections()
