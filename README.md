# ND - Orquestração com Airflow, Postgres e DBT
O projeto 'NovaDrive' conta com a orquestração de um Pipeline automatizado para ingestão e armazenamento de dados de uma concessionária fictícia. 

### Ferramentas 
Foram utilizados Apache Airflow, Docker, dbt e PostgreSQL.

## Arquitetura 
   Fonte de Dados Original (Postgres) --> Airflow (Orquestração) --> Fonte de Dados em Stage (Postgres)
               |--> dbt (Transformações)
               |--> Visualização de Dados (Power BI)

              
## Requisitos

Dentro do diretório do airflow:

```
docker-compose up -d
``` 