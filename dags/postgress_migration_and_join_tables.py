import pandas as pd

from datetime import datetime, timedelta
from sqlalchemy import create_engine

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook

default_args = {
    'owner': 'ayslanleal',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
} 

def migrate_data_venda():
    conn = BaseHook.get_connection("db_venda")
    engine_src = create_engine('postgresql://{}:{}@{}:{}/{}'.format(conn.login, conn.password,conn.host,conn.port, conn.schema))

    conn_tgt = BaseHook.get_connection("db_destino")
    engine_tgt = create_engine('postgresql://{}:{}@{}:{}/{}'.format(conn_tgt.login, conn_tgt.password,conn_tgt.host,conn_tgt.port, conn_tgt.schema))

    query = """select * from venda"""
    df_vendas = pd.read_sql(query, engine_src)
    df_vendas.to_sql('vendas', engine_tgt, if_exists='replace', index=False)

def migrate_data_funcionario():
    conn = BaseHook.get_connection("db_categoria_funcionario")
    engine_src = create_engine('postgresql://{}:{}@{}:{}/{}'.format(conn.login, conn.password,conn.host,conn.port, conn.schema))

    conn_tgt = BaseHook.get_connection("db_destino")
    engine_tgt = create_engine('postgresql://{}:{}@{}:{}/{}'.format(conn_tgt.login, conn_tgt.password,conn_tgt.host,conn_tgt.port, conn_tgt.schema))

    query = """select * from funcionario"""
    df_vendas = pd.read_sql(query, engine_src)
    df_vendas.to_sql('funcionario', engine_tgt, if_exists='replace', index=False)

def migrate_data_categoria():
    conn = BaseHook.get_connection("db_categoria_funcionario")
    engine_src = create_engine('postgresql://{}:{}@{}:{}/{}'.format(conn.login, conn.password,conn.host,conn.port, conn.schema))

    conn_tgt = BaseHook.get_connection("db_destino")
    engine_tgt = create_engine('postgresql://{}:{}@{}:{}/{}'.format(conn_tgt.login, conn_tgt.password,conn_tgt.host,conn_tgt.port, conn_tgt.schema))

    query = """select * from categoria"""
    df_vendas = pd.read_sql(query, engine_src)
    df_vendas.to_sql('categoria', engine_tgt, if_exists='replace', index=False)

def create_join_table():
    conn_tgt = BaseHook.get_connection("db_destino")
    engine_tgt = create_engine('postgresql://{}:{}@{}:{}/{}'.format(conn_tgt.login, conn_tgt.password,conn_tgt.host,conn_tgt.port, conn_tgt.schema))

    query = """
    select v.id_venda, v.data_venda, v.venda,
    c.nome_categoria ,f.nome_funcionario
    from vendas as v 
    inner join categoria as c
    on v.id_categoria = c.id 
    inner join funcionario as f
    on v.id_funcionario = f.id"""
    
    df_vendas = pd.read_sql(query, engine_tgt)
    df_vendas.to_sql('vendas_agrupadas', engine_tgt, if_exists='replace', index=False)


with DAG(
    dag_id = 'postgres_acess_remote',
    default_args = default_args,
    start_date = datetime(2022,3,7),
    schedule_interval = '@daily'
) as dag:

    task2 = PythonOperator(
        task_id= "migration_venda_table",
        python_callable=migrate_data_venda
    )

    task3 = PythonOperator(
        task_id= "migration_funcionario_table",
        python_callable=migrate_data_funcionario
    )

    task4 = PythonOperator(
        task_id= "migration_categoria_table",
        python_callable=migrate_data_categoria
    )

    task5 = PythonOperator(
        task_id= "create_join_table",
        python_callable=create_join_table
    )
    
    [task2, task3, task4] >> task5
