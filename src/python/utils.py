from typing import Dict
from enum import Enum
from sqlalchemy import create_engine, text
import functools
import jinja2
import logging
from config import config


logger = logging.getLogger('cluster_pipeline')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

engine_temp_duckdb = create_engine(config.db.temp_duckdb, echo=True)
engine_clusterdb_postgres = create_engine(config.db.clusterdb_postgres, echo=True)


class DB(Enum):
    TEMP_DUCKDB = 'temp_duckdb'
    CLUSTERDB_POSTGRES = 'clusterdb_postgres'


def run_sql_script_on_db(db: DB):
    def decorator_run_sql_script_on_db(func):
        @functools.wraps(func)
        def wrapper_sql_script_on_db(*args, **kwargs):
            e = get_db_engine(db)
            sql_file_path, params = func(*args, **kwargs)
            with e.begin() as conn:
                execute_sql_file(conn=conn,
                                 file_path=sql_file_path,
                                 params=params)
            conn.close()

        return wrapper_sql_script_on_db
    return decorator_run_sql_script_on_db


def get_db_engine(db: DB):
    if db == DB.TEMP_DUCKDB:
        return engine_temp_duckdb
    elif db == DB.CLUSTERDB_POSTGRES:
        return engine_clusterdb_postgres
    else:
        raise ValueError(f"Database {db.value} not supported")


def execute_sql_file(conn, file_path: str, params: Dict[str, str] = None):
    with open(file_path) as f:
        sql = f.read()

    if params is None:
        params = {}

    template = jinja2.Template(sql)
    sql = template.render(params=params)
    conn.execute(text(sql))