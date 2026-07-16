from sqlalchemy import create_engine, MetaData, Table, engine
from sqlalchemy.sql import text, select
import psycopg2
from logs import get_logger
import os

import pandas as pd

log = get_logger()

ENGINES = dict()
USER = "orders"
PWD = "s3cr3tp455w0rd"
DB = "orders"
HOST = os.environ.get("PG_HOST", "postgres")

def get_table(table, con, schema=None) -> Table:
   """Returns a sqlalchemy Table object via reflection
   :param table: string name of table
   :param con: connection object (sqlalchemy / psycopg2 etc)
   :schema: if the table is not in the default schema, put the string name here
   :returns: sqlalchemy Table object"""
   meta = MetaData(schema=schema)
   return Table(table, meta, autoload_with=con)

def get_connection_string():
   user = os.environ.get("PG_USER", "orders")
   pwd = os.environ.get("PG_PASSWORD", "s3cr3tp455w0rd")
   host = os.environ.get("PG_HOST", "postgres")
   db = os.environ.get("PG_DB", "orders")

   return f"postgresql+psycopg2://{user}:{pwd}@{host}:5432/{db}"

def get_sqlalchemy_engine() -> engine.Engine :
   if "local" not in ENGINES:
      ENGINES["local"] = create_engine(get_connection_string())
   else:
      eng = ENGINES["local"]
      try:
         with eng.connect() as con:
            con.execute(text("SELECT 1"))
      except Exception as e:
         log.warning(f'Error connection to postgres engine: {e}')
         ENGINES["local"] = create_engine(get_connection_string())
   return ENGINES["local"]

def get_psycopg2_connection() -> psycopg2.extensions.connection:
   connection_dict = {
      'dbname': DB,
      'user': USER,
      'password': PWD,
      'port': 5432,
      'host': HOST
   }
   return psycopg2.connect(**connection_dict)

def test_connections():
   """Tests the sqlalchemy connection and the psycopg2 connection"""
   log.info(f'Testing sqlalchemy engine')
   eng = get_sqlalchemy_engine()
   try:
      with eng.connect() as con:
         con.execute(text("SELECT 1"))
      log.info("sqlalchemy connection works")
   except Exception as e:
      log.warning(f"Error with sqlalchemy connection: {e}")

   log.info("Testing psycopg2 connection")
   try:
      con = get_psycopg2_connection()
      with con.cursor() as con:
         res = con.execute("SELECT 1;")
      log.info("psycopg2 connection works")
   except Exception as e:
      log.warning(f"Error with psycopg2 connection: {e}")

def check_schema():
   """Runs the CREATE TABLE statements and checks the tables"""
   log.info(f"Running postgres/schema.sql")
   eng = get_sqlalchemy_engine()
   with open("../postgres/schema.sql", "r") as fh:
      sql = text(fh.read())
   with eng.connect() as con:
      con.execute(sql)
      con.commit()
      info = get_table("tables", con, schema="information_schema")
      table_q = select(info.c.table_name, info.c.table_type).where(info.c.table_schema=="public")
      df = pd.read_sql(table_q, con)
      log.info(f"Tables:\n{df}")
   return df
