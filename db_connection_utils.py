from fed_etl.tools.db_connection import PgConnection, PgConnectionException
import logging
from sqlalchemy import inspect
from psycopg2 import sql, extras
from torrential.util import get_base_log

logging.basicConfig(level=logging.INFO)


class PgConnectionUtil(object):

    def __init__(self, server, env):
        self.conn = PgConnection(server, env).connect
        self.engine = self.conn.engine
        self.inspector = inspect(self.engine)
        self.connection = self.engine.raw_connection()
        self.cursor = self.connection.cursor(cursor_factory=extras.RealDictCursor)

    def get_table_metadata(self, schema_name, table_name):
        base_log = get_base_log()
        try:
            rs = self.inspector.get_columns(table_name=table_name, schema=schema_name)
            if rs:
                logging.info(f"""{base_log}: {schema_name}.{table_name} table metadata generated""")
            else:
                logging.info(f"""{base_log}: {schema_name}.{table_name} table not found in schema""")
        except:
            logging.error(f"""{base_log}: {schema_name}.{table_name} table column metadata errored out""")
            raise PgConnectionException(f"""{base_log}: {schema_name}.{table_name} table column metadata errored out""")
        return rs

    def create_table(self, schema_name, table_name, create_table_script):
        base_log = get_base_log()
        try:
            create_table_sql = f"""CREATE TABLE %s.%s (%s);""" % (schema_name, table_name, create_table_script)
            result = self.conn.execute_sql_query_return_result_set(create_table_sql,
                                                                   log_query=False)
            logging.info(f"""{base_log}: {schema_name}.{table_name} table has been created""")
            if result is None:
                logging.error(f"""{base_log}: {schema_name}.{table_name} error creating table, aborting""")
                raise PgConnectionException(
                    f"""{base_log}: {schema_name}.{table_name} error creating table, aborting""")
        except Exception as e:
            logging.error(f"""{base_log}: {schema_name}.{table_name} create table script has errors""")
            raise PgConnectionException(
                f"""{base_log}: {schema_name}.{table_name} create table script has errors""")


    def identify_primary_key(self, schema_name, table_name):
        base_log = get_base_log()
        try:
            rs = self.inspector.get_pk_constraint(table_name=table_name, schema=schema_name)
            if rs['constrained_columns']:
                logging.info(f"""{base_log}: {schema_name}.{table_name} primary key identified""")
            else:
                logging.info(f"""{base_log}: {schema_name}.{table_name} primary not found""")
        except:
            logging.error(f"""{base_log}: {schema_name}.{table_name} primary key identification errored out""")
            raise PgConnectionException(f"""{base_log}: {schema_name}.{table_name} primary key identification errored out""")
        return rs

    def insert_into_table(self, schema_name, table_name, list_of_values):
        base_log = get_base_log()
        column_names = (list(set([key for rows in list_of_values for key in rows.keys()])))
        insert_into = f"""Insert into {schema_name}.{table_name}"""
        query = sql.SQL(insert_into + " ({}) values ({}) ON CONFLICT DO NOTHING").format(
            sql.SQL(', ').join(map(sql.Identifier, column_names)),
            sql.SQL(', ').join(map(sql.Placeholder, column_names)))
        try:
            extras.execute_batch(self.cursor, query, list_of_values)
            self.connection.commit()
            logging.info(f"""{base_log}: {schema_name}.{table_name} insert data into table""")
        except Exception as e:
            logging.error(f"""{base_log}: {schema_name}.{table_name} insert data into table failed""")
            raise PgConnectionException(f"""{base_log}: {schema_name}.{table_name} insert data into table failed""")

