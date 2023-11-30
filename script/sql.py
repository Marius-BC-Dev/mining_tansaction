import json
import os
import re
import traceback
from decimal import Decimal

import psycopg2
import psycopg2.extras
import redis
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv()
db_config_dict = json.load(open('conf/config.json', 'r')).get(os.getenv('NETWORK_TYPE')).get('databases')
dbs = {}

max_write_rows = 1000
page_size = 1000

class PostgresDB:
    def __init__(self, dbname, user, password, host='localhost', port=5432):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def connect(self):
        self.connection = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            cursor_factory=RealDictCursor
        )
        self.cursor = self.connection.cursor()

    def disconnect(self):
        if self.connection:
            self.connection.close()

    def read_data(self, sql_query):
        try:
            self.connect()
            self.cursor.execute(sql_query)
            result = self.cursor.fetchall()
            rows = []
            for row in result:
                row = dict(row)
                rows.append(row)
            return rows
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error reading data:", error)
            print(sql_query)
            return []

    def write_data(self, table_name, data_list, update=False, primary_key='id'):
        sql_query = ''
        try:
            self.connect()
            wrote_row_count = 0
            while wrote_row_count < len(data_list):
                write_data = data_list[wrote_row_count:min(wrote_row_count + max_write_rows, len(data_list))]

                keys = list(write_data[0].keys())
                keys.remove(primary_key)
                keys.insert(0, primary_key)

                placeholders, sql_query = [], ''
                for data in write_data:
                    values = []
                    for k in keys:
                        value = data.get(k)
                        value = '\'{}\''.format(value)
                        values.append(value)
                    # Create a list of placeholder strings for VALUES
                    placeholder = '(' + ', '.join(values) + ')'
                    placeholders.append(placeholder)
                    # Prepare the SQL query based on whether it's an INSERT or UPDATE
                placeholders = ', '.join(placeholders)
                if update:
                    set_query = ', '.join([f"{key} = EXCLUDED.{key}" for key in keys[1:]])
                    sql_query = f"INSERT INTO {table_name} ({', '.join(keys)}) VALUES {placeholders} ON CONFLICT ({primary_key}) DO UPDATE SET {set_query};"
                else:
                    sql_query = f"INSERT INTO {table_name} ({', '.join(keys)}) VALUES {placeholders} ON CONFLICT ({primary_key}) DO NOTHING;"
                # Execute the SQL query
                if sql_query:
                    self.cursor.execute(sql_query)
                    wrote_row_count += max_write_rows
                    # print(table_name, wrote_row_count, min(wrote_row_count + max_write_rows, len(data_list)))

            # Commit changes to the database
            self.connection.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            traceback.print_exc()
            print("Error writing data:", error)
            print(sql_query)
            self.connection.rollback()

    def write_sql(self, sql_query):
        try:
            self.connect()
            self.cursor.execute(sql_query)
            # print(table_name, wrote_row_count, min(wrote_row_count + max_write_rows, len(data_list)))

            # Commit changes to the database
            self.connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            traceback.print_exc()
            print("Error writing data:", error)
            print(sql_query)
            self.connection.rollback()


def init_dbs(db_name):
    if 'redis' not in db_name:
        db_config = db_config_dict[db_name]
        dbs[db_name] = PostgresDB(dbname=db_config.get('NAME'),
                                  host=db_config.get("HOST"),
                                  user=db_config.get("USER"),
                                  password=db_config.get("PASSWORD"),
                                  port=db_config.get("PORT"))
    else:
        dbs[db_name] = redis.Redis(host='localhost', port=6379, db=0)


def get_db(db_name) -> PostgresDB:
    if not dbs.get(db_name):
        init_dbs(db_name)
    return dbs.get(db_name)


def read_from_db(db_name, sql):
    db = get_db(db_name)
    return db.read_data(sql)


def write_into_db(table_name, data_list, update=False, primary_key='id'):
    db = get_db('stablenet')
    db.write_data(table_name, data_list, update, primary_key)


def camel_to_case(name):
    name = re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
    return name


def put_to_redis(key: str, value: str):
    rdb = get_db('redis')
    rdb.set(name=key, value=value)


def get_from_redis(key: str):
    rdb = get_db('redis')
    try:
        value = rdb.get(name=key).decode()
    except:
        value = None
    return value
