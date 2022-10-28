#from pgdb import connect
import re
import psycopg2
import os
from queue import Queue
from threading import Thread
from get_metadata_sql import SQL_TO_GET_DATABASES, SQL_TO_GET_SCHEMAS, SQL_TO_GET_TABLES
import pandas as pd 
import numpy as np

def get_percentage_migrated(str_con_src, str_con_dst):
    src = GetTables(str_con_src)
    dst = GetTables(str_con_dst)
    print("comparing sizes")
    pd_tables = pd.DataFrame(src.list_table).merge(pd.DataFrame(dst.list_table),
                    on=["database","schema","table"])
    pd_tables["size_y"]= np.where(pd_tables["size_y"] > pd_tables["size_x"],
                     pd_tables["size_x"], pd_tables["size_y"])
    percentage_migrated = sum(pd_tables["size_y"])/sum(pd_tables["size_x"]) * 100
    return percentage_migrated


class GetTables:
    def __init__(self,str_connection="localhost:5432:postgres:postgres"):
        self.host = str_connection.split(":")[0]
        self.port = str_connection.split(":")[1]
        self.user = str_connection.split(":")[2]
        self.password = str_connection.split(":")[3]
        self.exclusion_list_database = [
            "template1", "template0", "rdsadmin", "cloudsqladmin"
        ]
        self.exclusion_list_schema = [
            "pg_temp_1", "pg_toast_temp_1", "pg_catalog", "information_schema",
            "pglogical"
        ]
        print(f"getting databases for {self.host}")
        self.get_databases()
        print(f"getting tables for {self.host}")
        self.get_tables()
    def get_databases(self):
        cur = self.connect_to_db("postgres").cursor()
        cur.execute(SQL_TO_GET_DATABASES)
        self.list_database = [
            _[0] for _ in cur.fetchall()
            if _[0] not in self.exclusion_list_database
        ]
        cur.close()
    def get_tables(self):
        self.list_schema = []
        self.list_table = []
        for db in self.list_database:
            cur = self.connect_to_db(db).cursor()
            cur.execute(SQL_TO_GET_SCHEMAS)
            list_schema = [{
                "database": db,
                "schema": _[0]
            } for _ in cur.fetchall()
                        if _[0] not in self.exclusion_list_schema]
            self.list_schema.extend(list_schema)

            list_schema_str = ",".join(
                [f"'{ _['schema'] }'" for _ in list_schema])
            cur.execute(SQL_TO_GET_TABLES.format(list_schema_str))
            list_table = [{
                "database": db,
                "schema": _[0],
                "table": _[1],
                "size": _[2]
            } for _ in cur.fetchall()]
            self.list_table.extend(list_table)
    def connect_to_db(self, database):
        conn = psycopg2.connect(dbname=database,
                                host=self.host,
                                user=self.user,
                                password=self.password,
                                port=self.port,
                                connect_timeout=3)
        return conn
