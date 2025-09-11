import clickhouse_connect
import datetime
import os
import pytz
import pandas as pd
import polars as pl
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

load_dotenv()
ClickHouse_host = os.environ.get('ClickHouse_host')
ClickHouse_port = os.environ.get('ClickHouse_port')
ClickHouse_username = os.environ.get('ClickHouse_username')
ClickHouse_password = os.environ.get('ClickHouse_password')


class ClickHouse_client:
    def __init__(self):
        self.client = clickhouse_connect.get_client(host=ClickHouse_host, 
                                        port=ClickHouse_port, 
                                        username=ClickHouse_username, 
                                        password=ClickHouse_password, 
                                        secure=True,
                                        verify=False,
                                        database='db1')


    def query_run (self, query_text):
        query_text = query_text
        result = self.client.query(query_text)
        self.df = pd.DataFrame(result.result_rows, columns=result.column_names)
        # self.df = pl.DataFrame(result.result_rows, schema=result.column_names)
        # self.df =  pl.from_pandas(self.df)
        # display(self.df)
        return self.df

    def get_schema (self, query_text):
        query_text = query_text
        result = self.client.query(query_text)
        column_names = result.column_names
        column_types = result.column_types
        columns_description = "(\n" + ",\n".join(f"    `{name}` {str(type).split('.')[-1].split()[0]}" for name, type in zip(column_names, column_types)) + "\n)"

        # Вывод результата
        print(columns_description)

        # print(self.query_schema)
        # display(self.query_schema)
        # return(self.query_schema)
