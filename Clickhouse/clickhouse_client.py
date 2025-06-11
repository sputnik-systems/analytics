import clickhouse_connect
import datetime
import os
import pytz
import pandas as pd
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

load_dotenv()
ClickHouse_host = os.environ['ClickHouse_host']
ClickHouse_port = os.environ['ClickHouse_port']
ClickHouse_username = os.environ['ClickHouse_username']
ClickHouse_password = os.environ['ClickHouse_password']


class ClickHouse_client:
    def __init__(self):
        self.client = clickhouse_connect.get_client(host=ClickHouse_host, 
                                        port=ClickHouse_port, 
                                        username=ClickHouse_username, 
                                        password=ClickHouse_password, 
                                        secure=True,
                                        verify=False)


    def query_run (self, query_text):
        query_text = query_text
        result = self.client.query(query_text)
        self.df = pd.DataFrame(result.result_rows, columns=result.column_names)
        # display(self.df)
        return self.df