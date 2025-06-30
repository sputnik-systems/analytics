---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.17.1
  kernelspec:
    display_name: myenv
    language: python
    name: python3
---

```python
import clickhouse_connect
import datetime
import os
import pytz
import pandas as pd
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import sys
sys.path.append('/home/boris/Documents/Work/analytics/Clickhouse')
from clickhouse_client import ClickHouse_client
ch = ClickHouse_client()
pd.set_option('display.max_rows', 1000)
```

# citizens_st_mobile_parquet remaking

```python
start_date = datetime.datetime.strptime('2023-07-06','%Y-%m-%d').date()
end_date = datetime.datetime.strptime('2025-04-21','%Y-%m-%d').date()

dates_pd = pd.DataFrame({
        'date': pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d'),
        'date_key': pd.date_range(start=start_date, end=end_date).strftime('year=%Y/month=%m/%d.parquet')
        })
dates_pd = dates_pd.iloc[::-1].reset_index(drop=True)
for day_index in dates_pd.index:
    date = dates_pd.loc[day_index,['date']].values[0]
    date_key = dates_pd.loc[day_index,['date_key']].values[0]
    query_text = f"""
        INSERT INTO FUNCTION s3(
        'https://storage.yandexcloud.net/dwh-asgard/citizens_st_mobile_parquet_uuid/{date_key}',
        'parquet'
        )
        SETTINGS s3_truncate_on_insert = 1
        SELECT
            citizens_st_mobile.report_date AS report_date,
            citizens_st_mobile.citizen_id AS citizen_id,
            citizens_st_mobile.trial_available AS trial_available,
            citizens_st_mobile.state AS state,
            citizens_st_mobile_ch_2025_05_30.flat_uuid AS flat_uuid,
            citizens_st_mobile_ch_2025_05_30.address_uuid AS address_uuid
        FROM db1.citizens_st_mobile_ch AS citizens_st_mobile
        LEFT JOIN
            (SELECT
                citizen_id,
                flat_uuid,
                address_uuid
            FROM db1.citizens_st_mobile_ch 
            WHERE report_date = '2025-05-30') AS citizens_st_mobile_ch_2025_05_30
            ON citizens_st_mobile_ch_2025_05_30.citizen_id = citizens_st_mobile.citizen_id
        WHERE report_date = '{date}'
        
    """
    ch.query_run(query_text)
    print(date)
# ch.query_run(query_text)

```

## Обновление данных после 2025-05-30

```python
start_date = datetime.datetime.strptime('2025-05-30','%Y-%m-%d').date()
end_date = datetime.datetime.strptime('2025-04-21','%Y-%m-%d').date()

dates_pd = pd.DataFrame({
        'date': pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d'),
        'date_key': pd.date_range(start=start_date, end=end_date).strftime('year=%Y/month=%m/%d.parquet')
        })
dates_pd = dates_pd.iloc[::-1].reset_index(drop=True)
for day_index in dates_pd.index:
    date = dates_pd.loc[day_index,['date']].values[0]
    date_key = dates_pd.loc[day_index,['date_key']].values[0]
    query_text = f"""
        INSERT INTO FUNCTION s3(
        'https://storage.yandexcloud.net/dwh-asgard/citizens_st_mobile_parquet/{date_key}',
        'parquet'
        )
        SETTINGS s3_truncate_on_insert = 1
        SELECT
            report_date,
            citizen_id,
            trial_available,
            state,
            flat_uuid,
            address_uuid
        FROM db1.citizens_st_mobile_ch
        WHERE report_date = '{date}'
        
    """
    ch.query_run(query_text)
    print(date)
# ch.query_run(query_text)

```

```python
query_text = """
SYSTEM REFRESH VIEW db1.citizens_st_mobile_parquet_mv
"""

ch.query_run(query_text)
```

citizens_st_mobile_parquet_mv

```python
query_text = """
SELECT
    *
FROM db1.citizens_st_mobile_parquet
WHERE flat_uuid != ''
ORDER BY report_date
limit 10
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.citizens_st_mobile_remake_parquet_ch
    (
    `report_date` Date,
    `citizen_id` Int32,
    `trial_available` Int32,
    `state` String,
    `flat_uuid` String,
    `address_uuid` String
    )
    ENGINE = MergeTree()
    ORDER BY report_date
    """

ch.query_run(query_text)
```

```python
query_text = """
CREATE MATERIALIZED VIEW db1.citizens_st_mobile_remake_parquet_mv
REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.citizens_st_mobile_remake_parquet_ch AS
INSERT INTO FUNCTION s3(
    'https://storage.yandexcloud.net/dwh-asgard/citizens_st_mobile_remake_parquet_ch/{date_key}',
    'parquet'
    )
    SETTINGS s3_truncate_on_insert = 1
    SELECT
        report_date,
        citizen_id,
        trial_available,
        state,
        flat_uuid,
        address_uuid
    FROM db1.citizens_st_mobile_ch AS citizens_st_mobile
    WHERE report_date = '{date}'
        
    """

ch.query_run(query_text)
```
