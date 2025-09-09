---
jupyter:
  jupytext:
    cell_metadata_filter: -all
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.17.2
  kernelspec:
    display_name: myenv
    language: python
    name: python3
---

# citizens_st_mobile
___



## Start

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

### Tags: #Source #Mobile #YandexFunctions

### Links: 
[[clichouse_schedule_function]]
___


### creating a table from s3

```python
query_text = """--sql
    CREATE TABLE db1.citizens_st_mobile
    (
    `report_date` Date,
    `citizen_id` Int32,
    `trial_available` Int32,
    `state` String,
    `activated_at` String,
    `flat_uuid` String,
    `address_uuid` String
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/citizens_st_mobile/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

### creating a table in ch

```python
query_text = """--sql
    CREATE TABLE db1.citizens_st_mobile_ch
    (
    `report_date` Date,
    `citizen_id` Int32,
    `trial_available` Int32,
    `state` String,
    `activated_at` DateTime,
    `flat_uuid` String,
    `address_uuid` String
    )
    ENGINE = MergeTree()
    ORDER BY citizen_id
    """

ch.query_run(query_text)
```

### creating a mv

dosn't work now

```python
query_text = """--sql
 --   CREATE MATERIALIZED VIEW db1.citizens_st_mobile_mv
 --   REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.citizens_st_mobile_ch AS
 --  SELECT
 --       *
 --   FROM db1.citizens_st_mobile
    """

ch.query_run(query_text)
```

### add data before 2025-05-30

```python
start_date = datetime.datetime.strptime('2023-07-10','%Y-%m-%d').date()
end_date = datetime.datetime.strptime('2025-05-29','%Y-%m-%d').date()

dates_pd = pd.DataFrame({
        'date': pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d'),
        'date_key': pd.date_range(start=start_date, end=end_date).strftime('%/year=%-Y/month=%m/%d%')
        })
dates_pd = dates_pd.iloc[::-1].reset_index(drop=True)
for day_index in dates_pd.index:
    date = dates_pd.loc[day_index,['date']].values[0]
    date_key = dates_pd.loc[day_index,['date_key']].values[0]
    query_text = f"""
        INSERT INTO db1.citizens_st_mobile_ch
        SELECT
            citizens_st_mobile.report_date AS report_date,
            citizens_st_mobile.citizen_id AS citizen_id,
            citizens_st_mobile.trial_available AS trial_available,
            citizens_st_mobile.state AS state,
            toDateTimeOrZero(citizens_st_mobile_ch_2025_05_30.activated_at) AS activated_at,
            citizens_st_mobile_ch_2025_05_30.flat_uuid AS flat_uuid,
            citizens_st_mobile_ch_2025_05_30.address_uuid AS address_uuid
        FROM db1.citizens_st_mobile AS citizens_st_mobile
        LEFT JOIN
            (SELECT
                citizen_id,
                flat_uuid,
                address_uuid,
                activated_at
            FROM db1.citizens_st_mobile
            WHERE report_date = '2025-05-30'
             AND _path LIKE '%/year=2025/month=05/30%') AS citizens_st_mobile_ch_2025_05_30
            ON citizens_st_mobile_ch_2025_05_30.citizen_id = citizens_st_mobile.citizen_id
        WHERE report_date = '{date}'
            AND _path LIKE '{date_key}'
    """
    ch.query_run(query_text)
    print(date)
# ch.query_run(query_text)
```

### add data after 2025-05-30

```python
start_date = datetime.datetime.strptime('2025-08-27','%Y-%m-%d').date()
end_date = datetime.datetime.strptime('2025-08-27','%Y-%m-%d').date()

dates_pd = pd.DataFrame({
        'date': pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d'),
        'date_key': pd.date_range(start=start_date, end=end_date).strftime('%/year=%-Y/month=%m/%d%')
        })
dates_pd = dates_pd.iloc[::-1].reset_index(drop=True)
for day_index in dates_pd.index:
    date = dates_pd.loc[day_index,['date']].values[0]
    date_key = dates_pd.loc[day_index,['date_key']].values[0]
    query_text = f"""
        INSERT INTO db1.citizens_st_mobile_ch
        SELECT
            report_date,
            citizen_id,
            trial_available,
            state,
            toDateTimeOrZero(activated_at) AS activated_at,
            flat_uuid,
            address_uuid
        FROM db1.citizens_st_mobile
        WHERE report_date = '{date}'
        AND _path LIKE '{date_key}'
        
    """
    ch.query_run(query_text)
    print(date)
# ch.query_run(query_text)
```

___
## Tools
___
### query


```python
query_text = """--sql
    SELECT
        report_date,
        count(*)
    FROM db1.citizens_st_mobile_ch
    GROUP By report_date
    ORDER BY report_date desc
    limit 100
    """

ch.query_run(query_text)

```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.citizens_st_mobile_ch DELETE WHERE report_date = '2025-08-26'
    """

ch.query_run(query_text)

```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.citizens_st_mobile_mv
    """

ch.query_run(query_text)
```


### drop ch

```python
query_text = """--sql
    DROP TABLE db1.citizens_st_mobile_ch
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.t_subscribtions_citizens_by_companies_and_cities_ch
"""

ch.query_run(query_text)
```


