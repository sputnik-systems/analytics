---
jupyter:
  jupytext:
    cell_metadata_filter: -all
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

___
### Tags: #Source #Mobile

### Links: 
___


## Creating a table from s3

doesn't work now

```python
query_text = """--sql
    CREATE TABLE db1.citizens_st_mobile_parquet
    (
    `report_date` Date,
    `citizen_id` Int32,
    `trial_available` Int32,
    `activated_at` String,
    `state` String,
    `flat_uuid` String,
    `address_uuid` String
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/citizens_st_mobile_parquet/year=*/month=*/*.parquet','parquet')
    """

ch.query_run(query_text)
```

## Creating table in CH

```python
query_text = """--sql
    CREATE TABLE db1.citizens_st_mobile_parquet_ch
    (
    `report_date` Date,
    `citizen_id` Int32,
    `trial_available` Int32,
    `activated_at` String,
    `state` String,
    `flat_uuid` String,
    `address_uuid` String
    )
    ENGINE = MergeTree()
    ORDER BY report_date
    """

ch.query_run(query_text)
```

## Creating MV

doesn't work now

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.citizens_st_mobile_parquet_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR 30 MINUTE RANDOMIZE FOR 1 HOUR TO db1.citizens_st_mobile_parquet_ch AS
    SELECT
        *
    FROM db1.citizens_st_mobile_parquet
    """

ch.query_run(query_text)
```

___
## Tools


### query

```python
query_text = """--sql
    SELECT
        *
    FROM db1.citizens_st_mobile_parquet_ch
    WHERE report_date = '2025-05-29'
    limit 100
    """

ch.query_run(query_text)
```

### Drop ch

```python
query_text = """
    DROP TABLE db1.citizens_st_mobile_parquet_ch
    """
ch.query_run(query_text)
```

### Drop mv

```python
query_text = """
    DROP TABLE db1.citizens_st_mobile_parquet_mv
    """
ch.query_run(query_text)
```
