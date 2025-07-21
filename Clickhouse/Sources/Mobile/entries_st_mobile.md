---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.17.2
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

```python
# creating a table from s3

query_text = """--sql
    CREATE TABLE db1.entries_st_mobile
    (
        `report_date` Date,
        `address_uuid`  String,
        `partner_uuid` String,
        `monetization` String,
        `ble_available` String,
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/entries_st_mobile/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
# creating a table for materialized view

query_text = """--sql
    CREATE TABLE db1.entries_st_mobile_ch
    (
        `report_date` Date,
        `address_uuid`  String,
        `partner_uuid` String,
        `monetization` String,
        `ble_available` String,
    )
    ENGINE = MergeTree()
    ORDER BY address_uuid
    """

ch.query_run(query_text)
```

```python
# creating a materialized view

query_text = """--sql
    CREATE MATERIALIZED VIEW db1.entries_st_mobile_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.entries_st_mobile_ch AS
    SELECT
        *
    FROM db1.entries_st_mobile
    """

ch.query_run(query_text)
```

___
## Tools
___
### query

```python
query_text = """--sql
    SELECT
        *
    FROM db1.entries_st_mobile_ch
    LIMIT 2
    """

ch.query_run(query_text)
```

### Drop ch

```python
query_text = """
    DROP TABLE db1.entries_st_mobile_ch
    """
ch.query_run(query_text)
```

### Drop mv

```python
query_text = """
    DROP TABLE db1.entries_st_mobile_mv
    """
ch.query_run(query_text)
```
