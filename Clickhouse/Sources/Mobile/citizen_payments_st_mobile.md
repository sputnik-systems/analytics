---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.17.3
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

```python
# creating a table from s3

query_text = """--sql
    CREATE TABLE db1.citizen_payments_st_mobile
    (
        `report_date` Date,
        `citizen_id`  Int32,
        `state` String,
        `amount` Int32,
        `paid_at` DateTime,
        `refunded_at` DateTime,
        `refunded_amount` Int16,
        `from` String,
        `payment_id` Int32,
        `product_ids` String
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/citizen_payments_st_mobile/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
# creating a table for materialized view

query_text = """--sql
    CREATE TABLE db1.citizen_payments_st_mobile_ch
    (
        `report_date` Date,
        `citizen_id`  Int32,
        `state` String,
        `amount` Int32,
        `paid_at` DateTime,
        `refunded_at` DateTime,
        `refunded_amount` Int16,
        `from` String,
        `payment_id` Int32,
        `product_ids` String
    )
    ENGINE = MergeTree()
    ORDER BY citizen_id
    """

ch.query_run(query_text)
```

```python
# creating a materialized view

query_text = """--sql
    CREATE MATERIALIZED VIEW db1.citizen_payments_st_mobile_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.citizen_payments_st_mobile_ch AS
    SELECT
        *
    FROM db1.citizen_payments_st_mobile
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
    FROM db1.citizen_payments_st_mobile_ch
    order by report_date desc
    LIMIT 2
    """

ch.query_run(query_text)
```

### Drop ch

```python
query_text = """
    DROP TABLE db1.citizen_payments_st_mobile_ch
    """
ch.query_run(query_text)
```

### Drop mv

```python
query_text = """
    DROP TABLE db1.citizen_payments_st_mobile_mv
    """
ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.citizen_payments_st_mobile_mv
"""

ch.query_run(query_text)
```
