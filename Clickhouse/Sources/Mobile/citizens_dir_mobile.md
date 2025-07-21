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
    CREATE TABLE db1.citizens_dir_mobile
    (
    `created_at` String,
    `activated_at` String,
    `localization` String,
    `flat_uuid` String,
    `address_uuid` String,
    `citizen_id` Int32
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/citizens_dir_mobile/citizens_dir_mobile.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
# creating a table for materialized view

query_text = """--sql
    CREATE TABLE db1.citizens_dir_mobile_ch
    (
    `created_at` String,
    `activated_at` String,
    `localization` String,
    `flat_uuid` String,
    `address_uuid` String,
    `citizen_id` Int32
    )
    ENGINE = MergeTree()
    ORDER BY citizen_id
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.citizens_dir_mobile_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.citizens_dir_mobile_ch AS
    SELECT
        *
    FROM db1.citizens_dir_mobile
    """

ch.query_run(query_text)
```

___
## Tools

### query
___

```python
query_text = """--sql
    SELECT
        *
    FROM db1.citizens_dir_mobile
    limit 100
    """

ch.query_run(query_text)

```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.citizens_dir_mobile_mv
    """

ch.query_run(query_text)
```


### drop ch

```python
query_text = """--sql
    DROP TABLE db1.citizens_dir_mobile_ch
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.citizens_dir_mobile_mv
"""

ch.query_run(query_text)
```
