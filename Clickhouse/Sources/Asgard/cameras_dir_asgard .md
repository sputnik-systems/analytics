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
    display_name: Python (myenv)
    language: python
    name: myenv
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

___
### Tags: #Source #Asgard

### Links: 
___
# creating a table from s3

```python
query_text = """--sql 
   CREATE TABLE db1.cameras_dir_asgard
(
    `camera_serial` String,
    `intercom_uuid` String,
    `camera_uuid` String,
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/cameras_dir_asgard/*.csv','CSVWithNames')
    """
```

```python

query_text = """--sql
    CREATE TABLE db1.cameras_dir_asgard_ch
(
    `camera_serial` String,
    `intercom_uuid` String,
    `camera_uuid` String,
)
    ENGINE = MergeTree()
    ORDER BY intercom_uuid
    """
ch.query_run(query_text)
```

```python

query_text = """--sql
    CREATE MATERIALIZED VIEW db1.cameras_dir_asgard_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.cameras_dir_asgard_ch AS
    SELECT
        *
    FROM db1.cameras_dir_asgard
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    SELECT
    *
    FROM db1.cameras_dir_asgard_ch
    LIMIT 10
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    TRUNCATE TABLE db1.cameras_dir_asgard_ch
    """

ch.query_run(query_text)

```

```python
query_text = """--sql
    DROP TABLE db1.cameras_dir_asgard_ch
    """

ch.query_run(query_text)
```

```python

```
