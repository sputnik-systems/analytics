---
jupyter:
  jupytext:
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
### Tags: #Source #Bitrix

### Links: 
___

```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.category_id_dir_bitrix
(
    `category_id` UInt32,
    `category_name` String,
    `stage_id` String,
    `name` String
)
ENGINE = S3('https://storage.yandexcloud.net/aggregated-data/bitrix_category_id/*.csv','CSVWithNames')
SETTINGS format_csv_delimiter = ';'
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.category_id_dir_bitrix_ch
(
    `category_id` UInt32,
    `category_name` String,
    `stage_id` String,
    `name` String
)
    ENGINE = MergeTree()
    ORDER BY category_id
    
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.category_id_dir_bitrix_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.category_id_dir_bitrix_ch AS
    SELECT
        category_id,
        category_name,
        if(category_id != 0, splitByChar(':', stage_id)[2],stage_id) AS stage_id,
        name
    FROM db1.category_id_dir_bitrix
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
    FROM db1.category_id_dir_bitrix_ch
    limit 100
    """

ch.query_run(query_text)

```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.category_id_dir_bitrix_mv
    """

ch.query_run(query_text)
```


### drop ch

```python
query_text = """--sql
    DROP TABLE db1.cameras_dir_asgard_ch
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.cameras_dir_asgard_mv
"""

ch.query_run(query_text)
```
