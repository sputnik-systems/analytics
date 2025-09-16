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
### Tags: #Source #ModifiedSources

### Links: 
___

```python
query_text = """--sql
CREATE TABLE db1.all_installetion_points_parquet
(
    `report_date` Date,
    `address_uuid` String,
    `city` String,
    `country` String,
    `region` String,
    `created_at` String,
    `full_address` String,
    `parent_uuid` String,
    `installation_point_id` String
)
ENGINE = S3(
    'https://storage.yandexcloud.net/aggregated-data/all_installetion_points_parquet/year=*/month=*/day=*/*.parquet',
    'Parquet'
)
"""

ch.query_run(query_text)
```

```python
    CAST(`report_date` AS DATE) AS `report_date`,
    CAST(`address_uuid` AS UTF8) AS `address_uuid`,
    CAST(`city` AS UTF8) AS `city`,
    CAST(`country` AS UTF8) AS `country`,
    CAST(`region` AS UTF8) AS `region`,
    CAST(`created_at` AS UTF8) AS `created_at`,
    CAST(`full_address` AS UTF8) AS `full_address`,
    CAST(`parent_uuid` AS UTF8) AS `parent_uuid`,
    CAST(`installation_point_id` AS UTF8) AS `installation_point_id`,
```

```python
query_text = """--sql
CREATE TABLE db1.all_installetion_points_parquet_ch
    (
    `report_date` Date,
    `address_uuid` String,
    `city` String,
    `country` String,
    `region` String,
    `created_at` String,
    `full_address` String,
    `parent_uuid` String,
    `installation_point_id` Int32
    )
    ENGINE = MergeTree()
    ORDER BY installation_point_id
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.all_installetion_points_parquet_mv
    REFRESH EVERY 1 DAY OFFSET 4 HOUR TO db1.all_installetion_points_parquet_ch AS
    SELECT
        *
    FROM db1.all_installetion_points_parquet
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
    FROM db1.all_installetion_points_parquet
    ORDER BY report_date desc
    limit 10
    """

ch.query_run(query_text)

```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.all_installetion_points_parquet_ch DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)

```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.all_installetion_points_parquet
    """

ch.query_run(query_text)
```


### drop ch

```python
query_text = """--sql
    DROP TABLE db1.all_installetion_points_parquet_ch
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.all_installetion_points_parquet_mv
"""

ch.query_run(query_text)
```
