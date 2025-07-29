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
### Tags: #Source #Partner

### Links: 
___

```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.cameras_dir_partner
(
    `serial_number` String,
    `service_partner_uuid` String,
    `partner_uuid` String,
    `intercom_uuid` String,
    `installation_point_id` Int64,
    `camera_uuid` String,
    `foreign_camera` Int16
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/cameras_dir_partner/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.cameras_dir_partner_ch
(
    `serial_number` String,
    `service_partner_uuid` String,
    `partner_uuid` String,
    `intercom_uuid` String,
    `installation_point_id` Int64,
    `camera_uuid` String,
    `foreign_camera` Int16
)
    ENGINE = MergeTree()
    ORDER BY partner_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.cameras_dir_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.cameras_dir_partner_ch AS
    SELECT
        *
    FROM db1.cameras_dir_partner
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
    FROM db1.cameras_dir_partner_ch
    limit 100
    """

ch.query_run(query_text)

```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.cameras_dir_partner_mv
    """

ch.query_run(query_text)
```


### drop ch

```python
query_text = """--sql
    DROP TABLE db1.cameras_dir_partner_ch
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.cameras_dir_partner_mv
"""

ch.query_run(query_text)
```
