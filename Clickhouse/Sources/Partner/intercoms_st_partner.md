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

<!-- #raw -->
___
### Tags: #Source #Partner

### Links: 
___
<!-- #endraw -->

```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.intercoms_st_partner
(
    `report_date` Date,
    `installation_date` DateTime,
    `service_partner_uuid` String,
    `partner_uuid` String,
    `intercom_uuid` String,
    `installation_point_id` Int64,
    `second_device_type` String,
    `second_device_state` Int16,
    `second_device_created_at` String,
    `model_identifier` String
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/intercoms_st_partner/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.intercoms_st_partner_ch
(
    `report_date` Date,
    `installation_date` DateTime,
    `service_partner_uuid` String,
    `partner_uuid` String,
    `intercom_uuid` String,
    `installation_point_id` Int64,
    `second_device_type` String,
    `second_device_state` Int16,
    `second_device_created_at` String,
    `model_identifier` String
)
    ENGINE = MergeTree()
    ORDER BY intercom_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.intercoms_st_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.intercoms_st_partner_ch AS
    SELECT
        *
    FROM db1.intercoms_st_partner
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
    FROM db1.intercoms_st_partner_ch
    ORDER BY report_date desc
    limit 100
    """

ch.query_run(query_text)

```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.intercoms_st_partner_ch DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)

```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.intercoms_st_partner_mv
    """

ch.query_run(query_text)
```


### drop ch

```python
query_text = """--sql
    DROP TABLE db1.intercoms_st_partner_ch
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.intercoms_st_partner_mv
"""

ch.query_run(query_text)
```
