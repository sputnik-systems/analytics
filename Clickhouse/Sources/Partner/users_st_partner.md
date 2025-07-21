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
query_text = """--sql
    CREATE TABLE db1.users_st_partner
    (
        `report_date` Date,
        `partner_uuid`  String,
        `last_logging` DateTime
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/users_st_partner/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.users_st_partner_ch
    (
        `report_date` Date,
        `partner_uuid`  String,
        `last_logging` DateTime
    )
    ENGINE = MergeTree()
    ORDER BY partner_uuid
    """

ch.query_run(query_text)

```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.users_st_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.users_st_partner_ch AS
    SELECT
        *
    FROM db1.users_st_partner
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
    FROM db1.users_st_partner_ch
    limit 10
    """

ch.query_run(query_text)
```

### Drop ch

```python
query_text = """
    DROP TABLE db1.users_st_partner_ch
    """
ch.query_run(query_text)
```

### Drop mv

```python
query_text = """
    DROP TABLE db1.users_st_partner_mv
    """
ch.query_run(query_text)
```
