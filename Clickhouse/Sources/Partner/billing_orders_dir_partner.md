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
### Tags: #Source #Partner

### Links: 
___

```python
# creating a table from s3

query_text = """--sql 
    CREATE TABLE db1.billing_orders_dir_partner
    (
        `created_at` DateTime,
        `state` String,
        `service` String,
        `cost` Float64,
        `total` Float64,
        `count` Int64,
        `billing_account_id` String,
        `partner_uuid` String
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/billing_orders_dir_partner/billing_orders_dir_partner.csv','CSVWithNames')
    PARTITION BY report_date
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.billing_orders_dir_partner_ch
    (
        `created_at` DateTime,
        `state` String,
        `service` String,
        `cost` Float64,
        `total` Float64,
        `count` Int64,
        `billing_account_id` String,
        `partner_uuid` String
    )
    ENGINE = MergeTree()
    ORDER BY partner_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.billing_orders_dir_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.billing_orders_dir_partner_ch AS
    SELECT
        *
    FROM db1.billing_orders_dir_partner
    """

ch.query_run(query_text)
```

```python
query_text = """--sql

SELECT
    *
FROM db1.billing_orders_dir_partner_ch
order by created_at DESC
limit 100

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
    FROM db1.billing_orders_dir_partner_ch
    limit 100
    """

ch.query_run(query_text)

```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.billing_orders_dir_partner_mv
    """

ch.query_run(query_text)
```


### drop ch

```python
query_text = """--sql
    DROP TABLE db1.billing_orders_dir_partner_ch
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.billing_orders_dir_partner_mv
"""

ch.query_run(query_text)
```
