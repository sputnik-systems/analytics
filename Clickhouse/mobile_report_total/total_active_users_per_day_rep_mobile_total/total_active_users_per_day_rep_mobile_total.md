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
    display_name: Python 3 (ipykernel)
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
### Tags: #Mobile_Report

### Links:  

[[total_active_users_per_day_full_table]]

### Table

```python
query_text = """--sql
CREATE TABLE db1.total_active_users_per_day_rep_mobile_total
(
    `report_date` Date,
    `partner_uuid` String,
    `city` String,
    `total_active_users_per_day` Int32,
    `total_active_user_monetization_per_day` Int32,
    `total_active_user_subscribed_monetization_per_day` Int32
)
ENGINE = MergeTree()
ORDER BY partner_uuid
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.total_active_users_per_day_rep_mobile_total_mv
    REFRESH EVERY 1 DAY OFFSET 6 HOUR 5 MINUTE TO db1.total_active_users_per_day_rep_mobile_total AS
    SELECT
        report_date,
        partner_uuid,
        city,
        round(avg(citizen_id_count) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
            ORDER BY report_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS total_active_users_per_day,
        round(avg(total_active_user_monetization) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
            ORDER BY report_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS total_active_user_monetization_per_day,
        round(avg(total_active_user_subscribed_monetization) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
            ORDER BY report_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS total_active_user_subscribed_monetization_per_day
    FROM
        (SELECT
            report_date,
            partner_uuid,
            city,
            count(citizen_id) as citizen_id_count,
            count(if(monetization = 1,citizen_id,null)) as total_active_user_monetization,
            count(if(monetization = 1 and subscriptions_state = 'activated',citizen_id,null)) as total_active_user_subscribed_monetization
        FROM db1.total_active_users_per_day_full_table
        GROUP BY report_date,
                partner_uuid,
                city
        )
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
    FROM db1.total_active_users_per_day_rep_mobile_total
    WHERE city !=''
    ORDER BY report_date DESC
    limit 100
    """

ch.query_run(query_text)
```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.total_activated_account_rep_mobile_full DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)
```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.total_activated_account_rep_mobile_full_mv
    """

ch.query_run(query_text)
```

### drop ch

```python
query_text = """--sql
    DROP TABLE db1.total_activated_account_rep_mobile_full
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.total_activated_account_rep_mobile_full_mv
"""

ch.query_run(query_text)
```
