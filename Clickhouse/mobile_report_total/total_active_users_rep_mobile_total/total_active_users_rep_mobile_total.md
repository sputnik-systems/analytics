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

<!-- #region -->
___
### Tags: #Mobile_Report

### Links:  

[[total_active_users_first_mention]]


### Table
<!-- #endregion -->

```python
query_text = """--sql
CREATE TABLE db1.total_active_users_rep_mobile_total
(
    `report_date` Date,
    `partner_uuid` String,
    `city` String,
    `total_active_users` UInt32,
    `new_active_users` UInt32,
    `total_active_users_monetization` UInt32,
    `total_active_user_subscribed_monetization` UInt32,
    `total_active_users_ble_available` UInt32,
    `total_active_users_ble_available_monetization` UInt32,
    `total_active_users_ble_available_subscribed_monetization` UInt32
)
ENGINE = MergeTree()
ORDER BY partner_uuid
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.total_active_users_rep_mobile_total_mv
    REFRESH EVERY 1 DAY OFFSET 6 HOUR 8 MINUTE TO db1.total_active_users_rep_mobile_total AS
    WITH dec_t AS (SELECT
            DISTINCT
            t1.report_date  AS report_date,
            t2.city  AS city,
            t2.partner_uuid  AS partner_uuid
        FROM
            (SELECT DISTINCT report_date FROM db1.total_active_users_first_mention) AS t1
        CROSS JOIN
            (SELECT DISTINCT city, partner_uuid FROM db1.total_active_users_first_mention ) AS t2
        ),
        --
        dec_and_full AS (
        SELECT
        dec_t.report_date AS report_date,
        dec_t.partner_uuid AS partner_uuid,
        dec_t.city AS city,
        total_active_users_day,
        new_active_users_day,
        total_active_users_monetization_day,
        total_active_user_subscribed_monetization_day,
        total_active_users_ble_available_day,
        total_active_users_ble_available_monetization_day,
        total_active_users_ble_available_subscribed_monetization_day
        FROM dec_t
        LEFT join (
            SELECT
                report_date,
                partner_uuid,
                city,
                count(DISTINCT citizen_id) AS total_active_users_day,
                count(DISTINCT if(toDateOrNull(activated_at) BETWEEN toStartOfMonth(report_date) and report_date,citizen_id,null)) AS new_active_users_day,
                count(DISTINCT if(monetization = 1,citizen_id,null)) AS total_active_users_monetization_day,
                count(DISTINCT if(monetization = 1 and subscriptions_state = 'activated',citizen_id,null)) as total_active_user_subscribed_monetization_day,
                count(DISTINCT if(ble_available = 'true',citizen_id,null)) AS total_active_users_ble_available_day,
                count(DISTINCT if(ble_available = 'true' and monetization = 1,citizen_id,null)) AS total_active_users_ble_available_monetization_day,
                count(DISTINCT if(ble_available = 'true' and monetization = 1 and subscriptions_state = 'activated',citizen_id,null)) AS total_active_users_ble_available_subscribed_monetization_day
            FROM db1.total_active_users_first_mention
            GROUP BY
                report_date,
                partner_uuid,
                city) AS full_table
            ON dec_t.report_date = full_table.report_date
            AND dec_t.partner_uuid = full_table.partner_uuid
            AND dec_t.city = full_table.city
        )
        --
        SELECT
            report_date,
            partner_uuid,
            city,
            sum(total_active_users_day) OVER (PARTITION BY toStartOfMonth(report_date),partner_uuid, city ORDER BY report_date) AS total_active_users,
            sum(new_active_users_day) OVER (PARTITION BY toStartOfMonth(report_date),partner_uuid, city ORDER BY report_date) AS new_active_users,
            sum(total_active_users_monetization_day) OVER (PARTITION BY toStartOfMonth(report_date),partner_uuid, city ORDER BY report_date) AS total_active_users_monetization,
            sum(total_active_user_subscribed_monetization_day) OVER (PARTITION BY toStartOfMonth(report_date),partner_uuid, city ORDER BY report_date) AS total_active_user_subscribed_monetization,
            sum(total_active_users_ble_available_day) OVER (PARTITION BY toStartOfMonth(report_date),partner_uuid, city ORDER BY report_date) AS total_active_users_ble_available,
            sum(total_active_users_ble_available_monetization_day) OVER (PARTITION BY toStartOfMonth(report_date),partner_uuid, city ORDER BY report_date)
                AS total_active_users_ble_available_monetization,
            sum(total_active_users_ble_available_subscribed_monetization_day) OVER (PARTITION BY toStartOfMonth(report_date),partner_uuid, city ORDER BY report_date)
                AS total_active_users_ble_available_subscribed_monetization
        FROM dec_and_full
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
        report_date,
        sum(new_active_users)
    FROM db1.total_active_users_rep_mobile_total
    GROUP BY report_date
    ORDER BY report_date DESC
    limit 100
    """

ch.query_run(query_text)
```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.total_active_users_per_day_full_table DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)
```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.total_active_users_rep_mobile_total_mv
    """

ch.query_run(query_text)
```

### drop ch

```python
query_text = """--sql
    DROP TABLE db1.total_active_users_rep_mobile_total
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.total_active_users_per_day_full_table_mv
"""

ch.query_run(query_text)
```
