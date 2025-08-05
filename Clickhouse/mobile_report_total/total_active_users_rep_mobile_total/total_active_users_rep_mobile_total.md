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

[[total_active_users_per_day_full_table]]

[[entries_st_mobile]]

[[citizens_dir_mobile]]


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
    WITH full_table AS(
    SELECT
        total_act.report_date AS report_date,
        total_act.partner_uuid AS partner_uuid,
        total_act.citizen_id AS citizen_id,
        total_act.monetization AS monetization,
        total_act.subscriptions_state AS subscriptions_state,
        entr_st_m.ble_available AS ble_available,
        cit_dir_m.activated_at AS activated_at,
        total_act.city AS city
    FROM db1.total_active_users_per_day_full_table AS total_act
    LEFT ANY JOIN db1.`entries_st_mobile_ch` AS entr_st_m
        ON `entr_st_m`.`report_date` = total_act.`report_date` 
        AND`entr_st_m`.`address_uuid` = total_act.`address_uuid`
    LEFT JOIN db1.`citizens_dir_mobile_ch` AS cit_dir_m
                ON `cit_dir_m`.`citizen_id` = total_act.`citizen_id` 
    )
    SELECT
        report_date,
        partner_uuid,
        city,
        count(DISTINCT citizen_id) AS total_active_users,
        count(DISTINCT if(toDateOrNull(activated_at) BETWEEN toStartOfMonth(report_date) and report_date,citizen_id,null)) new_active_users,
        count(DISTINCT if(monetization = 1,citizen_id,null)) as total_active_users_monetization,
        count(DISTINCT if(monetization = 1 and subscriptions_state = 'activated',citizen_id,null)) as total_active_user_subscribed_monetization,
        count(DISTINCT if(ble_available = 'true',citizen_id,null)) AS total_active_users_ble_available,
        count(DISTINCT if(ble_available = 'true' and monetization = 1,citizen_id,null)) AS total_active_users_ble_available_monetization,
        count(DISTINCT if(ble_available = 'true' and monetization = 1 and subscriptions_state = 'activated',citizen_id,null)) AS total_active_users_ble_available_subscribed_monetization
    FROM full_table
    GROUP BY
        report_date,
        partner_uuid,
        city
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
    FROM db1.total_active_users_rep_mobile_total
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
