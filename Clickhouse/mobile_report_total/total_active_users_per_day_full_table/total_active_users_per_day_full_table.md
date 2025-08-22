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

[[rep_mobile_citizens_id_city_partner]]

[[sessions_st_mobile]]

[[subscriptions_st_mobile]]


### Table
<!-- #endregion -->

```python
query_text = """--sql
CREATE TABLE db1.total_active_users_per_day_full_table
(
    `report_date` Date,
    `partner_uuid` String,
    `city` String,
    `citizen_id` Int32,
    `monetization` UInt8,
    `subscriptions_state` String,
    `address_uuid` String,
    `flat_uuid` String,
    `created_at` String,
    `activated_at` DateTime
)
ENGINE = MergeTree()
ORDER BY partner_uuid
"""

ch.query_run(query_text)
```

```python

```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.total_active_users_per_day_full_table_mv
    REFRESH EVERY 1 DAY OFFSET 4 HOUR 10 MINUTE TO db1.total_active_users_per_day_full_table AS
       SELECT
        r_m_c.partner_uuid AS partner_uuid,
        ses_st_m.citizen_id AS citizen_id,
        r_m_c.report_date AS report_date,
        r_m_c.monetization AS monetization,
        sub_st_m.state AS subscriptions_state,
        r_m_c.city AS city,
        r_m_c.address_uuid AS address_uuid,
        r_m_c.flat_uuid AS flat_uuid,
        c_d_m.created_at AS created_at,
        r_m_c.activated_at AS activated_at
    FROM db1.rep_mobile_citizens_id_city_partner as r_m_c
    JOIN db1.`sessions_st_mobile_ch` AS ses_st_m
        ON r_m_c.citizen_id = ses_st_m.citizen_id
        AND r_m_c.report_date = ses_st_m.report_date 
    LEFT JOIN db1.`subscriptions_st_mobile_ch` AS sub_st_m
        ON ses_st_m.citizen_id = sub_st_m.citizen_id
        AND ses_st_m.report_date = sub_st_m.report_date
    LEFT JOIN db1.`citizens_dir_mobile_ch` AS c_d_m ON c_d_m.`citizen_id`  = r_m_c.`citizen_id`
    WHERE 
        toDate(`last_use`) 
        BETWEEN toStartOfMonth(r_m_c.report_date) and r_m_c.report_date 
        AND  r_m_c.state = 'activated'
    --
     SETTINGS join_algorithm = 'partial_merge'
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
    FROM db1.total_active_users_per_day_full_table
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
    DROP TABLE db1.total_active_users_per_day_full_table_mv
    """

ch.query_run(query_text)
```

### drop ch

```python
query_text = """--sql
    DROP TABLE db1.total_active_users_per_day_full_table
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
