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
### Tags: #Mobile_Report

### Links:  

[[rep_mobile_citizens_id_city_partner]]

[[subscriptions_st_mobile]]

[[companies_st_partner]]

### Table

```python
query_text = """--sql
CREATE TABLE db1.subscriptions_report_citizens_flats_comerce_rep_mobile_total
(
    `report_date` Date,
    `citizen_id` Int32,
    `city` String,
    `partner_uuid` String,
    `pro_subs` Int16,
    `state` String,
    `created_at` String,
    `subscribed_from` String,
    `plan` String,
    `activated_at` String,
    `expires_date` String,
    `renew_stopped_at` String,
    `renew_failed_at` String,
    `started_from` String
)
ENGINE = MergeTree()
ORDER BY citizen_id
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
        CREATE MATERIALIZED VIEW db1.subscriptions_report_citizens_flats_comerce_rep_mobile_total_mv
        REFRESH EVERY 1 DAY OFFSET 4 HOUR 15 MINUTE TO db1.subscriptions_report_citizens_flats_comerce_rep_mobile_total AS
        SELECT
            sub_st.report_date AS report_date,
            sub_st.citizen_id AS citizen_id,
            city,
            rep_t.partner_uuid AS partner_uuid,
            pro_subs,
            sub_st.`state` AS state,
            sub_st.`created_at` AS created_at,
            subscribed_from,
            plan,
            sub_st.`activated_at` AS activated_at,
            expires_date,
            renew_stopped_at,
            renew_failed_at,
            started_from
        FROM db1.`rep_mobile_citizens_id_city_partner` AS rep_t 
        JOIN db1.`subscriptions_st_mobile_ch` AS sub_st ON 
                sub_st.report_date = rep_t.report_date
                AND sub_st.citizen_id = rep_t.citizen_id
        LEFT JOIN db1.`companies_st_partner_ch` AS comp_st
                ON rep_t.`partner_uuid`= comp_st.`partner_uuid`
                AND  sub_st.`report_date` = comp_st.`report_date`

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
    FROM db1.subscriptions_report_citizens_flats_comerce_rep_mobile_total
    limit 2
    """

ch.query_run(query_text)
```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.subscriptions_report_comerce_rep_mobile_total DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)
```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.subscriptions_report_citizens_flats_comerce_rep_mobile_total_mv
    """

ch.query_run(query_text)
```

### drop ch

```python
query_text = """--sql
    DROP TABLE db1.subscriptions_report_citizens_flats_comerce_rep_mobile_total
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.subscriptions_report_comerce_rep_mobile_total_mv
"""

ch.query_run(query_text)
```
