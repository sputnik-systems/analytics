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

[[citizens_dir_mobile]]

[[entries_st_mobile]]

### Table

```python
query_text = """--sql
CREATE TABLE db1.total_activated_account_rep_mobile_full
(
    `report_date` Date,
    `partner_uuid` String,
    `city` String,
    `total_activated_account` UInt64,
    `total_activated_account_monetization` UInt64,
    `total_activated_account_ble_available_monetization` UInt64,
    `total_activated_account_ble_available` UInt64
)
ENGINE = MergeTree()
ORDER BY partner_uuid
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.total_activated_account_rep_mobile_full_mv
    REFRESH EVERY 1 DAY OFFSET 6 HOUR 0 MINUTE TO db1.total_activated_account_rep_mobile_full AS
    SELECT
        rep_m.`report_date` AS report_date,
        rep_mobile_citizens_id_city_partner.partner_uuid AS partner_uuid,
        city,
        COUNT(DISTINCT rep_m.citizen_id) as total_activated_account,
        COUNT(DISTINCT IF(rep_m.monetization=1, rep_m.citizen_id, NULL)) as total_activated_account_monetization,
        COUNT(DISTINCT IF(rep_m.monetization=1 and ble_available = 'true', rep_m.citizen_id,NULL)) as total_activated_account_ble_available_monetization,
        COUNT(DISTINCT IF(ble_available = 'true', rep_m.citizen_id, NULL)) as total_activated_account_ble_available
    FROM db1.rep_mobile_citizens_id_city_partner AS rep_m
    LEFT JOIN db1.entries_st_mobile AS ent_st 
        ON ent_st.report_date = rep_m.report_date
        AND ent_st.address_uuid = rep_m.address_uuid
    LEFT JOIN db1.`citizens_dir_mobile` AS cit_dir 
        ON cit_dir.`citizen_id` = rep_m.`citizen_id`
    WHERE 
        toDateOrNull(cit_dir.`created_at`) < rep_m.`report_date`
        AND state = 'activated'
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
    FROM db1.total_activated_account_rep_mobile_full
    limit 2
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
