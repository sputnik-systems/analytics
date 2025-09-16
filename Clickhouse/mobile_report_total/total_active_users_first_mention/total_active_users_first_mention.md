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
    display_name: Python (myenv)
    language: python
    name: myenv
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
    SELECT
        *
    FROM
        (SELECT
            total_act.report_date AS report_date,
            total_act.partner_uuid AS partner_uuid,
            total_act.citizen_id AS citizen_id,
            total_act.monetization AS monetization,
            total_act.subscriptions_state AS subscriptions_state,
            entr_st_m.ble_available AS ble_available,
            cit_dir_m.activated_at AS activated_at,
            total_act.city AS city,
            ROW_NUMBER() OVER (
                    PARTITION BY
                        toStartOfMonth(total_act.report_date),
                        total_act.citizen_id
                    ORDER BY total_act.report_date
                ) = 1 AS is_first_mention_in_month
        FROM db1.total_active_users_per_day_full_table AS total_act
        LEFT JOIN db1.`entries_st_mobile_ch` AS entr_st_m
            ON `entr_st_m`.`report_date` = total_act.`report_date`
            AND`entr_st_m`.`address_uuid` = total_act.`address_uuid`
        LEFT JOIN db1.`citizens_dir_mobile_ch` AS cit_dir_m
                    ON `cit_dir_m`.`citizen_id` = total_act.`citizen_id`
        WHERE report_date = '2025-08-13'
            )
    WHERE is_first_mention_in_month = 1
    limit 10
"""

ch.get_schema(query_text)
```

```python
query_text = """--sql
CREATE TABLE db1.total_active_users_first_mention
(
    `report_date` Date,
    `partner_uuid` String,
    `citizen_id` Int32,
    `monetization` UInt8,
    `subscriptions_state` String,
    `ble_available` String,
    `activated_at` String,
    `city` String,
    `is_first_mention_in_month` UInt8
)
ENGINE = MergeTree()
ORDER BY partner_uuid
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.total_active_users_first_mention_mv
    REFRESH EVERY 1 DAY OFFSET 4 HOUR 20 MINUTE TO db1.total_active_users_first_mention AS
    WITH first_mantion AS (
SELECT
	report_date,
	citizen_id,
	ROW_NUMBER() OVER (
	                    PARTITION BY
	                        toStartOfMonth(report_date),
	                       citizen_id
	                    ORDER BY report_date
	                ) = 1 AS is_first_mention_in_month
FROM db1.total_active_users_per_day_full_table
),
--
total_act AS (
SELECT
        total_act.report_date AS report_date,
        total_act.partner_uuid AS partner_uuid,
        total_act.citizen_id AS citizen_id,
        total_act.monetization AS monetization,
        total_act.subscriptions_state AS subscriptions_state,
        total_act.address_uuid AS address_uuid,
        total_act.city AS city
FROM db1.total_active_users_per_day_full_table AS total_act
JOIN first_mantion ON total_act.report_date = first_mantion.report_date
					AND total_act.citizen_id = first_mantion.citizen_id
WHERE first_mantion.is_first_mention_in_month = 1
)
--
SELECT
    total_act.report_date AS report_date,
    total_act.partner_uuid AS partner_uuid,
    total_act.citizen_id AS citizen_id,
    total_act.monetization AS monetization,
    total_act.subscriptions_state AS subscriptions_state,
    total_act.city AS city,
    entr_st_m.ble_available AS ble_available,
    cit_dir_m.activated_at AS activated_at
FROM total_act
LEFT JOIN db1.`entries_st_mobile_ch` AS entr_st_m
        ON `entr_st_m`.`report_date` = total_act.`report_date`
        AND`entr_st_m`.`address_uuid` = total_act.`address_uuid`
LEFT JOIN db1.`citizens_dir_mobile_ch` AS cit_dir_m
                ON `cit_dir_m`.`citizen_id` = total_act.`citizen_id`
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
    FROM db1.total_active_users_first_mention
    ORDER BY report_date DESC
    limit 100
    """

ch.query_run(query_text)
```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.total_active_users_first_mention DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)
```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.total_active_users_first_mention_mv
    """

ch.query_run(query_text)
```

### drop ch

```python
query_text = """--sql
    DROP TABLE db1.total_active_users_first_mention
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.total_active_users_first_mention_mv
"""

ch.query_run(query_text)
```
