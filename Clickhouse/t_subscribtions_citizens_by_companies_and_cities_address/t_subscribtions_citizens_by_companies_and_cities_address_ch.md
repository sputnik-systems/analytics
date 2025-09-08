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
## Tags: #Tables #YandexFunctions

# Links:

[[citizens_st_mobile]]

[[subscriptions_st_mobile]]

[[entries_installation_points_dir_partner]]

[[installation_point_st_partner]]

[[citizen_payments_st_mobile]]

[[intercoms_st_partner]]

[[clichouse_schedule_function]]


_____

## t_full_citizen_id_in_flat_with_subscriptions

```python
query_text = """--sql
    CREATE TABLE db1.t_full_citizen_id_in_flat_with_subscriptions
    (
        `report_date` Date,
        `address_uuid` String,
        `citizen_id` UInt64
    )
    ENGINE = MergeTree()
    ORDER BY address_uuid
    """
ch.query_run(query_text)
```

```python
query_text = """--sql
CREATE MATERIALIZED VIEW db1.t_full_citizen_id_in_flat_with_subscriptions_mv
REFRESH EVERY 1 DAY OFFSET 4 HOUR 25 MINUTE TO db1.t_full_citizen_id_in_flat_with_subscriptions AS 
WITH t1 AS (SELECT 
	rmcicp.report_date AS report_date,
	flat_uuid
FROM db1.rep_mobile_citizens_id_city_partner AS rmcicp
JOIN db1.subscriptions_st_mobile_ch AS ssm 
	ON rmcicp.report_date = ssm.report_date 
	AND rmcicp.citizen_id = ssm.citizen_id
WHERE  ssm.state = 'activated'
)
SELECT
	t1.report_date AS report_date,
	rmcicp.address_uuid AS address_uuid,
	rmcicp.citizen_id AS citizen_id
FROM db1.rep_mobile_citizens_id_city_partner AS rmcicp
JOIN t1 AS ssm 
	ON rmcicp.report_date = ssm.report_date 
	AND rmcicp.flat_uuid = ssm.flat_uuid
WHERE t1.flat_uuid !=''
    """
ch.query_run(query_text)
```

```python
query_text = """
    DROP TABLE db1.t_full_citizen_id_in_flat_with_subscriptions
    """
ch.query_run(query_text)
```

```python
query_text = """
    DROP TABLE db1.t_full_citizen_id_in_flat_with_subscriptions_mv
    """
ch.query_run(query_text)

```

```python
query_text = """
    SELECT
        *
    FROM db1.t_full_citizen_id_in_flat_with_subscriptions
    order by report_date desc
    limit 10
    """
ch.query_run(query_text)
```

```python
query_text = """
    SELECT
    report_date,
    count(*)
    FROM db1.t_full_citizen_id_in_flat_with_subscriptions_mv
    group by report_date
    order by report_date desc
    """
ch.query_run(query_text)
```

```python
query_text = """
SYSTEM REFRESH VIEW db1.t_full_citizen_id_in_flat_with_subscriptions_mv
"""

ch.query_run(query_text)
```

_____

## t_citizen_id_in_flat_with_subscriptions

```python
query_text = """--sql
    CREATE TABLE db1.t_citizen_id_in_flat_with_subscriptions
    (
        `report_date` Date,
        `address_uuid` String,
        `citizen_id_in_flat_with_subscriptions` UInt64
    )
    ENGINE = MergeTree()
    ORDER BY address_uuid
    """
ch.query_run(query_text)

```

```python
query_text = """--sql
CREATE MATERIALIZED VIEW db1.t_citizen_id_in_flat_with_subscriptions_mv
REFRESH EVERY 1 DAY OFFSET 5 HOUR 27 MINUTE TO db1.t_citizen_id_in_flat_with_subscriptions AS 
SELECT
    report_date,
    address_uuid,
    COUNT(DISTINCT citizen_id) as citizen_id_in_flat_with_subscriptions
FROM db1.t_full_citizen_id_in_flat_with_subscriptions
WHERE address_uuid !=''
GROUP BY report_date,
        address_uuid
    """
ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.t_citizen_id_in_flat_with_subscriptions
ORDER BY report_date desc
LIMIT 10
    """
ch.query_run(query_text)
```

```python
query_text = """
    DROP TABLE db1.t_citizen_id_in_flat_with_subscriptions_mv
    """
ch.query_run(query_text)
```

```python
query_text = """
    DROP TABLE db1.t_citizen_id_in_flat_with_subscriptions
    """
ch.query_run(query_text)
```

____

## t_payments_amount

```python
query_text = """--sql
    CREATE TABLE db1.t_payments_amount
    (
        `report_date` Date,
        `address_uuid` String,
        `payments_amount` UInt64
    )
    ENGINE = MergeTree()
    ORDER BY address_uuid
    """
ch.query_run(query_text)
```

```python
query_text = """--sql
CREATE MATERIALIZED VIEW db1.t_payments_amount_mv
REFRESH EVERY 1 DAY OFFSET 5 HOUR 30 MINUTE TO db1.t_payments_amount AS 
SELECT
    report_date,
    address_uuid,
    sum(amount) AS payments_amount
FROM db1.rep_mobile_citizens_id_city_partner AS t_cit_id
JOIN db1.citizen_payments_st_mobile_ch AS citizen_payments_st_mobile
    ON citizen_payments_st_mobile.report_date = t_cit_id.report_date
    AND citizen_payments_st_mobile.citizen_id = t_cit_id.citizen_id
WHERE address_uuid != ''
GROUP BY report_date, address_uuid
    """
ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.t_payments_amount
order by report_date desc
LIMIT 10
    """
ch.query_run(query_text)
```

```python
query_text = """
    DROP TABLE db1.t_payments_amount
    """
ch.query_run(query_text)
```

```python
query_text = """
    DROP TABLE db1.t_payments_amount_mv
    """
ch.query_run(query_text)
```

____

## t_subscribed_citizen_id

```python
query_text = """--sql
    CREATE TABLE db1.t_subscribed_citizen_id
    (
        `report_date` Date,
        `address_uuid` String,
        `subscribed_citizen_id` UInt64
    )
    ENGINE = MergeTree()
    ORDER BY address_uuid
    """
ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.t_subscribed_citizen_id_mv
    REFRESH EVERY 1 DAY OFFSET 5 HOUR 33 MINUTE TO db1.t_subscribed_citizen_id AS 
    SELECT
        report_date,
        address_uuid,
        countDistinctIf(citizen_id, citizen_id != 0) AS subscribed_citizen_id
    FROM db1.subscriptions_report_citizens_flats_comerce_rep_mobile_total
    WHERE state = 'activated' AND address_uuid != ''
    GROUP BY report_date, address_uuid
    """
ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.t_subscribed_citizen_id
order by report_date desc
LIMIT 10
    """
ch.query_run(query_text)
```

```python
query_text = """
    DROP TABLE db1.t_subscribed_citizen_id
    """
ch.query_run(query_text)
```

```python
query_text = """
    DROP TABLE db1.t_subscribed_citizen_id_mv
    """
ch.query_run(query_text)
```

____

## t_activated_citizen_id

```python
query_text = """--sql
    CREATE TABLE db1.t_activated_citizen_id
    (
        `report_date` Date,
        `address_uuid` String,
        `activated_citizen_id` UInt64,
        `flat_uuid` UInt64
    )
    ENGINE = MergeTree()
    ORDER BY address_uuid
    """
ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.t_activated_citizen_id_mv
    REFRESH EVERY 1 DAY OFFSET 5 HOUR 35 MINUTE TO db1.t_activated_citizen_id AS 
     SELECT
        report_date,
        address_uuid,
        countDistinctIf(citizen_id, citizen_id != 0) AS activated_citizen_id,	
        countDistinctIf(flat_uuid, flat_uuid != '') AS flat_uuid
    FROM db1.rep_mobile_citizens_id_city_partner
    WHERE address_uuid != ''
    GROUP BY report_date, address_uuid
    """
ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.t_activated_citizen_id
ORDER BY report_date DESC
LIMIT 10
    """
ch.query_run(query_text)
```

```python
query_text = """
    DROP TABLE db1.t_activated_citizen_id
    """
ch.query_run(query_text)
```

```python
query_text = """
    DROP TABLE db1.t_activated_citizen_id_mv
    """
ch.query_run(query_text)
```

____

```python jupyter={"source_hidden": true}
import pandas as pd

query_text = """
INSERT INTO db1.t_subscribtions_citizens_by_companies_and_cities_address_ch
WITH t_citizen_id_in_flat_with_subscriptions AS(
    SELECT
        report_date,
        address_uuid,
        countIf(citizen_id != 0) AS citizen_id_in_flat_with_subscriptions
    FROM (
        SELECT 
            report_date,
            citizen_id,
            address_uuid,
            IF(ssm.state = 'activated', 1, 0) AS if_sub_active,
            max(if_sub_active) OVER (PARTITION BY flat_uuid, report_date ORDER BY report_date DESC) AS flat_with_sub_active
        FROM db1.rep_mobile_citizens_id_city_partner AS rmcicp
        LEFT JOIN db1.subscriptions_st_mobile_ch AS ssm 
            ON rmcicp.report_date = ssm.report_date 
            AND rmcicp.citizen_id = ssm.citizen_id
        WHERE report_date = toDate('{d}')
    ) AS x
    WHERE flat_with_sub_active = 1 AND address_uuid != ''
    GROUP BY report_date, address_uuid
),
t_payments_amount AS(
    SELECT
        report_date,
        address_uuid,
        sum(amount) AS payments_amount
    FROM db1.rep_mobile_citizens_id_city_partner AS t_cit_id
    JOIN db1.citizen_payments_st_mobile_ch AS citizen_payments_st_mobile
        ON citizen_payments_st_mobile.report_date = t_cit_id.report_date
        AND citizen_payments_st_mobile.citizen_id = t_cit_id.citizen_id
    WHERE report_date = toDate('{d}') AND address_uuid != ''
    GROUP BY report_date, address_uuid
),
t_subscribed_citizen_id AS(
    SELECT
        report_date,
        address_uuid,
        countDistinctIf(citizen_id, citizen_id != 0) AS subscribed_citizen_id
    FROM db1.subscriptions_report_citizens_flats_comerce_rep_mobile_total
    WHERE state = 'activated' AND address_uuid != '' AND report_date = toDate('{d}')
    GROUP BY report_date, address_uuid
),
t_activated_citizen_id AS (
    SELECT
        report_date,
        address_uuid,
        countDistinctIf(citizen_id, citizen_id != 0) AS activated_citizen_id,	
        countDistinctIf(flat_uuid, flat_uuid != '') AS flat_uuid
    FROM db1.rep_mobile_citizens_id_city_partner
    WHERE report_date = toDate('{d}') AND address_uuid != ''
    GROUP BY report_date, address_uuid
),
t1 AS (
    SELECT
        report_date,
        city,
        full_address,
        intercoms.partner_uuid AS partner_uuid,
        intercoms.installation_point_id AS installation_point_id,
        motherboard_ids,
        address_uuid,
        company_name, 
        partner_lk,
        tin
    FROM (
        SELECT
            report_date,
            installation_point_id,
            partner_uuid,
            arrayStringConcat(groupArray(motherboard_id), ',') AS motherboard_ids
        FROM db1.intercoms_st_partner_ch
        LEFT JOIN db1.intercoms_dir_asgard_ch 
            ON intercoms_st_partner_ch.intercom_uuid = intercoms_dir_asgard_ch.intercom_uuid
        WHERE report_date = toDate('{d}') AND installation_point_id != 0
        GROUP BY report_date, installation_point_id, partner_uuid
    ) AS intercoms
    LEFT JOIN db1.entries_installation_points_dir_partner_ch AS eipdp  
        ON intercoms.installation_point_id = eipdp.installation_point_id
    LEFT JOIN db1.companies_dir_partner AS cdp
        ON intercoms.partner_uuid = cdp.partner_uuid
)
SELECT
    t1.report_date AS report_date,
    city,
    full_address,
    partner_uuid,
    installation_point_id,
    motherboard_ids,
    t1.address_uuid AS address_uuid,
    company_name, 
    partner_lk,
    tin,
    activated_citizen_id,
    flat_uuid,
    subscribed_citizen_id,
    payments_amount,
    citizen_id_in_flat_with_subscriptions
FROM t1
LEFT JOIN t_citizen_id_in_flat_with_subscriptions 
    ON t1.report_date = t_citizen_id_in_flat_with_subscriptions.report_date 
    AND t1.address_uuid = t_citizen_id_in_flat_with_subscriptions.address_uuid
LEFT JOIN t_payments_amount 
    ON t1.report_date = t_payments_amount.report_date 
    AND t1.address_uuid = t_payments_amount.address_uuid
LEFT JOIN t_subscribed_citizen_id
    ON t1.report_date = t_subscribed_citizen_id.report_date 
    AND t1.address_uuid = t_subscribed_citizen_id.address_uuid
LEFT JOIN t_activated_citizen_id
    ON t1.report_date = t_activated_citizen_id.report_date 
    AND t1.address_uuid = t_activated_citizen_id.address_uuid
"""

start = '2023-10-01'
finish = '2025-08-19'
df = pd.date_range(start, finish)[::-1]

for ts in df:
    d = ts.strftime('%Y-%m-%d')
    ch.query_run(query_text.format(d=d))

```

```python
query_text = """--sql
    CREATE TABLE db1.t_subscribtions_citizens_by_companies_and_cities_address_ch 
    (
        `report_date` Date,
        `city` String,
        `full_address` String,
        `partner_uuid` String,
        `installation_point_id` Int64,
        `motherboard_ids` String,
        `address_uuid` String,
        `company_name` String,
        `partner_lk` String,
        `tin` String,
        `activated_citizen_id` UInt64,
        `flat_uuid` UInt64,
        `subscribed_citizen_id` UInt64,
        `payments_amount` Int64,
        `citizen_id_in_flat_with_subscriptions` UInt64
    )
    ENGINE = MergeTree()
    ORDER BY report_date
    """
ch.query_run(query_text)

```

___
## t_subscribtions_citizens_by_companies_and_cities_address_mv

```python
query_text = """--sql
CREATE MATERIALIZED VIEW db1.t_subscribtions_citizens_by_companies_and_cities_address_mv
REFRESH EVERY 1 DAY OFFSET 5 HOUR 45 MINUTE TO db1.t_subscribtions_citizens_by_companies_and_cities_address_ch AS 
WITH t1 AS (
    SELECT
        report_date,
        city,
        full_address,
        intercoms.partner_uuid AS partner_uuid,
        intercoms.installation_point_id AS installation_point_id,
        motherboard_ids,
        address_uuid,
        company_name, 
        partner_lk,
        tin
    FROM (
        SELECT
            report_date,
            installation_point_id,
            partner_uuid,
            arrayStringConcat(groupArray(motherboard_id), ',') AS motherboard_ids
        FROM db1.intercoms_st_partner_ch
        LEFT JOIN db1.intercoms_dir_asgard_ch 
            ON intercoms_st_partner_ch.intercom_uuid = intercoms_dir_asgard_ch.intercom_uuid
        WHERE installation_point_id != 0
        GROUP BY report_date, installation_point_id, partner_uuid
    ) AS intercoms
    LEFT JOIN db1.entries_installation_points_dir_partner_ch AS eipdp  
        ON intercoms.installation_point_id = eipdp.installation_point_id
    LEFT JOIN db1.companies_dir_partner AS cdp
        ON intercoms.partner_uuid = cdp.partner_uuid
)
SELECT
    t1.report_date AS report_date,
    city,
    full_address,
    partner_uuid,
    installation_point_id,
    motherboard_ids,
    t1.address_uuid AS address_uuid,
    company_name, 
    partner_lk,
    tin,
    activated_citizen_id,
    flat_uuid,
    subscribed_citizen_id,
    payments_amount,
    citizen_id_in_flat_with_subscriptions
FROM t1
LEFT JOIN db1.t_citizen_id_in_flat_with_subscriptions 
    ON t1.report_date = t_citizen_id_in_flat_with_subscriptions.report_date 
    AND t1.address_uuid = t_citizen_id_in_flat_with_subscriptions.address_uuid
LEFT JOIN db1.t_payments_amount 
    ON t1.report_date = t_payments_amount.report_date 
    AND t1.address_uuid = t_payments_amount.address_uuid
LEFT JOIN db1.t_subscribed_citizen_id
    ON t1.report_date = t_subscribed_citizen_id.report_date 
    AND t1.address_uuid = t_subscribed_citizen_id.address_uuid
LEFT JOIN db1.t_activated_citizen_id
    ON t1.report_date = t_activated_citizen_id.report_date 
    AND t1.address_uuid = t_activated_citizen_id.address_uuid
	"""
ch.query_run(query_text)
```

```python
query_text = """
    SELECT
        report_date,
        count(*)
    FROM db1.t_subscribtions_citizens_by_companies_and_cities_address_ch
    group by report_date
    order by
    report_date desc
    LIMIT 10

    """
ch.query_run(query_text)
```

```python
query_text = """
SYSTEM REFRESH VIEW db1.t_subscribtions_citizens_by_companies_and_cities_address_mv
"""

ch.query_run(query_text)
```

```python
query_text = """
    DROP TABLE db1.t_subscribtions_citizens_by_companies_and_cities_address_ch
    """
ch.query_run(query_text)
```

```python
query_text = """
    DROP TABLE db1.t_subscribtions_citizens_by_companies_and_cities_address_mv
    """
ch.query_run(query_text)
```

```python





```
