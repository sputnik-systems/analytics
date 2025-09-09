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
## Tags: #Tables

# Links:

[[t_full_citizen_id_in_flat_with_subscriptions]]

[[t_citizen_id_in_flat_with_subscriptions]]

[[t_payments_amount]]

[[t_subscribed_citizen_id]]

[[t_activated_citizen_id]]

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
    SELECT
        report_date,
        sum(activated_citizen_id)
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
