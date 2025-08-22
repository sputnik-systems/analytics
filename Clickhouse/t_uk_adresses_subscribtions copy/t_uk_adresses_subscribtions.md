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
### Tags: #Tables

### Links: 

[[t_subscribtions_citizens_by_companies_and_cities_address_ch]]

[[uk_addresses_st_partner]]

[[uk_st_partner]]

[[uk_dir_partner]]

[[entries_installation_points_dir_partner]]
___

```python
query_text = """--sql
CREATE TABLE db1.t_uk_adresses_subscribtions
(
    `report_date` Date,
    `partner_uuid_uk` String,
    `name` String,
    `address_uuid` String,
    `partner_uuid` String,
    `partner_uk_email` String,
    `company_name` String,
    `partner_lk` UInt32,
    `activated_citizen_id` UInt32,
    `citizen_id_in_flat_with_subscriptions` UInt32,
    `subscribed_citizen_id` UInt32,
    `payments_amount` UInt32,
    `city` String,
    `full_address` String,
    `flats_count` UInt16,
    `entrance_number` String,
    `building_number` String,
    `streat_name` String 
)
    ENGINE = MergeTree()
    ORDER BY report_date
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.t_uk_adresses_subscribtions_mv
    REFRESH EVERY 1 DAY OFFSET 5 HOUR 35 MINUTE TO db1.t_uk_adresses_subscribtions AS
WITH d_range AS (
SELECT
	report_date
FROM db1.t_subscribtions_citizens_by_companies_and_cities_address_ch
),
uk_st AS(
SELECT
	`report_date`,
     uk_s_p.`partner_uuid` AS partner_uuid_uk,
    `business_partner_uuid` AS partner_uuid,
    `updated_at`,
    `partner_uk_email`,
    `name`
FROM db1.uk_st_partner_ch AS uk_s_p
LEFT JOIN db1.uk_dir_partner_ch AS uk_d_p ON uk_d_p.partner_uuid_uk = uk_s_p.partner_uuid
WHERE report_date IN d_range
)
--
SELECT
    uk_addr.`report_date` AS report_date,
    uk_addr.`partner_uuid_uk` AS partner_uuid_uk,
    `name`,
    uk_addr.`address_uuid` AS address_uuid,
    uk_st.partner_uuid AS partner_uuid,
    partner_uk_email,
    cdp.company_name AS company_name,
    cdp.partner_lk AS partner_lk,
    activated_citizen_id,
    citizen_id_in_flat_with_subscriptions,
    subscribed_citizen_id,
    payments_amount,
    t_sub.city AS city,
    t_sub.full_address AS full_address,
    flats_count,
    `entrance_number`,
    `building_number`,
    `streat_name`    
FROM db1.uk_addresses_st_partner_ch AS uk_addr
LEFT JOIN uk_st ON uk_st.report_date = uk_addr.report_date 
				AND uk_st.partner_uuid_uk = uk_addr.partner_uuid_uk 
LEFT JOIN db1.companies_dir_partner_ch AS cdp ON cdp.partner_uuid = uk_st.partner_uuid
LEFT JOIN db1.t_subscribtions_citizens_by_companies_and_cities_address_ch AS t_sub
	ON t_sub.address_uuid = uk_addr.address_uuid
	AND t_sub.report_date = uk_addr.report_date
LEFT JOIN db1.entries_installation_points_dir_partner_ch AS enr_d_p 
    ON enr_d_p.address_uuid = uk_addr.address_uuid 
WHERE uk_addr.report_date in d_range
ORDER BY report_date DESC
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
    FROM db1.t_uk_adresses_subscribtions
    ORDER BY report_date
    limit 10
    """

ch.query_run(query_text)

```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.t_uk_adresses_subscribtions DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)

```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.t_uk_adresses_subscribtions_mv
    """

ch.query_run(query_text)
```


### drop ch

```python
query_text = """--sql
    DROP TABLE db1.t_uk_adresses_subscribtions
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.t_uk_adresses_subscribtions_mv
"""

ch.query_run(query_text)
```
