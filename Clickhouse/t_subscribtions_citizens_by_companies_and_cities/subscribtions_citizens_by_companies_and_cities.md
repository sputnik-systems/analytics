---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.17.1
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
___
# Links:

[[citizens_st_mobile]]
[[subscriptions_st_mobile]]
[[citizens_dir_mobile]]
[[subscriptions_st_mobile]]
[[citizens_dir_mobile]]
[[citizen_payments_st_mobile]]
[[companies_dir_partner]]



___
# t_subscribtions_citizens_by_companies_and_cities_ch


# Links

[[citizens_st_mobile_parquet]]<br>
[[subscriptions_st_mobile]]<br>
[[entries_installation_points_dir_partner]]<br>
[[installation_point_st_partner]]<br>
[[citizen_payments_st_mobile]]<br>

```python
query_text = """--sql
    CREATE TABLE db1.t_subscribtions_citizens_by_companies_and_cities_ch 
    (
        `report_date` Date,
        `city` String,
        `partner_uuid` String,
        `company_name` String,
        `partner_lk` String,
        `citizen_id_in_flat_with_subscriptions` UInt32,
        `payments_amount` UInt32,
        `activated_citizen_id` UInt32,
        `subscribed_citizen_id` UInt32,
        `flat_uuid` UInt32
    )
    ENGINE = MergeTree()
    ORDER BY report_date
    """
ch.query_run(query_text)
```

```python
query_text = """--sql
CREATE MATERIALIZED VIEW db1.t_subscribtions_citizens_by_companies_and_cities_mv 
REFRESH EVERY 1 DAY OFFSET 5 HOUR 25 MINUTE TO db1.t_subscribtions_citizens_by_companies_and_cities_ch AS
        WITH t1 AS(SELECT
        citizens_st_mobile.report_date AS report_date,
        entries_installation_points.city AS city,
        installation_point_st.partner_uuid AS partner_uuid,
        COUNT(DISTINCT IF(citizens_st_mobile.`state` = 'activated', citizens_st_mobile.citizen_id,NULL)) as activated_citizen_id,
        COUNT(DISTINCT IF(subscriptions_st_mobile.`state` = 'activated', subscriptions_st_mobile.citizen_id,NULL)) as subscribed_citizen_id,
        COUNT(DISTINCT IF(citizens_st_mobile.`state` = 'activated', citizens_st_mobile.flat_uuid,NULL)) as flat_uuid
        FROM db1.`citizens_st_mobile_ch` AS citizens_st_mobile
        LEFT JOIN db1.`subscriptions_st_mobile_ch` AS subscriptions_st_mobile
                ON citizens_st_mobile.`citizen_id` = subscriptions_st_mobile.`citizen_id`
                AND citizens_st_mobile.`report_date` = subscriptions_st_mobile.`report_date`
        LEFT JOIN db1.`entries_installation_points_dir_partner_ch` AS entries_installation_points 
                ON citizens_st_mobile.`address_uuid` = entries_installation_points.`address_uuid`
        LEFT JOIN db1.`installation_point_st_partner_ch` AS  installation_point_st
        ON entries_installation_points.`installation_point_id` = installation_point_st.`installation_point_id`
        AND installation_point_st.`report_date` = citizens_st_mobile.`report_date`
        WHERE 
                citizens_st_mobile.report_date = dateTrunc('month', report_date) 
                AND citizens_st_mobile.report_date >'2023-12-31'
        GROUP BY partner_uuid,
                city,
                report_date
        ),
        --
        t2 AS (SELECT
                citizen_payments_st_mobile.report_date AS report_date,
        entries_installation_points.city AS city,
        installation_point_st.partner_uuid AS partner_uuid,
        sum(amount) AS payments_amount
        FROM db1.`citizen_payments_st_mobile_ch` AS citizen_payments_st_mobile
        LEFT JOIN db1.`citizens_st_mobile_ch` AS citizens_st_mobile 
                ON citizen_payments_st_mobile.citizen_id = citizens_st_mobile.citizen_id  
                AND citizen_payments_st_mobile.report_date = citizens_st_mobile.report_date 
        LEFT JOIN db1.`entries_installation_points_dir_partner_ch` AS entries_installation_points 
                ON citizens_st_mobile.`address_uuid` = entries_installation_points.`address_uuid`
        LEFT JOIN db1.`installation_point_st_partner_ch` AS installation_point_st
        ON entries_installation_points.`installation_point_id` = installation_point_st.`installation_point_id` 
        AND installation_point_st.`report_date` = citizen_payments_st_mobile.`report_date`
        WHERE 
                citizen_payments_st_mobile.report_date = dateTrunc('month', report_date) 
                AND citizen_payments_st_mobile.report_date >'2023-12-31'
                AND citizen_payments_st_mobile.`state` = 'success'
        GROUP BY 
                partner_uuid,
        city,
        report_date
        ),
        --
        t3 AS (SELECT
                report_date,
        city,
        partner_uuid,
                count(flat_uuid) AS citizen_id_in_flat_with_subscriptions
        FROM
                (SELECT 
                citizens_st_mobile.report_date AS report_date,
                city,
                installation_point_st.partner_uuid AS partner_uuid,
                if(citizens_st_mobile.flat_uuid !='',citizens_st_mobile.flat_uuid,null) AS flat_uuid,
                citizens_st_mobile.citizen_id AS citizen_id,
                if(subscriptions_st_mobile.state = 'activated',1,0) AS if_sub_active,
                max(if_sub_active) OVER (partition by citizens_st_mobile.flat_uuid, citizens_st_mobile.report_date ORDER BY citizens_st_mobile.report_date DESC) AS flat_with_sub_active
                FROM db1.citizens_st_mobile_ch AS citizens_st_mobile
                LEFT JOIN db1.subscriptions_st_mobile_ch AS subscriptions_st_mobile 
                        ON citizens_st_mobile.`citizen_id` = subscriptions_st_mobile.`citizen_id`
                        AND citizens_st_mobile.`report_date` = subscriptions_st_mobile.`report_date`
                LEFT JOIN db1.`entries_installation_points_dir_partner_ch` AS entries_installation_points 
                        ON citizens_st_mobile.`address_uuid` = entries_installation_points.`address_uuid`
                LEFT JOIN db1.`installation_point_st_partner_ch` AS  installation_point_st
                ON entries_installation_points.`installation_point_id` = installation_point_st.`installation_point_id`
                AND installation_point_st.`report_date` = citizens_st_mobile.`report_date`
                WHERE citizens_st_mobile.report_date = dateTrunc('month', report_date) 
                        AND citizens_st_mobile.report_date >'2023-12-31'
                )
        WHERE flat_with_sub_active = 1
        GROUP BY report_date,
                        city,
                        partner_uuid
        )
        --
        SELECT
                t1.report_date AS report_date,
                t1.city AS city,
                t1.partner_uuid AS partner_uuid,
                company_name,
                partner_lk,
                citizen_id_in_flat_with_subscriptions,
                payments_amount,
                activated_citizen_id,
                subscribed_citizen_id,
                flat_uuid
        FROM  t1 
        LEFT JOIN t2 ON t1.report_date = t2.report_date
                                AND t1.city = t2.city
                                AND t1.partner_uuid = t2.partner_uuid
        LEFT JOIN t3 ON t1.report_date = t3.report_date
                                AND t1.city = t3.city
                                AND t1.partner_uuid = t3.partner_uuid
        LEFT JOIN db1.companies_dir_partner_ch AS companies_dir_partner 
                                ON companies_dir_partner.partner_uuid = t1.partner_uuid
"""
ch.query_run(query_text)
```

___


### DROP CH

```python
query_text = """
DROP TABLE db1.t_subscribtions_citizens_by_companies_and_cities_ch
"""

ch.query_run(query_text)
```

### DROP MV

```python
query_text = """
DROP TABLE db1.t_subscribtions_citizens_by_companies_and_cities_mv
"""

ch.query_run(query_text)
```

### REFRESH MV

```python
query_text = """
SYSTEM REFRESH VIEW db1.t_subscribtions_citizens_by_companies_and_cities_ch
"""

ch.query_run(query_text)
```
