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

[[citizens_st_mobile]]

[[subscriptions_st_mobile]]

[[entries_installation_points_dir_partner]]

[[installation_point_st_partner]]

[[citizen_payments_st_mobile]]

[[intercoms_st_partner]]

```python
query_text = """--sql
WITH citizens_st_mobile AS(
	SELECT
		report_date,
		citizen_id,
		address_uuid,
		flat_uuid,
		state
	FROM db1.`citizens_st_mobile_ch` 
	WHERE `state` = 'activated'
		and report_date = yesterday()
	),
	subscriptions_st_mobile AS(
	SELECT 
		report_date,
		citizen_id,
		state
	FROM db1.subscriptions_st_mobile_ch
	WHERE state = 'activated'
		and report_date = yesterday()
	),
	installation_point_st AS (
	SELECT 
		report_date,
		installation_point_id
	FROM db1.installation_point_st_partner_ch
	WHERE report_date= yesterday()
	),
	intercoms_st_partner AS (
	SELECT
		report_date,
		intercom_uuid,
		partner_uuid,
		installation_point_id
	FROM db1.intercoms_st_partner_ch 
	WHERE report_date = yesterday()
	)
	SELECT
		citizens_st_mobile.report_date AS report_date,
		intercoms_st_partner.intercom_uuid AS intercom_uuid,
		COUNT(DISTINCT if(citizens_st_mobile.citizen_id !=0, citizens_st_mobile.citizen_id ,Null)) as activated_citizen_id,	
		COUNT(DISTINCT if(subscriptions_st_mobile.citizen_id !=0, subscriptions_st_mobile.citizen_id ,Null)) as subscribed_citizen_id,	
		COUNT(DISTINCT if(citizens_st_mobile.flat_uuid !='', citizens_st_mobile.flat_uuid ,Null)) as flat_uuid
	FROM citizens_st_mobile
	LEFT JOIN  subscriptions_st_mobile
			ON citizens_st_mobile.`citizen_id` = subscriptions_st_mobile.`citizen_id`
			AND citizens_st_mobile.`report_date` = subscriptions_st_mobile.`report_date`
	LEFT JOIN db1.`entries_installation_points_dir_partner_ch` AS entries_installation_points 
			ON citizens_st_mobile.`address_uuid` = entries_installation_points.`address_uuid`
	LEFT JOIN installation_point_st
			ON entries_installation_points.`installation_point_id` = installation_point_st.`installation_point_id`
			AND installation_point_st.`report_date` = citizens_st_mobile.`report_date`
	LEFT JOIN intercoms_st_partner 
			ON intercoms_st_partner.installation_point_id = installation_point_st.installation_point_id 
			AND intercoms_st_partner.report_date = citizens_st_mobile.report_date 
	LEFT JOIN  db1.intercoms_dir_asgard_ch AS intercoms_dir_asgard
			ON intercoms_dir_asgard.intercom_uuid = intercoms_st_partner.intercom_uuid
	LEFT JOIN db1.companies_dir_partner_ch AS companies_dir_partner 
			ON companies_dir_partner.partner_uuid = intercoms_st_partner.partner_uuid
	GROUP BY report_date,
			intercom_uuid
    LIMIT 10
	
    """
ch.get_schema(query_text)

```

```python
query_text = """--sql
    CREATE TABLE db1.t_sub_by_intercoms 
    (
        `report_date` Date,
        `intercom_uuid` String,
        `activated_citizen_id` UInt64,
        `subscribed_citizen_id` UInt64,
        `flat_uuid` UInt64
    )
    ENGINE = MergeTree()
    ORDER BY intercom_uuid
    """
ch.query_run(query_text)

```

___

```python
query_text = """--sql
CREATE MATERIALIZED VIEW db1.t_sub_by_intercoms_mv
REFRESH EVERY 1 DAY OFFSET 5 HOUR 27 MINUTE TO db1.t_sub_by_intercoms AS 
WITH citizens_st_mobile AS(
	SELECT
		report_date,
		citizen_id,
		address_uuid,
		flat_uuid,
		state
	FROM db1.`citizens_st_mobile_ch` 
	WHERE `state` = 'activated'
	),
	subscriptions_st_mobile AS(
	SELECT 
		report_date,
		citizen_id,
		state
	FROM db1.subscriptions_st_mobile_ch
	WHERE state = 'activated'
	),
	installation_point_st AS (
	SELECT 
		report_date,
		installation_point_id
	FROM db1.installation_point_st_partner_ch
	),
	intercoms_st_partner AS (
	SELECT
		report_date,
		intercom_uuid,
		partner_uuid,
		installation_point_id
	FROM db1.intercoms_st_partner_ch 
	)
	SELECT
		citizens_st_mobile.report_date AS report_date,
		intercoms_st_partner.intercom_uuid AS intercom_uuid,
		COUNT(DISTINCT if(citizens_st_mobile.citizen_id !=0, citizens_st_mobile.citizen_id ,Null)) as activated_citizen_id,	
		COUNT(DISTINCT if(subscriptions_st_mobile.citizen_id !=0, subscriptions_st_mobile.citizen_id ,Null)) as subscribed_citizen_id,	
		COUNT(DISTINCT if(citizens_st_mobile.flat_uuid !='', citizens_st_mobile.flat_uuid ,Null)) as flat_uuid
	FROM citizens_st_mobile
	LEFT JOIN  subscriptions_st_mobile
			ON citizens_st_mobile.`citizen_id` = subscriptions_st_mobile.`citizen_id`
			AND citizens_st_mobile.`report_date` = subscriptions_st_mobile.`report_date`
	LEFT JOIN db1.`entries_installation_points_dir_partner_ch` AS entries_installation_points 
			ON citizens_st_mobile.`address_uuid` = entries_installation_points.`address_uuid`
	LEFT JOIN installation_point_st
			ON entries_installation_points.`installation_point_id` = installation_point_st.`installation_point_id`
			AND installation_point_st.`report_date` = citizens_st_mobile.`report_date`
	LEFT JOIN intercoms_st_partner 
			ON intercoms_st_partner.installation_point_id = installation_point_st.installation_point_id 
			AND intercoms_st_partner.report_date = citizens_st_mobile.report_date 
	LEFT JOIN  db1.intercoms_dir_asgard_ch AS intercoms_dir_asgard
			ON intercoms_dir_asgard.intercom_uuid = intercoms_st_partner.intercom_uuid
	LEFT JOIN db1.companies_dir_partner_ch AS companies_dir_partner 
			ON companies_dir_partner.partner_uuid = intercoms_st_partner.partner_uuid
	GROUP BY report_date,
			intercom_uuid
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
    FROM db1.t_sub_by_intercoms
    limit 10
    """

ch.query_run(query_text)

```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.t_sub_by_intercoms DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)

```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.t_sub_by_intercoms_mv
    """

ch.query_run(query_text)
```

### drop ch

```python
query_text = """--sql
    DROP TABLE db1.t_sub_by_intercoms
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.t_sub_by_intercoms_mv
"""

ch.query_run(query_text)
```
