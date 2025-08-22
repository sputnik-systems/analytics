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
    CREATE TABLE db1.t_subscribtions_citizens_by_companies_and_cities_address_ch 
    (
        `report_date` Date,
        `intercom_uuid` String,
        `motherboard_id` String,
        `city` String,
        `full_address` String,
        `company_name` String,
        `partner_uuid` String,
        `partner_lk` String,
        `tin` String,
        `citizen_id_in_flat_with_subscriptions` UInt32,
        `payments_amount` UInt32,
        `activated_citizen_id` UInt32,
        `subscribed_citizen_id` UInt32,
        `flat_uuid` UInt32,
        `address_uuid` String
    )
    ENGINE = MergeTree()
    ORDER BY report_date
    """
ch.query_run(query_text)

```

___

```python
query_text = """--sql
CREATE MATERIALIZED VIEW db1.t_subscribtions_citizens_by_companies_and_cities_address_mv
REFRESH EVERY 1 DAY OFFSET 5 HOUR 27 MINUTE TO db1.t_subscribtions_citizens_by_companies_and_cities_address_ch AS 
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
	),
	citizen_payments_st_mobile AS (
	SELECT
		report_date,
		citizen_id,
		amount
	FROM db1.citizen_payments_st_mobile_ch 
	WHERE state = 'success'
		AND report_date = yesterday()
	),
	t4 AS(
	SELECT  
		intercoms_st_partner.report_date AS report_date,
		city,
		company_name,
		partner_lk,
		intercoms_st_partner.intercom_uuid AS intercom_uuid,
		intercoms_st_partner.partner_uuid AS partner_uuid,
		motherboard_id,
		full_address,
		address_uuid,
		tin
	FROM intercoms_st_partner
	LEFT JOIN  db1.intercoms_dir_asgard_ch AS intercoms_dir_asgard
			ON intercoms_dir_asgard.intercom_uuid = intercoms_st_partner.intercom_uuid
	LEFT JOIN db1.companies_dir_partner_ch AS companies_dir_partner 
			ON companies_dir_partner.partner_uuid = intercoms_st_partner.partner_uuid
	LEFT JOIN installation_point_st
		ON installation_point_st.`installation_point_id` = intercoms_st_partner.`installation_point_id`
		AND installation_point_st.`report_date` = intercoms_st_partner.`report_date`
	LEFT JOIN db1.entries_installation_points_dir_partner_ch AS entries_installation_points 
			ON installation_point_st.`installation_point_id` = entries_installation_points.`installation_point_id`
	),
	--
	t1 AS (SELECT
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
	),
	--
	t2 AS (SELECT
		citizen_payments_st_mobile.report_date AS report_date,
		intercoms_st_partner.intercom_uuid AS intercom_uuid,
		sum(amount) AS payments_amount
	FROM citizen_payments_st_mobile
	LEFT JOIN citizens_st_mobile 
			ON citizen_payments_st_mobile.citizen_id = citizens_st_mobile.citizen_id  
			AND citizen_payments_st_mobile.report_date = citizens_st_mobile.report_date 
	LEFT JOIN db1.`entries_installation_points_dir_partner_ch` AS entries_installation_points 
			ON citizens_st_mobile.`address_uuid` = entries_installation_points.`address_uuid`
	LEFT JOIN installation_point_st
			ON entries_installation_points.`installation_point_id` = installation_point_st.`installation_point_id` 
			AND installation_point_st.`report_date` = citizen_payments_st_mobile.`report_date`
	LEFT JOIN intercoms_st_partner 
			ON intercoms_st_partner.installation_point_id = installation_point_st.installation_point_id 
			AND intercoms_st_partner.report_date = citizen_payments_st_mobile.report_date
	LEFT JOIN db1.companies_dir_partner_ch AS companies_dir_partner 
			ON companies_dir_partner.partner_uuid = intercoms_st_partner.partner_uuid
	LEFT JOIN  db1.intercoms_dir_asgard_ch AS intercoms_dir_asgard
			ON intercoms_dir_asgard.intercom_uuid = intercoms_st_partner.intercom_uuid
	GROUP BY 	
			report_date,
			intercom_uuid
	),
	--
	t3 AS (SELECT
		report_date,
		intercom_uuid,
		count(if(citizen_id !=0,citizen_id,NULL)) AS citizen_id_in_flat_with_subscriptions
	FROM
		(SELECT
			installation_point_st.report_date AS report_date,
			installation_point_st.citizen_id AS citizen_id,
			installation_point_st.installation_point_id AS installation_point_id,
			installation_point_st.flat_with_sub_active AS flat_with_sub_active,
			intercom_uuid
		FROM
			(SELECT 
				citizens_st_mobile.report_date AS report_date,
				citizens_st_mobile.citizen_id AS citizen_id,
				installation_point_st.installation_point_id AS installation_point_id,
				if(subscriptions_st_mobile.state = 'activated',1,0) AS if_sub_active,
				max(if_sub_active) OVER (partition by citizens_st_mobile.flat_uuid, citizens_st_mobile.report_date ORDER BY citizens_st_mobile.report_date DESC) AS flat_with_sub_active
			FROM citizens_st_mobile AS citizens_st_mobile
			LEFT JOIN subscriptions_st_mobile 
				ON citizens_st_mobile.`citizen_id` = subscriptions_st_mobile.`citizen_id`
				AND citizens_st_mobile.`report_date` = subscriptions_st_mobile.`report_date`
			LEFT JOIN db1.`entries_installation_points_dir_partner_ch` AS entries_installation_points 
				ON citizens_st_mobile.`address_uuid` = entries_installation_points.`address_uuid`
			LEFT JOIN db1.`installation_point_st_partner_ch` AS  installation_point_st
				ON entries_installation_points.`installation_point_id` = installation_point_st.`installation_point_id`
				AND installation_point_st.`report_date` = citizens_st_mobile.`report_date`) AS installation_point_st
			LEFT JOIN intercoms_st_partner 
				ON intercoms_st_partner.installation_point_id = installation_point_st.installation_point_id 
				AND intercoms_st_partner.report_date = installation_point_st.report_date
		WHERE flat_with_sub_active = 1
		)
	GROUP BY report_date,
			intercom_uuid
	)
	--
	SELECT
		t1.report_date AS report_date,
		t1.intercom_uuid AS intercom_uuid,
		t4.full_address AS full_address,
		t4.company_name AS company_name,
		t4.city AS city,
		t4.partner_uuid AS partner_uuid,
		t4.partner_lk AS partner_lk,
		t4.motherboard_id AS motherboard_id,
		t4.tin AS tin,
		t4.address_uuid AS address_uuid,
		payments_amount,
		activated_citizen_id,
		subscribed_citizen_id,
		flat_uuid,
		citizen_id_in_flat_with_subscriptions
	FROM  t1 
	LEFT JOIN t2 ON t1.report_date = t2.report_date
				AND t1.intercom_uuid = t2.intercom_uuid
	LEFT JOIN t3 ON t1.report_date = t3.report_date
				AND t1.intercom_uuid = t3.intercom_uuid
	LEFT JOIN t4  ON t1.report_date = t4.report_date
				AND t1.intercom_uuid = t4.intercom_uuid
--	SETTINGS join_algorithm = 'partial_merge'
	"""
ch.query_run(query_text)
```

```python
query_text = """
    SELECT
        *
    FROM db1.t_subscribtions_citizens_by_companies_and_cities_address_ch
    WHERE intercom_uuid !=''
        AND full_address !=''
    ORDER BY report_date DESC
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
