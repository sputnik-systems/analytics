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

```python

```

```python
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

```python
query_text = """--sql
INSERT INTO db1.t_subscribtions_citizens_by_companies_and_cities_address_ch
WITH t_citizen_id_in_flat_with_subscriptions AS(
SELECT
	report_date,
	address_uuid,
	count(if(citizen_id !=0,citizen_id,NULL)) AS citizen_id_in_flat_with_subscriptions
FROM(
	SELECT 
		report_date,
		citizen_id,
		address_uuid,
		if(ssm.state = 'activated',1,0) AS if_sub_active,
		max(if_sub_active) OVER (partition by flat_uuid, report_date ORDER BY report_date DESC) AS flat_with_sub_active
	FROM db1.rep_mobile_citizens_id_city_partner AS rmcicp
	LEFT JOIN db1.subscriptions_st_mobile_ch AS ssm 
		ON rmcicp.report_date = ssm.report_date 
		AND rmcicp.citizen_id = ssm.citizen_id
	WHERE report_date = yesterday()
	)
WHERE flat_with_sub_active = 1 AND address_uuid!=''
GROUP BY report_date,
		address_uuid),
--
t_payments_amount AS(SELECT
	report_date,
	address_uuid,
	sum(amount) AS payments_amount
FROM db1.rep_mobile_citizens_id_city_partner AS t_cit_id
JOIN db1.`citizen_payments_st_mobile_ch` AS citizen_payments_st_mobile
	ON citizen_payments_st_mobile.`report_date` = t_cit_id.`report_date`
	AND citizen_payments_st_mobile.`citizen_id` = t_cit_id.`citizen_id`
WHERE report_date = yesterday() AND address_uuid!=''
GROUP BY report_date,
		address_uuid),
--		
t_subscribed_citizen_id AS(SELECT
	report_date,
	address_uuid,
	COUNT(DISTINCT if(citizen_id !=0, citizen_id ,Null)) as subscribed_citizen_id
FROM db1.subscriptions_report_citizens_flats_comerce_rep_mobile_total
WHERE state = 'activated' AND address_uuid!=''
	AND report_date = yesterday()
GROUP BY report_date,
		address_uuid
ORDER BY report_date DESC),
--
t_activated_citizen_id AS (SELECT
	report_date,
	address_uuid,
	COUNT(DISTINCT if(citizen_id !=0, citizen_id ,Null)) as activated_citizen_id,	
	COUNT(DISTINCT if(flat_uuid !='', flat_uuid ,Null)) as flat_uuid
FROM db1.rep_mobile_citizens_id_city_partner
WHERE report_date = yesterday()  AND address_uuid!=''
GROUP BY report_date,
		address_uuid),
--
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
FROM
	(SELECT
		report_date,
		installation_point_id,
		partner_uuid,
		arrayStringConcat(groupArray(motherboard_id),',') AS motherboard_ids
	FROM db1.intercoms_st_partner_ch
	LEFT JOIN db1.intercoms_dir_asgard_ch ON intercoms_st_partner_ch.intercom_uuid = intercoms_dir_asgard_ch.intercom_uuid
	WHERE report_date = yesterday() AND installation_point_id != 0
	GROUP BY 
		report_date,
		installation_point_id,
		partner_uuid) AS intercoms
	LEFT JOIN db1.entries_installation_points_dir_partner_ch AS eipdp  
		ON intercoms.installation_point_id = eipdp.installation_point_id
	LEFT JOIN db1.companies_dir_partner AS cdp
		ON intercoms.partner_uuid = cdp.partner_uuid
)
--
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
LEFT JOIN	t_citizen_id_in_flat_with_subscriptions 
	ON t1.report_date = t_citizen_id_in_flat_with_subscriptions.report_date 
	AND t1.address_uuid = t_citizen_id_in_flat_with_subscriptions.address_uuid
LEFT JOIN	t_payments_amount 
	ON t1.report_date = t_payments_amount.report_date 
	AND t1.address_uuid = t_payments_amount.address_uuid
LEFT JOIN	t_subscribed_citizen_id
	ON t1.report_date = t_subscribed_citizen_id.report_date 
	AND t1.address_uuid = t_subscribed_citizen_id.address_uuid
LEFT JOIN	t_activated_citizen_id
	ON t1.report_date = t_activated_citizen_id.report_date 
	AND t1.address_uuid = t_activated_citizen_id.address_uuid
	"""
ch.query_run(query_text)
```

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
				AND installation_point_st.`report_date` = citizens_st_mobile.`report_date`
			) AS installation_point_st
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
