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

device-history-by-company-and-city-clickhouse

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
```

```python
query_text = """--sql
-- Соединяем добавляем данные из справочников
SELECT 
    *
FROM db1.intercoms_st_asgard_ch AS intercoms_st_asgard_ch
						
limit 10
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    device_type,
    created_at,
    row_number() OVER (PARTITION BY device_uuid ORDER BY created_at) AS r_number
FROM db1.device_history_dir_partner
limit 2
"""
ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    device_uuid,
    device_type,
    created_at,
    if(company_uuid_from = 'b1782e4f-9198-49d1-b5aa-7bdba9c87d21' AND r_number = 1,1,0) AS first_appearance
FROM
    (SELECT
        device_type,
        device_uuid,
        company_uuid_from,
        company_uuid_to,
        created_at,
        row_number() OVER (PARTITION BY device_uuid ORDER BY created_at) AS r_number
FROM db1.device_history_dir_partner
) AS t1
limit 10
"""
ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT 
	intercoms_st_asgard_ch.report_date as report_date,
	intercoms_st_asgard_ch.intercom_uuid AS device_uuid,
	motherboard_id AS serial_number,
	'intercom' AS type,
	intercoms_st_asgard_ch.partner_uuid AS asgard_partner_uuid,
	intercoms_st_partner_ch.partner_uuid AS partner_uuid,
	intercoms_st_partner_ch.installation_point_id AS installation_point_id,
	intercoms_st_partner_ch.model_identifier as model_identifier
FROM db1.intercoms_st_asgard_ch AS intercoms_st_asgard_ch
LEFT JOIN db1.intercoms_dir_asgard_ch AS intercoms_dir_asgard_ch ON intercoms_st_asgard_ch.intercom_uuid = intercoms_dir_asgard_ch.intercom_uuid
LEFT JOIN db1.intercoms_st_partner_ch AS intercoms_st_partner_ch 
	ON intercoms_st_partner_ch.intercom_uuid = intercoms_st_asgard_ch.intercom_uuid
	AND intercoms_st_partner_ch.report_date = intercoms_st_asgard_ch.report_date
LEFT JOIN db1.intercoms_dir_partner_ch AS intercoms_dir_partner_ch ON intercoms_dir_partner_ch.intercom_uuid = intercoms_st_asgard_ch.intercom_uuid
limit 10					
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    --LAST_VALUE(cspc.installation_point_id) IGNORE NULLS OVER (PARTITION BY COALESCE(cspc.intercom_uuid,cspc.camera_uuid)  ORDER BY cspc.report_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_installation_point_id,
    --cspc.report_date as report_date,
    --COALESCE(cspc.intercom_uuid, cspc.camera_uuid) AS device_uuid,
    --COALESCE(motherboard_id, camera_serial) AS serial_number,
    IF(cspc.intercom_uuid = "","intercom","") AS device_type,
    cspc.partner_uuid as partner_uuid,
    cspc.installation_point_id as installation_point_id,
    ROW_NUMBER() OVER(PARTITION BY cspc.intercom_uuid, cspc.report_date ORDER BY cspc.installation_point_id DESC) AS bug_filter
FROM db1.cameras_st_partner_ch AS cspc
LEFT JOIN db1.installation_point_st_partner_ch  AS  ipcp
    ON cspc.installation_point_id = ipcp.installation_point_id
    AND cspc.report_date = ipcp.report_date
LEFT JOIN db1.cameras_dir_asgard_ch AS cdac
    ON cspc.camera_uuid = cdac.camera_uuid
LEFT JOIN db1.intercoms_dir_asgard_ch AS idac
    ON cspc.intercom_uuid = idac.intercom_uuid
WHERE cspc.`report_date`> DATE('2023-11-12')
LIMIT 10
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
-- Соединяем добавляем данные из справочников
WITH intercoms_t AS (SELECT 
	intercoms_st_asgard_ch.report_date as report_date,
	intercoms_st_asgard_ch.intercom_uuid AS device_uuid,
	motherboard_id AS serial_number,
	'intercom' AS type,
	intercoms_st_asgard_ch.partner_uuid AS asgard_partner_uuid,
	intercoms_st_partner_ch.partner_uuid AS partner_uuid,
	intercoms_st_partner_ch.installation_point_id AS installation_point_id,
	intercoms_st_partner_ch.model_identifier as model_identifier
FROM db1.intercoms_st_asgard_ch AS intercoms_st_asgard_ch
LEFT JOIN db1.intercoms_dir_asgard_ch AS intercoms_dir_asgard_ch ON intercoms_st_asgard_ch.intercom_uuid = intercoms_dir_asgard_ch.intercom_uuid
LEFT JOIN db1.intercoms_st_partner_ch AS intercoms_st_partner_ch 
	ON intercoms_st_partner_ch.intercom_uuid = intercoms_st_asgard_ch.intercom_uuid
	AND intercoms_st_partner_ch.report_date = intercoms_st_asgard_ch.report_date
LEFT JOIN db1.intercoms_dir_partner_ch AS intercoms_dir_partner_ch ON intercoms_dir_partner_ch.intercom_uuid = intercoms_st_asgard_ch.intercom_uuid							
),
--Добавляем данные из истории устройст. Удаляем ковычки, мешающие работе
device_history_clean AS(SELECT 
	date_of_changing, 
    number_of_changing, 
    replace(text_of_changing, '''', '') AS text_of_changing,
    replace(serial_number, '''', '') AS serial_number,
    replace(device_type, '''', '') AS device_type,
    replace(device_uuid, '''', '') AS device_uuid,
    replace(installation_point_id, '''', '') AS installation_point_id,
    replace(partner_uuid, '''', '') AS partner_uuid,
    replace(partner_uuid_lag, '''', '') AS partner_uuid_lag
FROM db1.device_history)
--
SELECT 
	report_date,
	intercoms_t.partner_uuid AS partner_uuid,
	type,
	number_of_changing,
	text_of_changing,
	installation_point_id,
	device_uuid,
	intercoms_t.serial_number as serial_number
FROM intercoms_t
LEFT JOIN device_history_clean ON device_history_clean.serial_number = intercoms_t.serial_number
							AND device_history_clean.date_of_changing = intercoms_t.report_date
limit 10
"""

ch.query_run(query_text)
```

```python
SELECT
	report_date,
	partner_uuid,
	installation_point_id,
	camera_uuid,
	intercom_uuid,
	IF(n_row = 1,1,0) AS first_appearence,
	IF(n_row != 1 AND lag_partner_uuid != partner_uuid,1,0) AS partner_uuid_change,
	IF(n_row != 1 AND lag_installation_point_id != 0 AND installation_point_id = 0,1,0) AS installation_point_removing,
	IF(n_row != 1 AND lag_installation_point_id = 0 AND installation_point_id != 0 AND installation_point_id != last_installation_point_id,1,0) AS installation_point_installation,
	IF(n_row != 1 AND last_installation_point_id = 0 AND installation_point_id != 0,1,0) AS installation_point_first_installing,
	IF(n_row != 1 AND installation_point_id !=0 AND lag_installation_point_id != installation_point_id,1,0) AS installation_pont_changing
FROM
	(SELECT
		report_date,
		partner_uuid,
		installation_point_id,
		intercom_uuid,
		camera_uuid,
		lagInFrame(installation_point_id,1,0) OVER (PARTITION BY camera_uuid ORDER BY report_date) AS lag_installation_point_id,
		lagInFrame(partner_uuid,1,'') OVER (PARTITION BY camera_uuid ORDER BY report_date) AS lag_partner_uuid,
		anyLast(installation_point_id) OVER (PARTITION BY camera_uuid ORDER BY report_date) AS last_installation_point_id,
		ROW_NUMBER() OVER (PARTITION by camera_uuid order by report_date) as n_row
	FROM db1.cameras_st_partner_ch)
```

```python
query_text = """--sql
-- Соединяем добавляем данные из справочников
WITH intercoms_t AS (SELECT 
	intercoms_st_asgard_ch.report_date as report_date,
	intercoms_st_asgard_ch.intercom_uuid AS device_uuid,
	motherboard_id AS serial_number,
	'intercom' AS type,
	intercoms_st_asgard_ch.partner_uuid AS asgard_partner_uuid,
	intercoms_st_partner_ch.partner_uuid AS partner_uuid,
	intercoms_st_partner_ch.installation_point_id AS installation_point_id,
	intercoms_st_partner_ch.model_identifier as model_identifier
FROM db1.intercoms_st_asgard_ch AS intercoms_st_asgard_ch
LEFT JOIN db1.intercoms_dir_asgard_ch AS intercoms_dir_asgard_ch ON intercoms_st_asgard_ch.intercom_uuid = intercoms_dir_asgard_ch.intercom_uuid
LEFT JOIN db1.intercoms_st_partner_ch AS intercoms_st_partner_ch 
	ON intercoms_st_partner_ch.intercom_uuid = intercoms_st_asgard_ch.intercom_uuid
	AND intercoms_st_partner_ch.report_date = intercoms_st_asgard_ch.report_date
LEFT JOIN db1.intercoms_dir_partner_ch AS intercoms_dir_partner_ch ON intercoms_dir_partner_ch.intercom_uuid = intercoms_st_asgard_ch.intercom_uuid							
),
--Добавляем данные из истории устройст. Удаляем ковычки, мешающие работе
device_history_clean AS(SELECT 
	date_of_changing, 
    number_of_changing, 
    replace(text_of_changing, '''', '') AS text_of_changing,
    replace(serial_number, '''', '') AS serial_number,
    replace(device_type, '''', '') AS device_type,
    replace(device_uuid, '''', '') AS device_uuid,
    replace(installation_point_id, '''', '') AS installation_point_id,
    replace(partner_uuid, '''', '') AS partner_uuid,
    replace(partner_uuid_lag, '''', '') AS partner_uuid_lag
FROM db1.device_history),
--
intercoms_and_history AS(SELECT 
	report_date,
	intercoms_t.partner_uuid AS partner_uuid,
	type,
	number_of_changing,
	text_of_changing,
	installation_point_id,
	device_uuid ,
	intercoms_t.serial_number as serial_number
FROM intercoms_t
LEFT JOIN device_history_clean ON device_history_clean.serial_number = intercoms_t.serial_number
							AND device_history_clean.date_of_changing = intercoms_t.report_date
)
--
SELECT 
	report_date,
	partner_uuid,
	type,
	count(serial_number)
	--count(if(text_of_changing LIKE '%Первое упоминание%',serial_number,Null)) AS count_new_serial_number
	--count(if(text_of_changing LIKE '%Возвращение на точку установки%',serial_number,Null)) AS count_serial_number_returned,
	--count(if(text_of_changing LIKE '%Изменение партнера%',serial_number,Null)) AS count_serial_number_partner_change,
	--count(if(text_of_changing LIKE '%Cнятие с точки установки%',serial_number,Null)) AS count_serial_number_removal,
	--count(if(text_of_changing LIKE '%Изменение точки установки%',serial_number,Null)) AS count_serial_number_installation_point_change,
	--count(if((text_of_changing LIKE '%Первая установка%') 
	--		OR (text_of_changing LIKE '%Первое упоминание%' AND installation_point_id != 0),serial_number,Null)) AS count_serial_number_first_installation,
	--count(if(partner_uuid != '',serial_number,Null)) AS count_serial_number,
    --count(if( installation_point_id != 0,serial_number,Null)) AS count_serial_number_on_installation_point
FROM
	intercoms_and_history
GROUP BY
	report_date,
	partner_uuid,
	type
limit 100
"""
ch.query_run(query_text)
```

```python

query_text = """--sql
    SELECT
        order_date,
        partner_lk,
        type_simple,
        sum(count_in_order) count_in_order,
        sum(sale_sum) AS sale_sum
    FROM 
        (SELECT 
            order_date,
            partner_lk,
            count_in_order,
            sale_sum,
            CASE
                WHEN model = 'ГОС' OR model = 'ГОС 22' OR model = 'ГОС БР' THEN 'ГОС'
                WHEN model = 'ИО' OR model = 'ИО 22' OR model = 'ИО АПИ' OR model = 'ИО ПРО' THEN 'ИО'
                ElSE 'Прочие модели'
            END AS type_simple
        FROM db1.all_order_google_sheets
        WHERE type = 'Домофоны'
        )
    GROUP BY partner_lk,
            order_date,
            type_simple
    """
ch.query_run(query_text)
```

```python
query_text = """--sql
    WITH all_order_intercomes AS(
        SELECT
            order_date,
            partner_lk,
            type_simple,
            sum(count_in_order) count_in_order,
            sum(sale_sum) AS sale_sum
        FROM 
            (SELECT 
                order_date,
                partner_lk,
                count_in_order,
                sale_sum,
                CASE
                    WHEN model = 'ГОС' OR model = 'ГОС 22' OR model = 'ГОС БР' THEN 'ГОС'
                    WHEN model = 'ИО' OR model = 'ИО 22' OR model = 'ИО АПИ' OR model = 'ИО ПРО' THEN 'ИО'
                    ElSE 'Прочие модели'
                END AS type_simple
            FROM db1.all_order_google_sheets
            WHERE type = 'Домофоны'
            )
        GROUP BY partner_lk,
                order_date,
                type_simple),
    --
        all_order_with_accumulated_sum AS(
        SELECT
            order_date,
            if(partner_lk = 0,Null,partner_lk) as partner_lk,
            type_simple,
            count_in_order,
            sale_sum,
            sum(count_in_order) OVER (PARTITION BY partner_lk,type_simple ORDER BY order_date) AS accumulated_count,
            sum(sale_sum) OVER (PARTITION BY partner_lk,type_simple ORDER BY order_date) AS accumulated_sum
        FROM all_order_intercomes
        ),
    --			
        date_range_cross_join AS (
        SELECT 
            date_range,	
            partner_lk,
            type_simple
        FROM db1.date_range_table
        CROSS JOIN 
            (SELECT
                DISTINCT
                partner_lk,
                type_simple
            FROM all_order_with_accumulated_sum) as all_order_with_accumulated_sum
        ),
    --
        date_range_with_orders AS (SELECT
            date_range,
            if(partner_lk = 0,Null,partner_lk) as partner_lk,
            if(type_simple = '',Null, type_simple) as type_simple,
            if(count_in_order = 0,Null,count_in_order) as count_in_order,
            if(accumulated_count = 0,Null,accumulated_count) as accumulated_count,
            if(sale_sum = 0, Null,sale_sum) as sale_sum,
            if(accumulated_sum = 0,Null,accumulated_sum) AS accumulated_sum
        FROM date_range_cross_join
        LEFT JOIN all_order_with_accumulated_sum ON all_order_with_accumulated_sum.order_date = date_range_cross_join.date_range
                                                AND all_order_with_accumulated_sum.partner_lk = date_range_cross_join.partner_lk
                                                AND all_order_with_accumulated_sum.type_simple = date_range_cross_join.type_simple
        ORDER BY  type_simple,partner_lk, date_range)
    --
    SELECT
        date_range,
        partner_lk,
        type_simple,
    --	last_value(count_in_order) IGNORE NULLS OVER (partition by partner_lk,type_simple ORDER BY date_range) as count_in_order,
        count_in_order,
        last_value(accumulated_count) IGNORE NULLS OVER (partition by partner_lk,type_simple ORDER BY date_range) as accumulated_count,
        sale_sum,
    --	last_value(sale_sum) IGNORE NULLS OVER (partition by partner_lk,type_simple ORDER BY date_range) as sale_sum,
        last_value(accumulated_sum) IGNORE NULLS OVER (partition by partner_lk,type_simple ORDER BY date_range) as accumulated_sum
    FROM date_range_with_orders
    ORDER BY  type_simple,partner_lk, date_range
    """

ch.query_run(query_text)
```

```python

INSERT INTO FUNCTION s3(
    'https://storage.yandexcloud.net/aggregated-data/all_order_accumulated_data/all_order_accumulated_data.parquet',
    'parquet'
)
SETTINGS s3_truncate_on_insert = 1
WITH all_order_intercomes AS(
	SELECT
		order_date,
		partner_lk,
		type_simple,
		sum(count_in_order) count_in_order,
		sum(sale_sum) AS sale_sum
	FROM 
		(SELECT 
			order_date,
			partner_lk,
			count_in_order,
			sale_sum,
			CASE
				WHEN model = 'ГОС' OR model = 'ГОС 22' OR model = 'ГОС БР' THEN 'ГОС'
				WHEN model = 'ИО' OR model = 'ИО 22' OR model = 'ИО АПИ' OR model = 'ИО ПРО' THEN 'ИО'
				ElSE 'Прочие модели'
			END AS type_simple
		FROM db1.all_order_google_sheets
		WHERE type = 'Домофоны'
		)
	GROUP BY partner_lk,
			order_date,
			type_simple),
--
	all_order_with_accumulated_sum AS(
	SELECT
		order_date,
		if(partner_lk = 0,Null,partner_lk) as partner_lk,
		type_simple,
		count_in_order,
		sale_sum,
		sum(count_in_order) OVER (PARTITION BY partner_lk,type_simple ORDER BY order_date) AS accumulated_count,
		sum(sale_sum) OVER (PARTITION BY partner_lk,type_simple ORDER BY order_date) AS accumulated_sum
	FROM all_order_intercomes
	),
--			
	date_range_cross_join AS (
	SELECT 
		date_range,	
		partner_lk,
		type_simple
	FROM db1.date_range_table
	CROSS JOIN 
		(SELECT
			DISTINCT
			partner_lk,
			type_simple
		FROM all_order_with_accumulated_sum) as all_order_with_accumulated_sum
	),
--
	date_range_with_orders AS (SELECT
		date_range,
		if(partner_lk = 0,Null,partner_lk) as partner_lk,
		if(type_simple = '',Null, type_simple) as type_simple,
		if(count_in_order = 0,Null,count_in_order) as count_in_order,
		if(accumulated_count = 0,Null,accumulated_count) as accumulated_count,
		if(sale_sum = 0, Null,sale_sum) as sale_sum,
		if(accumulated_sum = 0,Null,accumulated_sum) AS accumulated_sum
	FROM date_range_cross_join
	LEFT JOIN all_order_with_accumulated_sum ON all_order_with_accumulated_sum.order_date = date_range_cross_join.date_range
											AND all_order_with_accumulated_sum.partner_lk = date_range_cross_join.partner_lk
											AND all_order_with_accumulated_sum.type_simple = date_range_cross_join.type_simple
	ORDER BY  type_simple,partner_lk, date_range)
--
SELECT
	date_range,
	partner_lk,
	type_simple,
--	last_value(count_in_order) IGNORE NULLS OVER (partition by partner_lk,type_simple ORDER BY date_range) as count_in_order,
	count_in_order,
	last_value(accumulated_count) IGNORE NULLS OVER (partition by partner_lk,type_simple ORDER BY date_range) as accumulated_count,
	sale_sum,
--	last_value(sale_sum) IGNORE NULLS OVER (partition by partner_lk,type_simple ORDER BY date_range) as sale_sum,
	last_value(accumulated_sum) IGNORE NULLS OVER (partition by partner_lk,type_simple ORDER BY date_range) as accumulated_sum
FROM date_range_with_orders
ORDER BY  type_simple,partner_lk, date_range


```
