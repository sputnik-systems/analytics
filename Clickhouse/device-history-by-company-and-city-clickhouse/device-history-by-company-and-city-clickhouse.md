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

# Start

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

# t_device_history_by_companys_and_citys_ch


## main

```python
query_text = """--sql
CREATE TABLE db1.t_device_history_by_companys_and_citys_ch 
(
    report_date Date,
	partner_uuid  String,
	installation_point_id UInt32,
	camera_uuid String,
	intercom_uuid String,
    first_appearance UInt32,
    motherboard_id String,
	camera_serial String,
    partner_uuid_change UInt32,
    installation_point_removing UInt32,
    installation_point_installation UInt32,
    installation_point_first_installing UInt32,
    installation_pont_changing UInt32,
)
ENGINE = MergeTree()
ORDER BY partner_uuid
"""
ch.query_run(query_text)
```

```python
query_text = """--sql
CREATE MATERIALIZED VIEW db1.t_device_history_by_companys_and_citys_mv 
	REFRESH EVERY 1 DAY OFFSET 5 HOUR 5 MINUTE TO db1.t_device_history_by_companys_and_citys_ch AS
--таблицы с устройствами от Асгарда имеют информацию в том числе до появления первой точки установки. Но там есть лишние устройства и в целом бывают проблемы.
WITH asgard_t AS (
	SELECT
		report_date,
		cameras_dir_asgard_ch.intercom_uuid AS intercom_uuid,
		motherboard_id,
		camera_serial,
		cameras_st_asgard_ch.camera_uuid AS camera_uuid,
		cameras_st_asgard_ch.partner_uuid AS partner_uuid,
		lagInFrame(cameras_st_asgard_ch.partner_uuid) OVER (PARTITION BY camera_uuid ORDER BY report_date) AS lag_partner_uuid,
		ROW_NUMBER() OVER (PARTITION by camera_uuid order by report_date) as n_row
	FROM db1.cameras_st_asgard_ch as cameras_st_asgard_ch
	LEFT JOIN db1.cameras_dir_asgard_ch as cameras_dir_asgard_ch 
		ON cameras_st_asgard_ch.camera_uuid = cameras_dir_asgard_ch.camera_uuid
	LEFT JOIN db1.intercoms_dir_asgard_ch AS intercoms_dir_asgard_ch 
		ON intercoms_dir_asgard_ch.intercom_uuid = cameras_dir_asgard_ch.intercom_uuid
	WHERE partner_uuid !='b1782e4f-9198-49d1-b5aa-7bdba9c87d21'
),
-- таблицы по партнеру обладают информацией по точкам установки, и данные по партнерам у них точнее
partner_t AS (
SELECT
	report_date,
	partner_uuid,
	installation_point_id,
	intercom_uuid,
	camera_uuid,
	lag_installation_point_id,
	lagInFrame(last_installation_point_id,1) OVER (PARTITION BY camera_uuid ORDER BY report_date) AS last_installation_point_id
FROM
	(SELECT
		report_date,
		partner_uuid,
		installation_point_id,
		intercom_uuid,
		camera_uuid,
		lagInFrame(installation_point_id) OVER (PARTITION BY camera_uuid ORDER BY report_date) AS lag_installation_point_id,
		last_value(if(installation_point_id = 0,Null,installation_point_id)) ignore nulls OVER (PARTITION BY camera_uuid  ORDER BY `report_date` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_installation_point_id
	FROM db1.cameras_st_partner_ch)
)
SELECT
	report_date,
	asgard_t.partner_uuid AS partner_uuid,
	installation_point_id,
	camera_uuid,
	asgard_t.intercom_uuid AS intercom_uuid,
	motherboard_id,
	camera_serial,
	IF(n_row = 1,1,0) AS first_appearance,
	IF(n_row != 1 AND COALESCE(lag_partner_uuid,'') != COALESCE(asgard_t.partner_uuid,'') AND lag_partner_uuid != '',1,0) AS partner_uuid_change,
	IF(n_row != 1 AND lag_installation_point_id != 0 AND installation_point_id = 0,1,0) AS installation_point_removing,
	IF(n_row != 1 AND last_installation_point_id is Null AND installation_point_id !=0,1,0) AS installation_point_first_installing,
	IF(n_row != 1 AND 
			lag_installation_point_id = 0 AND
			installation_point_id != 0 AND
			installation_point_id = COALESCE(last_installation_point_id,0),1,0) AS installation_point_installation,
	IF(n_row != 1 AND (
				(
				lag_installation_point_id = 0 AND
				last_installation_point_id is NOT Null AND
				installation_point_id != 0 AND
				installation_point_id != COALESCE(last_installation_point_id,0)
				)
				OR
				(
				last_installation_point_id is NOT Null AND
				lag_installation_point_id != 0 AND
				lag_installation_point_id != installation_point_id
				)
				),1,0) AS installation_pont_changing
FROM asgard_t 
LEFT JOIN partner_t 
		ON asgard_t.camera_uuid = partner_t.camera_uuid
		AND asgard_t.report_date = partner_t.report_date
"""
ch.query_run(query_text)
```

## advanced

```python
query_text = """
SYSTEM REFRESH VIEW db1.t_device_history_by_companys_and_citys_mv
"""

ch.query_run(query_text)
```

```python
query_text = """
DROP TABLE db1.t_device_history_by_companys_and_citys_mv
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    sum(partner_uuid_change)
FROM db1.t_device_history_by_companys_and_citys_ch
WHERE report_date = '2025-06-13'
LIMIT 10
"""
ch.query_run(query_text)
```

# t_device_and_shipment_history_by_company_and_city


## main

```python
query_text = """--sql
CREATE TABLE db1.t_device_and_shipment_history_by_company_and_city (
    `report_date` Date,
    `partner_uuid` String,
    `device_type` String,
    `tariff` String,
    `count_serial_number` UInt32,
    `count_serial_number_not_enterprise` UInt32,
    `count_new_serial_number` UInt32,
    `count_serial_number_returned` UInt32,
    `count_serial_number_partner_change` UInt32,
    `count_serial_number_removal` UInt32,
    `count_serial_number_installation_point_change` UInt32,
    `count_serial_number_first_installation` UInt32,
    `count_serial_number_on_installation_point` UInt32,
    `count_first_appearance_on_lk` UInt32
)
ENGINE = MergeTree()
ORDER BY partner_uuid
"""
ch.query_run(query_text)
```

```python
query_text = """--sql
CREATE MATERIALIZED VIEW db1.t_device_and_shipment_history_by_company_and_city_mv 
	REFRESH EVERY 1 DAY OFFSET 5 HOUR 10 MINUTE TO db1.t_device_and_shipment_history_by_company_and_city AS
WITH  device_history AS (
SELECT
    report_date,
	partner_uuid,
	installation_point_id,
    coalesce(intercom_uuid,camera_uuid) AS device_uuid,
    if(intercom_uuid = '', 'camera','intercom') as device_type,
    if(intercom_uuid = '', camera_serial,motherboard_id) as device_serial,
    first_appearance,
    partner_uuid_change,
    installation_point_removing,
    installation_point_installation,
    installation_point_first_installing,
    installation_pont_changing,
FROM db1.t_device_history_by_companys_and_citys_ch
),
-- изменяем таблицу изменений устройств у партнеров, чтобы далее определять первое появление на личном кабинете партнера
t_first_appearance AS (SELECT
    device_uuid,
    device_type,
    toDate(created_at) as report_date,
    if(company_uuid_from = 'b1782e4f-9198-49d1-b5aa-7bdba9c87d21' AND r_number = 1,1,0) AS first_appearance_on_lk
FROM
    (SELECT
        device_type,
        device_uuid,
        company_uuid_from,
        company_uuid_to,
        created_at,
        row_number() OVER (PARTITION BY device_uuid ORDER BY created_at) AS r_number
FROM db1.device_history_dir_partner_ch
) AS t1
WHERE first_appearance_on_lk = 1
),
--
compleate_table AS (SELECT
    device_history.report_date as report_date,
    company_name,
    partner_lk,
	device_history.partner_uuid AS partner_uuid,
	installation_point_id,
    device_history.device_uuid AS device_uuid,
    device_history.device_type AS device_type,
    device_serial,
    first_appearance,
    partner_uuid_change,
    installation_point_removing,
    installation_point_installation,
    installation_point_first_installing,
    installation_pont_changing,
    tariff,
    first_appearance_on_lk
    --все устроства, что на первую доступную дату не были на личном кабинете спутника добавляются дальнейшего суммирования устройств в личных кабинетах по дням
    --max(if(first_appearance_on_lk = 0 ,if(report_date = '2023-07-12' and partner_uuid !='b1782e4f-9198-49d1-b5aa-7bdba9c87d21',1,0),first_appearance_on_lk)) 
    --OVER (PARTITION BY device_uuid ORDER BY report_date) AS first_appearance_max
FROM device_history
LEFT JOIN t_first_appearance 
    ON device_history.device_uuid = t_first_appearance.device_uuid 
    AND device_history.report_date = t_first_appearance.report_date
LEFT JOIN db1.companies_dir_partner_ch AS companies_dir_partner_ch 
    ON device_history.partner_uuid = companies_dir_partner_ch.partner_uuid
LEFT JOIN db1.companies_st_partner_ch AS companies_st_partner_ch 
    ON device_history.partner_uuid = companies_st_partner_ch.partner_uuid
    AND device_history.report_date = companies_st_partner_ch.report_date
order by report_date) 
--
SELECT 
	report_date,
	partner_uuid,
	device_type,
    tariff,
	count(if(partner_uuid != '' AND partner_uuid !='b1782e4f-9198-49d1-b5aa-7bdba9c87d21',device_uuid,null)) AS count_serial_number,
    count(if(partner_uuid != '' AND partner_uuid !='b1782e4f-9198-49d1-b5aa-7bdba9c87d21' AND tariff != 'enterprise',device_uuid,null)) AS count_serial_number_not_enterprise,
	sum(first_appearance) AS count_new_serial_number,
	sum(installation_point_installation) AS count_serial_number_returned,
    sum(partner_uuid_change) AS count_serial_number_partner_change,
	sum(installation_point_removing) AS count_serial_number_removal,
	sum(installation_pont_changing) AS count_serial_number_installation_point_change,
	sum(installation_point_first_installing) AS count_serial_number_first_installation,
    sum(if(installation_point_id != 0 ,1,0)) AS count_serial_number_on_installation_point,
    sum(first_appearance_on_lk) AS count_first_appearance_on_lk
    --sum(first_appearance_max) AS count_first_appearance_max
FROM
	compleate_table
WHERE
    report_date >= '2024-01-01'
GROUP BY
	report_date,
	partner_uuid,
	device_type,
    tariff
"""
ch.query_run(query_text)
```

## advanced

```python
query_text = """
SYSTEM REFRESH VIEW db1.t_device_and_shipment_history_by_company_and_city_mv
"""
ch.query_run(query_text)
```

```python
query_text = """
DROP TABLE db1.t_device_and_shipment_history_by_company_and_city_mv
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    sum(count_serial_number_partner_change)
FROM db1.t_device_and_shipment_history_by_company_and_city
WHERE report_date = '2025-06-13'
LIMIT 10
"""
ch.query_run(query_text)
```

```python
query_text="""
SELECT
    count(distinct camera_uuid)
FROM db1.cameras_st_asgard_ch
WHERE report_date = '2025-06-14'
    AND partner_uuid != 'b1782e4f-9198-49d1-b5aa-7bdba9c87d21'
GROUP BY report_date
limit 10
"""
ch.query_run(query_text)
```
