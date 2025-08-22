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

[[complaints_st_partner]]

[[cameras_st_partner]]

[[intercoms_st_partner]]

[[installation_point_st_partner]]

[[entries_installation_points_dir_partner]]

[[companies_dir_partner]]

[[t_all_installetion_points]]
___

```python
query_text = """--sql
WITH data_range AS(
SELECT
	report_date
FROM db1.complaints_st_partner_ch
),
--
cameras_st_partner AS(
SELECT
	report_date,
	 camera_uuid,
	 installation_point_id
FROM db1.cameras_st_partner_ch
WHERE report_date in data_range
    ),
--
intercoms_st_partner AS(
SELECT
	report_date,
	 intercom_uuid,
	 installation_point_id
FROM db1.intercoms_st_partner_ch
WHERE report_date in data_range
    ),
companies AS(
SELECT
	report_date,
	parent_uuid,
	ins_p_s_p.partner_uuid AS partner_uuid,
	partner_lk,
	tin,
	company_name 
FROM db1.installation_point_st_partner_ch AS ins_p_s_p
LEFT JOIN db1.entries_installation_points_dir_partner_ch AS eipdp 
	ON eipdp.installation_point_id = ins_p_s_p.installation_point_id
LEFT JOIN db1.companies_dir_partner_ch AS cdp
	ON cdp.partner_uuid = ins_p_s_p.partner_uuid
WHERE report_date in data_range
	)	
--
SELECT
	 com_s_p.report_date AS report_date,
    `issue_id`, 
    `src_type`, 
    `src_uuid`, 
    `src_address_uuid`, 
    `src_address_type`, 
    `device_uuid`, 
    `device_type`, 
    `err_code`, 
    `err_src`,
    `err_func`, 
    `aasm_state`,
    `process_error`, 
    `actor_type`, 
    `actor_identifier`,
    com_s_p.`created_at` AS created_at,
    `updated_at`,
    com_s_p.`installation_point_id` AS installation_point_id,
    `city`,
    `full_address`,
    `region`,
    all_in_p.`parent_uuid` AS parent_uuid,
    `partner_lk`,
	`tin`,
	`company_name` 
FROM    
	(SELECT
	    com_s_p.`report_date` AS report_date,
	    `issue_id`, -- это на будущее,  когда будет группировка жалоб по ишьб
	    `src_type`, --какая сущность пожаловалсь (Citizen/Camera/Intercom)
	    `src_uuid`, -- uuid этой сущности
	    `src_address_uuid`, -- адрес этой сущности
	    `src_address_type`, -- адрес этой сущности
	    `device_uuid`, -- uuid устройтсва на которое жалуется сущность 
	    `device_type`, 
	    `err_code`, --  код ошибки intercom_offline / video / no_intercom
	    `err_src`,	-- откуда пришло MOB (мобилка) / WEB (веб интерфейсы для партнеров) / API (бэк )/ DEV (устройство)
	    `err_func`, --sync (там еще будет)
	    `aasm_state`,	-- статус жалобы pending / processed / failed  
	    `process_error`, -- ошибка если статус failed  
	    `actor_type`, -- какой живой человек с сердцем пожаловался или авто система (будет либо Citizen, Partner, auto)
	    `actor_identifier`, --uuid / id актэра
	    `created_at`,
	    `updated_at`,
	      if(cameras_st_partner.`installation_point_id` = 0 
	      OR cameras_st_partner.`installation_point_id` IS NULL, 
	      intercoms_st_partner.`installation_point_id`,  cameras_st_partner.`installation_point_id`) AS installation_point_id
	FROM db1.complaints_st_partner_ch AS com_s_p
	LEFT JOIN cameras_st_partner 
		ON cameras_st_partner.report_date = com_s_p.report_date 
		AND cameras_st_partner.camera_uuid = com_s_p.device_uuid
	LEFT JOIN intercoms_st_partner 
		ON intercoms_st_partner.report_date = com_s_p.report_date 
		AND intercoms_st_partner.intercom_uuid = com_s_p.device_uuid
		) AS com_s_p
	LEFT JOIN db1.t_all_installetion_points AS all_in_p
		ON all_in_p.report_date = com_s_p.report_date
		AND all_in_p.installation_point_id = com_s_p.installation_point_id
	LEFT JOIN companies 
		ON companies.report_date = all_in_p.report_date
		AND companies.parent_uuid = all_in_p.parent_uuid
"""

ch.get_schema(query_text)
```

```python
query_text = """--sql
CREATE TABLE db1.t_complaints_st_partner
(
    `report_date` Date,
    `issue_id` String,
    `src_type` String,
    `src_uuid` String,
    `src_address_uuid` String,
    `src_address_type` String,
    `device_uuid` String,
    `device_type` String,
    `err_code` String,
    `err_src` String,
    `err_func` String,
    `aasm_state` String,
    `process_error` String,
    `actor_type` String,
    `actor_identifier` String,
    `created_at` DateTime64,
    `updated_at` DateTime64,
    `installation_point_id` Int64,
    `city` String,
    `full_address` String,
    `region` String,
    `parent_uuid` String,
    `partner_lk` String,
    `tin` String,
    `company_name` String
)
    ENGINE = MergeTree()
    ORDER BY report_date
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.t_complaints_st_partner_mv
    REFRESH EVERY 1 DAY OFFSET 5 HOUR 37 MINUTE TO db1.t_complaints_st_partner AS
WITH data_range AS(
SELECT
	report_date
FROM db1.complaints_st_partner_ch
),
--
cameras_st_partner AS(
SELECT
	report_date,
	 camera_uuid,
	 installation_point_id
FROM db1.cameras_st_partner_ch
WHERE report_date in data_range
    ),
--
intercoms_st_partner AS(
SELECT
	report_date,
	 intercom_uuid,
	 installation_point_id
FROM db1.intercoms_st_partner_ch
WHERE report_date in data_range
    ),
companies AS(
SELECT
	report_date,
	parent_uuid,
	ins_p_s_p.partner_uuid AS partner_uuid,
	partner_lk,
	tin,
	company_name 
FROM db1.installation_point_st_partner_ch AS ins_p_s_p
LEFT JOIN db1.entries_installation_points_dir_partner_ch AS eipdp 
	ON eipdp.installation_point_id = ins_p_s_p.installation_point_id
LEFT JOIN db1.companies_dir_partner_ch AS cdp
	ON cdp.partner_uuid = ins_p_s_p.partner_uuid
WHERE report_date in data_range
	)	
--
SELECT
	 com_s_p.report_date AS report_date,
    `issue_id`, 
    `src_type`, 
    `src_uuid`, 
    `src_address_uuid`, 
    `src_address_type`, 
    `device_uuid`, 
    `device_type`, 
    `err_code`, 
    `err_src`,
    `err_func`, 
    `aasm_state`,
    `process_error`, 
    `actor_type`, 
    `actor_identifier`,
    com_s_p.`created_at` AS created_at,
    `updated_at`,
    com_s_p.`installation_point_id` AS installation_point_id,
    `city`,
    `full_address`,
    `region`,
    all_in_p.`parent_uuid` AS parent_uuid,
    `partner_lk`,
	`tin`,
	`company_name` 
FROM    
	(SELECT
	    com_s_p.`report_date` AS report_date,
	    `issue_id`, -- это на будущее,  когда будет группировка жалоб по ишьб
	    `src_type`, --какая сущность пожаловалсь (Citizen/Camera/Intercom)
	    `src_uuid`, -- uuid этой сущности
	    `src_address_uuid`, -- адрес этой сущности
	    `src_address_type`, -- адрес этой сущности
	    `device_uuid`, -- uuid устройтсва на которое жалуется сущность 
	    `device_type`, 
	    `err_code`, --  код ошибки intercom_offline / video / no_intercom
	    `err_src`,	-- откуда пришло MOB (мобилка) / WEB (веб интерфейсы для партнеров) / API (бэк )/ DEV (устройство)
	    `err_func`, --sync (там еще будет)
	    `aasm_state`,	-- статус жалобы pending / processed / failed  
	    `process_error`, -- ошибка если статус failed  
	    `actor_type`, -- какой живой человек с сердцем пожаловался или авто система (будет либо Citizen, Partner, auto)
	    `actor_identifier`, --uuid / id актэра
	    `created_at`,
	    `updated_at`,
	      if(cameras_st_partner.`installation_point_id` = 0 
	      OR cameras_st_partner.`installation_point_id` IS NULL, 
	      intercoms_st_partner.`installation_point_id`,  cameras_st_partner.`installation_point_id`) AS installation_point_id
	FROM db1.complaints_st_partner_ch AS com_s_p
	LEFT JOIN cameras_st_partner 
		ON cameras_st_partner.report_date = com_s_p.report_date 
		AND cameras_st_partner.camera_uuid = com_s_p.device_uuid
	LEFT JOIN intercoms_st_partner 
		ON intercoms_st_partner.report_date = com_s_p.report_date 
		AND intercoms_st_partner.intercom_uuid = com_s_p.device_uuid
		) AS com_s_p
	LEFT JOIN db1.t_all_installetion_points AS all_in_p
		ON all_in_p.report_date = com_s_p.report_date
		AND all_in_p.installation_point_id = com_s_p.installation_point_id
	LEFT JOIN companies 
		ON companies.report_date = all_in_p.report_date
		AND companies.parent_uuid = all_in_p.parent_uuid
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
    FROM db1.t_complaints_st_partner
    ORDER BY report_date DESC
    limit 10
    """

ch.query_run(query_text)

```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.t_complaints_st_partner DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)

```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.t_complaints_st_partner_mv
    """

ch.query_run(query_text)
```


### drop ch

```python
query_text = """--sql
    DROP TABLE db1.t_complaints_st_partner
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.t_complaints_st_partner_mv
"""

ch.query_run(query_text)
```
