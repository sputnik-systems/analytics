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

# t_count_of_subscribes_by_adreses


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
## Links:
[[installation_point_st_partner]]
[[entries_installation_points_dir_partner]]
[[cameras_st_partner]]
[[subscriptions_st_mobile]]
[[citizens_st_mobile]]
[[companies_st_partner]]
[[companies_dir_partner]]


___
## Table_creating

```python
query_text = """--sql
    CREATE TABLE db1.t_count_of_subscribes_by_adreses
    (
        `report_date` Date,
        `installation_point_id` Int64,
        `address_uuid` String,
        `partner_uuid` String,
        `flats_count_full` Int16,
        `flats_count` Int16,
        `archive_from_partner` String,
        `count_of_subscribes` UInt64,
        `company_name` String,
        `partner_lk` String,
        `tin` String,
        `camera_dvr_depth` Int32,
        `monetization_is_allowed` Int16
    )
    ENGINE = MergeTree()
    ORDER BY (report_date,partner_uuid)
    """

ch.query_run(query_text)
```

___
## MV_creating

```python
query_text = """--sql
CREATE MATERIALIZED VIEW db1.t_count_of_subscribes_by_adreses_mv
REFRESH EVERY 1 DAY OFFSET 5 HOUR 33 MINUTE TO db1.t_count_of_subscribes_by_adreses AS
WITH t_entries_ip_dir_p AS (
    SELECT
        DISTINCT
        ip_st_p.report_date AS report_date,
        ip_st_p.installation_point_id AS installation_point_id,
        e_ip_dir_p.address_uuid AS address_uuid,
        e_ip_dir_p.partner_uuid AS partner_uuid,
        flats_count_full,
        flats_count,
        monetization_is_allowed
    FROM db1.installation_point_st_partner_ch AS ip_st_p
    LEFT JOIN db1.entries_installation_points_dir_partner_ch  AS e_ip_dir_p
        ON e_ip_dir_p.installation_point_id = ip_st_p.installation_point_id
    WHERE ip_st_p.installation_point_id is not null 
    AND ip_st_p.installation_point_id != 0
    ),
    --
    t_cameras_st_p AS(
    SELECT
        DISTINCT
        report_date,
        installation_point_id,
        archive_from_partner,
        camera_dvr_depth
    FROM db1.`cameras_st_partner_ch`
    ),
    --
    cameras_status AS (
    SELECT 
        DISTINCT
        t_entries_ip_dir_p.report_date AS report_date,
        t_entries_ip_dir_p.installation_point_id AS installation_point_id,
        address_uuid,
        t_entries_ip_dir_p.partner_uuid AS partner_uuid,
        flats_count_full,
        flats_count,
        archive_from_partner,
        camera_dvr_depth,
        monetization_is_allowed
    FROM t_entries_ip_dir_p
    LEFT JOIN t_cameras_st_p 
        ON t_entries_ip_dir_p.report_date = t_cameras_st_p.report_date
        AND t_entries_ip_dir_p.installation_point_id = t_cameras_st_p.installation_point_id
    ),
    --
    sub_st_m_ch AS (SELECT 
		`report_date`,
		`citizen_id`,
		`state` 
	FROM db1.subscriptions_st_mobile_ch)
	,
	cit_dir_m AS (SELECT 
		citizen_id, 
		report_date, 
		address_uuid
	FROM db1.citizens_st_mobile_ch),
   	--
    subscriptions_count AS (
    SELECT
	  sub_st_m_ch.report_date,
	  cit_dir_m.address_uuid,
	  COUNTIf(sub_st_m_ch.state = 'activated') AS count_of_subscribes
	FROM sub_st_m_ch
	LEFT ANY JOIN cit_dir_m
	  ON cit_dir_m.citizen_id = sub_st_m_ch.citizen_id
	  AND cit_dir_m.report_date = sub_st_m_ch.report_date
	GROUP BY report_date, address_uuid
    ),
    --
    company AS (
    SELECT
        report_date,
        comp_st_p.partner_uuid as partner_uuid,
        company_name,
        partner_lk,
        tin
    FROM db1.companies_st_partner_ch AS comp_st_p
    LEFT JOIN db1.companies_dir_partner_ch AS comp_dir_p ON comp_dir_p.partner_uuid = comp_st_p.partner_uuid
    )
--
SELECT
    DISTINCT
    cameras_status.report_date AS report_date,
    installation_point_id,
    cameras_status.address_uuid AS address_uuid,
    cameras_status.partner_uuid as partner_uuid,
    flats_count_full,
    flats_count,
    archive_from_partner,
    count_of_subscribes,
    company_name,
    partner_lk,
    tin,
    camera_dvr_depth,
    monetization_is_allowed
FROM cameras_status    
LEFT JOIN subscriptions_count 
    ON subscriptions_count.report_date = cameras_status.report_date
    AND subscriptions_count.address_uuid = cameras_status.address_uuid
LEFT JOIN company 
    ON cameras_status.partner_uuid = company.partner_uuid 
    AND cameras_status.report_date = company.report_date
SETTINGS join_any_take_last_row = 1,
		join_algorithm = 'partial_merge'

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
FROM db1.t_count_of_subscribes_by_adreses
ORDER BY report_date DESC
limit 10

"""

ch.query_run(query_text)
```

### refreash_mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.t_count_of_subscribes_by_adreses_mv
"""

ch.query_run(query_text)
```

___
### drop_table

```python
query_text = """ 
DROP TABLE db1.t_count_of_subscribes_by_adreses
"""

ch.query_run(query_text)
```

### drop_mv

```python
query_text = """ 
DROP TABLE db1.t_count_of_subscribes_by_adreses_mv
"""

ch.query_run(query_text)
```
