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

___
## Tags: #Tables
____
## Links:

[[all_installetion_points_parquet]]
[[companies_st_partner]]
[[companies_dir_partner]]
[[billing_orders_devices_st_partner]]
[[cameras_st_partner]]
[[cameras_dir_partner]]
[[intercoms_dir_asgard]]


____
## Creating a table

```python
query_text = """--sql
    CREATE TABLE db1.t_device_billins
    (
        `report_date` Date,
        `service_partner_uuid` String,
        `partner_uuid` String,
        `device_uuid` String,
        `device_serial_number` String,
        `device_type` LowCardinality(String),
        `installation_point_id` UInt32,
        `camera_dvr_depth` UInt8,
        `foreign_camera` UInt8,
        `archive_from_partner` LowCardinality(String),
        `included_by` String,
        `included_at` DateTime,
        `disabled_by` String,
        `disabled_at` DateTime,
        `service` LowCardinality(String),
        `total` Float32,
        `is_blocked` UInt8,
        `pro_subs` UInt8,
        `enterprise_subs` UInt8,
        `billing_pro` UInt8,
        `enterprise_not_paid` UInt8,
        `enterprise_test` UInt8,
        `balance` Float64,
        `kz_pro`UInt8,
        `company_name` String,
        `partner_lk` LowCardinality(String),
        `registration_date` String,
        `tin` String,
        `kpp` String,
        `city` String,
        `country` LowCardinality(String),
        `full_address` String,
        `parent_uuid` String,
        `region` LowCardinality(String)
    )

    ENGINE = MergeTree()
    ORDER BY report_date
    """

ch.query_run(query_text)
```

____
## Creating a MV

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.t_device_billins_mv
    REFRESH EVERY 1 DAY OFFSET 5 HOUR 25 MINUTE TO db1.t_device_billins AS
    WITH all_installetion_points_parquet AS (
    SELECT
        `address_uuid`,
        `city`,
        `country`,
        `created_at`,
        `full_address`,
        `installation_point_id`,
        `parent_uuid`,
        `region`,
        `report_date`,
    FROM db1.`all_installetion_points_parquet_ch`
    ),
    companies_st_partner AS (
    SELECT
        `report_date`,
        companies_st_partner.`partner_uuid` as partner_uuid,
        `is_blocked`,
        `pro_subs`,
        `enterprise_subs`,
        `billing_pro`,
        `enterprise_not_paid`,
        `enterprise_test`,
        `balance`,
        `kz_pro`,
        `company_name`,
        `partner_lk`,
        `registration_date`,
        `tin`,
        `kpp`
    FROM db1.`companies_st_partner_ch` AS companies_st_partner 
    LEFT JOIN db1.`companies_dir_partner_ch` AS companies_dir_partner 
        ON  companies_st_partner.partner_uuid = companies_dir_partner.partner_uuid 
    ),
    billing_orders_devices_dir_partner AS (
    SELECT
        `billing_account_id`,
        `cost`,
        `count`,
        toDate(`created_at`) as report_date,
        `device_type`,
        `device_uuid`,
        `partner_uuid`,
        `service`,
        `state`,
        `total`
    FROM db1.`billing_orders_devices_st_partner_ch`
    ),
    cameras_st_partner AS (
    SELECT
        `report_date`,
        cameras_st_partner.`service_partner_uuid` AS service_partner_uuid,
        cameras_st_partner.`partner_uuid` AS partner_uuid,
        if(`motherboard_id`!= '',`motherboard_id`,`serial_number`) AS device_serial_number,
        if(cameras_st_partner.`intercom_uuid` != '',cameras_st_partner.`intercom_uuid` ,cameras_st_partner.`camera_uuid`) AS device_uuid,
        if(cameras_st_partner.`intercom_uuid` != '', 'intercom', if(`foreign_camera` = 1,'camera_foreign','camera')) AS device_type,
        cameras_st_partner.`installation_point_id` AS installation_point_id,
        `camera_dvr_depth`,
        `archive_from_partner`,
        `included_by`,
        `included_at`,
        `disabled_by`,
        `disabled_at`,
        `foreign_camera`
    FROM db1.`cameras_st_partner_ch` AS cameras_st_partner
    LEFT JOIN db1.`cameras_dir_partner_ch` AS cameras_dir_partner 
        ON cameras_st_partner.camera_uuid = cameras_dir_partner.camera_uuid
    LEFT JOIN db1.`intercoms_dir_asgard_ch` AS intercoms_dir_asgard 
        ON cameras_st_partner.intercom_uuid = intercoms_dir_asgard.intercom_uuid
    )
    --
    SELECT 
        DISTINCT
        cameras_st_partner.`report_date` AS report_date,
        `service_partner_uuid`,
        cameras_st_partner.`partner_uuid` AS partner_uuid,
        cameras_st_partner.`device_uuid` AS device_uuid,
        device_serial_number,
        CASE
            WHEN cameras_st_partner.device_type = 'intercom' THEN 'Домофон'
            WHEN cameras_st_partner.device_type = 'camera' THEN 'Камера IZI'
            WHEN cameras_st_partner.device_type = 'camera_foreign' THEN 'Сторонняя камера'
            ELSE NULL
        END AS device_type,
        cameras_st_partner.`installation_point_id` AS installation_point_id,
        `camera_dvr_depth`,
        `foreign_camera`,
        `archive_from_partner`,
        `included_by`,
        `included_at`,
        `disabled_by`,
        `disabled_at`,
        IF(`service`!='', `service`, 'Нет услуг') AS `service`,
        `total`,
        `is_blocked`,
        `pro_subs`,
        `enterprise_subs`,
        `billing_pro`,
        `enterprise_not_paid`,
        `enterprise_test`,
        `balance`,
        `kz_pro`,
        `company_name`,
        `partner_lk` ,
        `registration_date`
        `tin`,
        `kpp`,
        `city`,
        `country`,
        `full_address`,
        `parent_uuid`,
        `region`
    FROM cameras_st_partner
    LEFT JOIN billing_orders_devices_dir_partner
        ON billing_orders_devices_dir_partner.report_date = cameras_st_partner.report_date    
        AND billing_orders_devices_dir_partner.device_uuid = cameras_st_partner.device_uuid
    LEFT JOIN  companies_st_partner
        ON companies_st_partner.report_date = cameras_st_partner.report_date    
        AND companies_st_partner.partner_uuid = cameras_st_partner.partner_uuid
    LEFT JOIN all_installetion_points_parquet
        ON all_installetion_points_parquet.report_date = cameras_st_partner.report_date    
        AND all_installetion_points_parquet.installation_point_id = cameras_st_partner.installation_point_id
    """
ch.query_run(query_text)
```

___
## query

```python
query_text = """ 
SELECT
    *
FROM db1.t_device_billins
WHERE report_date = '2025-06-07'
limit 10
"""

ch.query_run(query_text)
```

___
## DROP

```python
query_text = """ 
DROP TABLE db1.t_device_billins
"""

ch.query_run(query_text)
```

___
## REFRESH

```python
query_text = """
SYSTEM REFRESH VIEW db1.t_device_billins_mv
"""

ch.query_run(query_text)
```
