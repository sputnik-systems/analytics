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

# Links:

[[cameras_st_asgard]]
[[cameras_dir_partner]]
[[companies_st_partner]]
[[companies_dir_partner]]


# Create table


```python
query_text = """--sql
    CREATE TABLE db1.t_cameras_by_companies 
    (
        `tariff_full` String,
        `report_date` Date, 
        `partner_uuid` String,
        `partner_lk` String,
        `company_name` String,
        `tin` String,
        `kpp` String,
        `camera_with_intercom_count` UInt32,
        `external_camera_izi_count` UInt32,
        `external_camera_foreign_count` UInt32
    )
    ENGINE = MergeTree()
    ORDER BY report_date
    """
ch.query_run(query_text)
```

# Create mv

```python
query_text = """--sql
CREATE MATERIALIZED VIEW db1.t_cameras_by_companies_mv
REFRESH EVERY 1 DAY OFFSET 5 HOUR 15 MINUTE TO db1.t_cameras_by_companies AS
SELECT
    comp_s_par.tariff_full AS tariff_full,
    cam_s_asg.report_date AS report_date, 
    cam_s_asg.partner_uuid AS partner_uuid,
    comp_d_par.partner_lk AS partner_lk,
    comp_d_par.company_name AS company_name,
    comp_d_par.tin AS tin,
    comp_d_par.kpp AS kpp,
    SUM(IF(camera_with_intercom = 1, 1,0)) AS camera_with_intercom_count,
    SUM(IF(camera_with_intercom = 0 AND COALESCE(foreign_camera,0) = 0,1,0)) AS external_camera_izi_count,
    SUM(IF(camera_with_intercom = 0 AND foreign_camera = 1,1,0)) AS external_camera_foreign_count
FROM
    db1.cameras_st_asgard_ch AS cam_s_asg
    LEFT JOIN db1.cameras_dir_partner_ch AS cam_d_par ON cam_s_asg.camera_uuid = cam_d_par.camera_uuid
    LEFT JOIN db1.companies_st_partner_ch AS comp_s_par 
    						ON cam_s_asg.partner_uuid = comp_s_par.partner_uuid
                            AND cam_s_asg.report_date = comp_s_par.report_date
    LEFT JOIN db1.companies_dir_partner_ch AS comp_d_par 
    						ON comp_s_par.partner_uuid = comp_d_par.partner_uuid
GROUP BY 
    comp_s_par.tariff_full AS tariff_full,
    cam_s_asg.report_date AS report_date, 
    cam_s_asg.partner_uuid AS partner_uuid,
    comp_d_par.partner_lk AS partner_lk,
    comp_d_par.company_name AS company_name,
    comp_d_par.tin AS tin,
    comp_d_par.kpp AS kpp
    """
ch.query_run(query_text)
```

# Additional

```python
query_text = """--sql
SELECT
    DISTINCT
    partner_uuid,
    
FROM db1.billing_orders_dir_partner
limit 10
    """
ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.t_cameras_by_companies
LIMIT 10
    """
ch.query_run(query_text)
```

```python
query_text = """
    DROP TABLE db1.companies_st_partner_mv
    """
ch.query_run(query_text)
```

```python
query_text = """
SYSTEM REFRESH VIEW db1.t_cameras_by_companies
"""

ch.query_run(query_text)
```
