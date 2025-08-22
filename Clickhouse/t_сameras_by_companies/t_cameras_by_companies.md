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
___
# Links:

[[cameras_st_asgard]]
[[cameras_dir_partner]]
[[companies_st_partner]]
[[companies_dir_partner]]


# Creating a table


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

# Creating mv

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

```python
query_text = """--sql
SELECT
    *
FROM db1.cameras_st_asgard_ch
ORDER BY report_date DESC
LIMIT 1

    """
ch.query_run(query_text)
```

# query


```python
query_text = """--sql
SELECT
    *
FROM db1.t_cameras_by_companies
ORDER BY report_date DESC
LIMIT 1

    """
ch.query_run(query_text)
```

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>tariff_full</th>
      <th>report_date</th>
      <th>partner_uuid</th>
      <th>partner_lk</th>
      <th>company_name</th>
      <th>tin</th>
      <th>kpp</th>
      <th>camera_with_intercom_count</th>
      <th>external_camera_izi_count</th>
      <th>external_camera_foreign_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Start</td>
      <td>2023-11-01</td>
      <td>77eb19de-7879-4f7e-b2ff-6a270eae9ba2</td>
      <td>144393</td>
      <td>ООО "ИНТЕЛСК"</td>
      <td>5040075820</td>
      <td></td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>

```python
query_text = """
    DROP TABLE db1.companies_st_partner_mv
    """
ch.query_run(query_text)
```

```python
query_text = """
SYSTEM REFRESH VIEW db1.t_cameras_by_companies_mv
"""

ch.query_run(query_text)
```
