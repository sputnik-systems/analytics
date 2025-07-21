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
___
## Links:
[[installation_point_st_partner]]
[[entries_installation_points_dir_partner]]
[[companies_st_partner]]
[[companies_dir_partner]]
[[intercoms_st_partner]]


___
## Table_creating

```python
query_text = """--sql
    CREATE TABLE db1.t_billings_cohorts
    (
        `full_address` String,
        `region` String,
        `city` String,
        `country` String,
        `report_date` Date,
        `installation_point_id` Int64,
        `digital_keys_count` String,
        `device_keys_count` String,
        `monetization_is_allowed` Int16,
        `monetization` Int16,
        `partner_uuid` String,
        `parent_uuid` String,
        `flats_count` Int16,
        `company_name` String,
        `tin` String,
        `kpp` String,
        `partner_lk` String,
        `lat` String,
        `lon` String,
        `building_cohorts` String,
        `building_cohorts_rank` String,
        `intercom_uuid` String,
        `flat_range` String
    )
    ENGINE = MergeTree()
    ORDER BY partner_uuid
    """

ch.query_run(query_text)
```

___
## MV_creating

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.t_billings_cohorts_mv
    REFRESH EVERY 1 DAY OFFSET 5 HOUR 30 MINUTE TO db1.t_billings_cohorts AS
SELECT
    `full_address`,
    `region`,
    `city`,
    `country`,
    installation_point_st_partner.`report_date` AS `report_date`,
    installation_point_st_partner.`installation_point_id` AS `installation_point_id`,
    `digital_keys_count`,
    `device_keys_count`,
    `monetization_is_allowed`,
    `monetization`,
    installation_point_st_partner.`partner_uuid` AS `partner_uuid`,
    `parent_uuid`,
    `flats_count`,
    `company_name`,
    `tin`,
    `kpp`,
    `partner_lk`,
    `lat`,
    `lon`,
    CASE
        WHEN `flats_count`<=25 THEN 'Малоэтажки  - до 25 кв'
        WHEN `flats_count` > 25 AND `flats_count` <= 48  THEN 'Многоэтажки - от 25 до 48 кв'
        WHEN `flats_count` > 48 THEN 'Высотки - от 48 кв и больше'
        ELSE NULL
    END building_cohorts,
    CASE
        WHEN `flats_count`<=25 THEN '0. Малоэтажки  - до 25 кв'
        WHEN `flats_count` > 25 AND `flats_count` <= 48 THEN '1. Многоэтажки - от 25 до 48 кв'
        WHEN `flats_count` > 48 THEN '2. Высотки - от 48 кв и больше'
        ELSE NULL
    END building_cohorts_rank,
    intercoms_st_partner.`intercom_uuid` AS intercom_uuid,
    intercoms_st_partner.`model_identifier` as flat_range
FROM db1.`installation_point_st_partner_ch` AS installation_point_st_partner
LEFT JOIN db1.`entries_installation_points_dir_partner_ch` AS entries_installation_points_dir_partner
    ON installation_point_st_partner.`installation_point_id` = entries_installation_points_dir_partner.`installation_point_id`
LEFT JOIN db1.`companies_st_partner_ch` AS companies_st_partner
	ON `installation_point_st_partner`.`report_date` = `companies_st_partner`.`report_date`
    AND `installation_point_st_partner`.`partner_uuid` = `companies_st_partner`.`partner_uuid`
LEFT JOIN db1.`companies_dir_partner_ch` AS companies_dir_partner 
	ON companies_dir_partner.`partner_uuid` = companies_st_partner.`partner_uuid`
LEFT JOIN db1.`intercoms_st_partner_ch` AS intercoms_st_partner
	ON intercoms_st_partner.`installation_point_id` = installation_point_st_partner.`installation_point_id`
    AND intercoms_st_partner.`report_date` = installation_point_st_partner.`report_date`
WHERE installation_point_st_partner.`partner_uuid` is not null
        AND installation_point_st_partner.`installation_point_id` is not null
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
FROM db1.t_billings_cohorts
limit 100

"""

ch.query_run(query_text)
```

### refreash_mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.t_device_billins_mv
"""

ch.query_run(query_text)
```

___
### drop_table

```python
query_text = """ 
DROP TABLE db1.t_billings_cohorts
"""

ch.query_run(query_text)
```

### drop_mv

```python
query_text = """ 
DROP TABLE db1.t_billings_cohorts_mv
"""

ch.query_run(query_text)
```
