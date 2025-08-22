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

[[installation_point_st_partner]]

[[entries_installation_points_dir_partner]]

[[installation_point_st_partner]]

[[buildings_st_partner]]

[[gates_st_partner]]
___

```python
query_text = """--sql
CREATE TABLE db1.t_all_installetion_points
(
    `address_uuid` String,
    `city` String,
    `country` String,
    `created_at` String,
    `full_address` String,
    `installation_point_id` Int32,
    `parent_uuid` String ,
    `region` String,
    `report_date` Date
)
    ENGINE = MergeTree()
    ORDER BY installation_point_id
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.t_all_installetion_points_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR 50 MINUTE TO db1.t_all_installetion_points AS
    SELECT
        parent_uuid,
        installation_point_id,
        address_uuid,
        report_date,
        full_address,
        created_at,
        region,
        country,
        city
    FROM (
        SELECT
            parent_uuid,
            installation_point_id,
            address_uuid,
            report_date,
            full_address,
            created_at,
            region,
            country,
            city
        FROM db1.installation_point_st_partner_ch as installation_point_st_partner
        LEFT JOIN db1.entries_installation_points_dir_partner_ch USING(installation_point_id)
        WHERE installation_point_st_partner.installation_point_id != 0 AND installation_point_st_partner.installation_point_id IS NOT NULL
        UNION ALL
        SELECT
            parent_uuid,
            installation_point_id,
            address_uuid,
            report_date,
            arrayStringConcat(arraySlice(splitByString(',', full_address), 1, length(splitByString(',', full_address)) - 1), ',') AS full_address,
            created_at,
            region,
            country,
            city
        FROM db1.buildings_st_partner_ch as buildings_st_partner
        LEFT JOIN db1.entries_installation_points_dir_partner USING(parent_uuid)
        WHERE buildings_st_partner.installation_point_id != 0 AND buildings_st_partner.installation_point_id IS NOT NULL
        UNION ALL
        SELECT
            parent_uuid,
            installation_point_id,
            address_uuid,
            report_date,
            arrayStringConcat(arraySlice(splitByString(',', full_address), 1, length(splitByString(',', full_address)) - 1), ',') AS full_address,
            created_at,
            region,
            country,
            city
        FROM db1.gates_st_partner as gates_st_partner
        LEFT JOIN db1.entries_installation_points_dir_partner USING(parent_uuid)
        WHERE gates_st_partner.installation_point_id != 0 AND gates_st_partner.installation_point_id IS NOT NULL
    )
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
    FROM db1.t_all_installetion_points
    ORDER BY report_date desc
    limit 100
    """

ch.query_run(query_text)

```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.all_installetion_points_parquet_ch DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)

```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.all_installetion_points_parquet_mv
    """

ch.query_run(query_text)
```


### drop ch

```python
query_text = """--sql
    DROP TABLE db1.all_installetion_points_parquet_ch
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.all_installetion_points_parquet_mv
"""

ch.query_run(query_text)
```
