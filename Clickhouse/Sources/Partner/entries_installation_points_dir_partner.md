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
### Tags: #Source #Partner

### Links: 
___

```python
# creating a table from s3

query_text = """--sql
    CREATE TABLE db1.entries_installation_points_dir_partner
    (
        `full_address` String,
        `created_at` String,
        `number` Int32,
        `lat` String,
        `lon` String,
        `first_flat` Int16,
        `last_flat` Int16,
        `flats_count` Int16,
        `address_uuid` String,
        `parent_uuid` String,
        `partner_uuid` String,
        `installation_point_id` Int64,
        `region` String,
        `country` String,
        `city` String,
        `city_uuid` String
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/entries_installation_points_dir_partner/entries_installation_points_dir_partner.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
   CREATE TABLE db1.entries_installation_points_dir_partner_ch
    (
        `full_address` String,
        `created_at` String,
        `number` Int32,
        `lat` String,
        `lon` String,
        `first_flat` Int16,
        `last_flat` Int16,
        `flats_count_full` Int16,
        `flats_count` Int16,
        `address_uuid` String,
        `parent_uuid` String,
        `partner_uuid` String,
        `installation_point_id` Int64,
        `region` String,
        `country` String,
        `city` String,
        `city_uuid` String,
        `parts` Array(String),
        `entrance_number` String,
        `building_number` String,
        `streat_name` String        
    )
    ENGINE = MergeTree()
    ORDER BY installation_point_id
    """

ch.query_run(query_text)
```

Количество квартир в базе отличается от квартир на подъездах

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.entries_installation_points_dir_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.entries_installation_points_dir_partner_ch AS
    SELECT
	    `full_address`,
	    `created_at`,
	    `number`,
	    `lat`,
	    `lon`,
	    `first_flat`,
	    `last_flat`,
	    `flats_count`,
	    `last_flat` - `first_flat` + 1 AS `flats_count_full`,
	    `address_uuid`,
	    `parent_uuid`,
	    `partner_uuid`,
	    `installation_point_id`,
	    `region`,
	    `country`,
	    `city`,
		`city_uuid`,
		splitByString(', ', full_address) AS parts,
        parts[-1] AS entrance_number,
        parts[-2] AS building_number,
        parts[-3] AS streat_name 
	FROM db1.entries_installation_points_dir_partner
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
    FROM db1.entries_installation_points_dir_partner_ch
    limit 10
    """

ch.query_run(query_text)
```

### Drop ch

```python
query_text = """
    DROP TABLE db1.entries_installation_points_dir_partner_ch
    """
ch.query_run(query_text)
```

### Drop mv

```python
query_text = """
    DROP TABLE db1.entries_installation_points_dir_partner_mv
    """
ch.query_run(query_text)
```
