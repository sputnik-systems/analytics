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

## Tags #Tables
## Links:
[[entries_installation_points_dir_partner]]


____

```python
query_text = """--sql
    CREATE TABLE db1.building_test_1
    (
    `building_address`	String,
    `Street` String,
    `building_number` String,	
    `parent_uuid` String,
    `region` String,
    `country` String,
    `city` String,
    `city_uuid` String,
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/for_script_1/*','CSVWithNames')
    SETTINGS format_csv_delimiter=';'
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    SELECT
        *
    FROM db1.building_test_1
    limit 100
    """

ch.get_schema(query_text)
```

```python
query_text = """
    DROP TABLE db1.2gis
    """
ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.2gis
    (
        `Id дома` String,
        `Название` String,
        `Проект` String,
        `Субъект` String,
        `Район субъекта` String,
        `Город` String,
        `Район города` String,
        `Почтовый индекс` Int32,
        `Улица 1` String,
        `Дом 1` String,
        `Улица 2` String,
        `Дом 2 `String,
        `Улица 3`String,
        `Дом 3`String,
        `Этажность` Int16,
        `Назначение` String,
        `Количество организаций`Int16,
        `Застройщики` String,
        `Название слоя` String,
        `Диапазон квартир` String,
        `Количество подъездов` Int16,
        `Квартир в доме` Int32,
        `Квартир в подъезде` Int32
        )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/2GIS/*','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    SELECT
        *
    FROM db1.2gis
    limit 100
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    SELECT
        `Квартир в подъезде`,
        `Квартир в доме`,
        `Количество подъездов`
    FROM db1.2gis
    limit 100
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    SELECT
        upperUTF8(2gis.`Дом 1`),
        upperUTF8(2gis.`Город`),
        upperUTF8(2gis.`Улица 1`)
    FROM db1.2gis AS 2gis 
    limit 100
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    SELECT
        count(parent_uuid)
    FROM db1.building_test_1 AS building
    limit 100
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    SELECT
        count(parent_uuid)
    FROM
        (SELECT
            `building_address`,
            `Street`,
            `building_number`,
            `parent_uuid`,
            `region`,
            `country`,
            `city`,
            `city_uuid`,
            2gis.`Квартир в подъезде`,
            2gis.`Квартир в доме`,
            2gis.`Количество подъездов`
        FROM db1.building_test_1 AS building
        LEFT JOIN db1.2gis AS 2gis 
            ON upperUTF8(2gis.`Дом 1`) = upperUTF8(building.building_number) 
            AND upperUTF8(2gis.`Город`)  = upperUTF8(building.city)
            AND upperUTF8(2gis.`Улица 1`)  = upperUTF8(building.Street)
        WHERE 2gis.`Квартир в подъезде` != 0
        )
    limit 100
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    SELECT
        2gis.upper(`Дом 1`)
        2gis.upper(`Город`)
        2gis.upper(`Улица 1`),
        2gis.`Квартир в подъезде`,
        2gis.`Квартир в доме`,
        2gis.`Количество подъездов`
    FROM db1.2gis AS 2gis 
    WHERE 2gis.`Квартир в подъезде` != 0
    limit 100
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    SELECT
        2gis.upper(`Дом 1`)
        2gis.upper(`Город`)
        2gis.upper(`Улица 1`),
        2gis.`Квартир в подъезде`,
        2gis.`Квартир в доме`,
        2gis.`Количество подъездов`
    FROM db1.2gis AS 2gis 
    WHERE 2gis.`Квартир в подъезде` != 0
    limit 100
    """

ch.query_run(query_text)
```

_____

```python
query_text = """--sql
   CREATE TABLE db1.building_dir_partner
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
        `city_uuid` String
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
    REFRESH EVERY 1 DAY OFFSET 4 HOUR TO db1.entries_installation_points_dir_partner_ch AS
    SELECT
		DISTINCT
		substringIndex(full_address, ',', length(splitByChar(',', `full_address`)) - 1) AS building_address,
		`parent_uuid`,
		`region`,
		`country`,
		`city`,
		`city_uuid`
	FROM db1.entries_installation_points_dir_partner_ch
    """

ch.query_run(query_text)
```

```python

```

___
## Tools


### query

```python
query_text = """--sql
    SELECT
        *
    FROM db1.entries_installation_points_dir_partner_ch
    limit 100
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
