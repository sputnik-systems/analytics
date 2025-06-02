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

```python
import clickhouse_connect
import datetime
import os
import pytz
import pandas as pd
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

from ClickHouse_client import ClickHouse_client
ch = ClickHouse_client()
```

___

## Show all tables

```python
query_text = """--sql
    SHOW TABLES FROM db1
"""
ch.query_run(query_text)
```

____

## [[uk_addresses_partner]] 


```python
query_text = """--sql
    CREATE TABLE db1.uk_addresses_partner
    (
        `patrner_uuid_uk` String,
        `address_uuid`  String,
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/uk_addresses_partner/uk_addresses_partner.csv','CSVWithNames')
    """

ch.query_run(query_text)

```

```python
query_text = """--sql
    CREATE TABLE db1.uk_addresses_partner_ch
    (
        `patrner_uuid_uk` String,
        `address_uuid`  String,
    )
    ENGINE = MergeTree()
    ORDER BY patrner_uuid_uk
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.uk_addresses_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.uk_addresses_partner_ch AS
    SELECT
        *
    FROM db1.uk_addresses_partner
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    SELECT
        *
    FROM db1.uk_addresses_partner_ch
    LIMIT 2
    """

ch.query_run(query_text)
```

___

## [[users_st_partner]]


```python
query_text = """--sql
    CREATE TABLE db1.users_st_partner
    (
        `report_date` Date,
        `partner_uuid`  String,
        `last_logging` DateTime
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/users_st_partner/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.users_st_partner_ch
    (
        `report_date` Date,
        `partner_uuid`  String,
        `last_logging` DateTime
    )
    ENGINE = MergeTree()
    ORDER BY partner_uuid
    """

ch.query_run(query_text)

```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.users_st_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.users_st_partner_ch AS
    SELECT
        *
    FROM db1.users_st_partner
    """

ch.query_run(query_text)
```

___

## [[uk_addresses_st_partner]]

```python
# creating a table from s3

query_text = """--sql
    CREATE TABLE db1.uk_addresses_st_partner
    (
        `report_date` Date,
        `partner_uuid_uk`  String,
        `address_uuid` String
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/uk_addresses_st_partner/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
# creating a table for materialized view

query_text = """--sql
    CREATE TABLE db1.uk_addresses_st_partner_ch
    (
       `report_date` Date,
        `partner_uuid_uk` String,
        `address_uuid` String
    )
    ENGINE = MergeTree()
    ORDER BY partner_uuid_uk
    """

ch.query_run(query_text)
```

```python
# creating a materialized view

query_text = """--sql
    CREATE MATERIALIZED VIEW db1.uk_addresses_st_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.uk_addresses_st_partner_ch AS
    SELECT
        *
    FROM db1.uk_addresses_st_partner
    """

ch.query_run(query_text)
```

___

## [[uk_dir_partner]]

```python
# creating a table from s3

query_text = """--sql
    CREATE TABLE db1.uk_dir_partner
    (
        `id` Int32,
        `name`  String,
        `partner_uuid_uk` String,
        `created_at` String
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/uk_dir_partner/uk_dir_partner.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
# creating a table for materialized view

query_text = """--sql
    CREATE TABLE db1.uk_dir_partner_ch
    (
        `id` Int32,
        `name`  String,
        `partner_uuid_uk` String,
        `created_at` String
    )
    ENGINE = MergeTree()
    ORDER BY partner_uuid_uk
    """

ch.query_run(query_text)
```

```python
# creating a materialized view

query_text = """--sql
    CREATE MATERIALIZED VIEW db1.uk_dir_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.uk_dir_partner_ch AS
    SELECT
        *
    FROM db1.uk_dir_partner
    """

ch.query_run(query_text)
```

___

## [[uk_st_partner]]



```python
# creating a table from s3

query_text = """--sql
    CREATE TABLE db1.uk_st_partner
    (
        `partner_uuid` String,
        `business_partner_uuid`  String,
        `updated_at` DateTime,
        `partner_uk_email` String,
        `report_date` Date
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/uk_st_partner/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
# creating a table for materialized view

query_text = """--sql
    CREATE TABLE db1.uk_st_partner_ch
    (
        `partner_uuid` String,
        `business_partner_uuid` String,
        `updated_at` DateTime,
        `partner_uk_email` String,
        `report_date` Date
    )
    ENGINE = MergeTree()
    ORDER BY partner_uuid
    """

ch.query_run(query_text)
```

```python
# creating a materialized view

query_text = """--sql
    CREATE MATERIALIZED VIEW db1.uk_st_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.uk_st_partner_ch AS
    SELECT
        *
    FROM db1.uk_st_partner
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    SELECT
        *
    FROM db1.uk_st_partner_ch
    """

ch.query_run(query_text)
```
