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

## Show all tables

```python
query_text = """--sql
    SHOW TABLES FROM db1
"""
ch.query_run(query_text)
```

## uk_addresses_partner

```bash vscode={"languageId": "powershell"}
jupytext --sync adding_tables.ipynb 
```

<!-- #region vscode={"languageId": "powershell"} -->
[[uk_addresses_partner]],
[[uk_addresses_partner]],
[[uk_addresses_partner]] 
<!-- #endregion -->

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

## users_st_partner

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

```python
query_text = """--sql
   SELECT * FROM db1.users_st_partner_ch
   WHERE partner_uuid != ''
   LIMIT 10
    """

ch.query_run(query_text)
```

## uk_addresses_st_partner

```python
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
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.uk_addresses_st_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.uk_addresses_st_partner_ch AS
    SELECT
        *
    FROM db1.uk_addresses_st_partner
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
   SELECT * FROM db1.uk_addresses_st_partner_ch
   WHERE partner_uuid_uk != ''
   LIMIT 10
    """

ch.query_run(query_text)
```
