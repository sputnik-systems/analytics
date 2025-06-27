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
## Drop the Table

```python
query_text = """
    DROP TABLE db1.citizens_st_mobile_parquet_mv
    """
ch.query_run(query_text)
```

___

## Refreshing the data

```python
query_text = """
SYSTEM REFRESH VIEW db1.citizens_st_mobile_parquet_mv
"""

ch.query_run(query_text)
```

___
## Change refresh time

```python
query_text = """
ALTER TABLE db1.companies_st_partner_mv
MODIFY REFRESH EVERY 1 DAY OFFSET 4 HOUR RANDOMIZE FOR 1 HOUR;
"""

ch.query_run(query_text)
```

___

## Show all tables

```python
query_text = """--sql
    SHOW TABLES FROM db1
"""
df = ch.query_run(query_text)

```

____
[[uk_addresses_partner]]
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

___

## [[no_video_on_stream_mobile_st_asgard]]

```python
# creating a table from s3

query_text = """--sql
    CREATE TABLE db1.no_video_on_stream_mobile_st_asgard
    (
        `report_date` Date,
        `camera_uuid`  String,
        `count` Int32,
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/no_video_on_stream_mobile_st_asgard/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
# creating a table for materialized view

query_text = """--sql
    CREATE TABLE db1.no_video_on_stream_mobile_st_asgard_ch
    (
        `report_date` Date,
        `camera_uuid`  String,
        `count` Int32,
    )
    ENGINE = MergeTree()
    ORDER BY camera_uuid
    """

ch.query_run(query_text)
```

```python
# creating a materialized view

query_text = """--sql
    CREATE MATERIALIZED VIEW db1.no_video_on_stream_mobile_st_asgard_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.no_video_on_stream_mobile_st_asgard_ch AS
    SELECT
        *
    FROM db1.no_video_on_stream_mobile_st_asgard
    """

ch.query_run(query_text)
```

___

## [[entries_st_mobile]]

```python
# creating a table from s3

query_text = """--sql
    CREATE TABLE db1.entries_st_mobile
    (
        `report_date` Date,
        `address_uuid`  String,
        `partner_uuid` String,
        `monetization` String,
        `ble_available` String,
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/entries_st_mobile/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
# creating a table for materialized view

query_text = """--sql
    CREATE TABLE db1.entries_st_mobile_ch
    (
        `report_date` Date,
        `address_uuid`  String,
        `partner_uuid` String,
        `monetization` String,
        `ble_available` String,
    )
    ENGINE = MergeTree()
    ORDER BY address_uuid
    """

ch.query_run(query_text)
```

```python
# creating a materialized view

query_text = """--sql
    CREATE MATERIALIZED VIEW db1.entries_st_mobile_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.entries_st_mobile_ch AS
    SELECT
        *
    FROM db1.entries_st_mobile
    """

ch.query_run(query_text)
```

___

## [[citizen_payments_st_mobile]]

```python
# creating a table from s3

query_text = """--sql
    CREATE TABLE db1.citizen_payments_st_mobile
    (
        `report_date` Date,
        `citizen_id`  Int32,
        `state` String,
        `amount` Int32,
        `paid_at` DateTime,
        `refunded_at` DateTime,
        `refunded_amount` Int16,
        `from` String,
        `payment_id` Int32,
        `product_ids` String
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/citizen_payments_st_mobile/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
# creating a table for materialized view

query_text = """--sql
    CREATE TABLE db1.citizen_payments_st_mobile_ch
    (
        `report_date` Date,
        `citizen_id`  Int32,
        `state` String,
        `amount` Int32,
        `paid_at` DateTime,
        `refunded_at` DateTime,
        `refunded_amount` Int16,
        `from` String,
        `payment_id` Int32,
        `product_ids` String
    )
    ENGINE = MergeTree()
    ORDER BY citizen_id
    """

ch.query_run(query_text)
```

```python
# creating a materialized view

query_text = """--sql
    CREATE MATERIALIZED VIEW db1.citizen_payments_st_mobile_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.citizen_payments_st_mobile_ch AS
    SELECT
        *
    FROM db1.citizen_payments_st_mobile
    """

ch.query_run(query_text)
```

___

## [[sessions_st_mobile]]

```python
# creating a table from s3

query_text = """--sql
    CREATE TABLE db1.sessions_st_mobile
    (
        `report_date` Date,
        `citizen_id`  Int32,
        `created_at` DateTime,
        `last_use` DateTime,
        `updated_at` DateTime,
        `platform` String,
        `call_enabled` Int16,
        `app` String,
        `logged_in` Int16,
        `timezone` String
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/sessions_st_mobile/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
# creating a table for materialized view

query_text = """--sql
    CREATE TABLE db1.sessions_st_mobile_ch
    (
        `report_date` Date,
        `citizen_id`  Int32,
        `created_at` DateTime,
        `last_use` DateTime,
        `updated_at` DateTime,
        `platform` String,
        `call_enabled` Int16,
        `app` String,
        `logged_in` Int16,
        `timezone` String
    )
    ENGINE = MergeTree()
    ORDER BY citizen_id
    """

ch.query_run(query_text)
```

```python
# creating a materialized view

query_text = """--sql
    CREATE MATERIALIZED VIEW db1.sessions_st_mobile_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.sessions_st_mobile_ch AS
    SELECT
        *
    FROM db1.sessions_st_mobile
    """

ch.query_run(query_text)
```

___

## [[subscriptions_st_mobile]]

```python

query_text = """--sql
    CREATE TABLE db1.subscriptions_st_mobile_2
(
    `report_date` Date,
    `citizen_id` Int32,
    `state` String,
    `created_at` String,
    `subscribed_from` String,
    `auto_renew_status` Int16,
    `activated_at` String,
    `plan` String,
    `expires_date` String,
    `renew_stopped_at` String,
    `renew_failed_at` String,
    `started_from` String,
    `renew_fail_reason` String
)
    ENGINE = S3Queue('https://storage.yandexcloud.net/dwh-asgard/subscriptions_st_mobile/year=*/month=*/*.csv','CSVWithNames')
    SETTINGS mode = 'unordered'
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.subscriptions_st_mobile_ch_2
    (
    `report_date` Date,
    `citizen_id` Int32,
    `state` String,
    `created_at` String,
    `subscribed_from` String,
    `auto_renew_status` Int16,
    `activated_at` String,
    `plan` String,
    `expires_date` String,
    `renew_stopped_at` String,
    `renew_failed_at` String,
    `started_from` String,
    `renew_fail_reason` String
    )
    ENGINE = MergeTree()
    ORDER BY citizen_id
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.subscriptions_st_mobile_mv_2 TO db1.subscriptions_st_mobile_ch_2 AS
    SELECT
        *
    FROM db1.subscriptions_st_mobile
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    SELECT
        *
    FROM db1.subscriptions_st_mobile_ch
    WHERE report_date != '2025-06-25'
    """
ch.query_run(query_text)
```

```python
query_text = """--sql
    SELECT
        *
    FROM db1.subscriptions_st_mobile_ch_2
    """

ch.query_run(query_text)
```

```python
# creating a table from s3

query_text = """--sql
    CREATE TABLE db1.subscriptions_st_mobile
(
    `report_date` Date,
    `citizen_id` Int32,
    `state` String,
    `created_at` String,
    `subscribed_from` String,
    `auto_renew_status` Int16,
    `activated_at` String,
    `plan` String,
    `expires_date` String,
    `renew_stopped_at` String,
    `renew_failed_at` String,
    `started_from` String,
    `renew_fail_reason` String
)
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/subscriptions_st_mobile/year=*/month=*/*.csv','CSVWithNames');
    """

ch.query_run(query_text)

```

```python
# creating a table for materialized view

query_text = """--sql
    CREATE TABLE db1.subscriptions_st_mobile_ch
    (
    `report_date` Date,
    `citizen_id` Int32,
    `state` String,
    `created_at` String,
    `subscribed_from` String,
    `auto_renew_status` Int16,
    `activated_at` String,
    `plan` String,
    `expires_date` String,
    `renew_stopped_at` String,
    `renew_failed_at` String,
    `started_from` String,
    `renew_fail_reason` String
    )
    ENGINE = MergeTree()
    ORDER BY citizen_id
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.subscriptions_st_mobile_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.subscriptions_st_mobile_ch AS
    SELECT
        *
    FROM db1.subscriptions_st_mobile
    """

ch.query_run(query_text)
```

___

## [[citizens_st_mobile]]


```python
# creating a table from s3

query_text = """--sql
    CREATE TABLE db1.citizens_st_mobile
    (
    `report_date` Date,
    `citizen_id` Int32,
    `trial_available` Int32,
    `state` String,
    `flat_uuid` String,
    `address_uuid` String
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/citizens_st_mobile/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
# creating a table for materialized view

query_text = """--sql
    CREATE TABLE db1.citizens_st_mobile_ch
    (
    `report_date` Date,
    `citizen_id` Int32,
    `trial_available` Int32,
    `state` String,
    `flat_uuid` String,
    `address_uuid` String
    )
    ENGINE = MergeTree()
    ORDER BY report_date
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.citizens_st_mobile_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.citizens_st_mobile_ch AS
    SELECT
        *
    FROM db1.citizens_st_mobile
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    SELECT
        *
    FROM db1.citizens_st_mobile_ch
    where report_date = '2024-01-01'
    limit 100
    """

ch.query_run(query_text)
```

___

## [[citizens_st_mobile_parquet]]

```python
query_text = """--sql
    CREATE TABLE db1.citizens_st_mobile_parquet
    (
    `report_date` Date,
    `citizen_id` Int32,
    `trial_available` Int32,
    `state` String,
    `flat_uuid` String,
    `address_uuid` String
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/citizens_st_mobile_parquet/year=*/month=*/*.parquet','parquet')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.citizens_st_mobile_parquet_ch
    (
    `report_date` Date,
    `citizen_id` Int32,
    `trial_available` Int32,
    `state` String,
    `flat_uuid` String,
    `address_uuid` String
    )
    ENGINE = MergeTree()
    ORDER BY report_date
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.citizens_st_mobile_parquet_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR 30 MINUTE RANDOMIZE FOR 1 HOUR TO db1.citizens_st_mobile_parquet_ch AS
    SELECT
        *
    FROM db1.citizens_st_mobile_parquet
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    SELECT
        *
    FROM db1.citizens_st_mobile_parquet_ch
    WHERE report_date = '2025-05-29'
    limit 100
    """

ch.query_run(query_text)
```

```python
query_text = """
    DROP TABLE db1.citizens_st_mobile_parquet
    """
ch.query_run(query_text)
```

```python
query_text = """
    SYSTEM T TABLE db1.citizens_st_mobile_parquet
    """
ch.query_run(query_text)
```

___

## [[citizens_dir_mobile]]


```python
# creating a table from s3

query_text = """--sql
    CREATE TABLE db1.citizens_dir_mobile
    (
    `created_at` String,
    `activated_at` String,
    `localization` String,
    `flat_uuid` String,
    `address_uuid` String,
    `citizen_id` Int32
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/citizens_dir_mobile/citizens_dir_mobile.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
# creating a table for materialized view

query_text = """--sql
    CREATE TABLE db1.citizens_dir_mobile_ch
    (
    `created_at` String,
    `activated_at` String,
    `localization` String,
    `flat_uuid` String,
    `address_uuid` String,
    `citizen_id` Int32
    )
    ENGINE = MergeTree()
    ORDER BY citizen_id
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.citizens_dir_mobile_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.citizens_dir_mobile_ch AS
    SELECT
        *
    FROM db1.citizens_dir_mobile
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    SELECT
        *
    FROM db1.citizens_dir_mobile_ch
    limit 100
    """

ch.query_run(query_text)
```

___

## [[citizen_payments_dir_mobile]]


```python
# creating a table from s3

query_text = """--sql
    CREATE TABLE db1.citizen_payments_dir_mobile
    (
    `citizen_id` Int32,
    `payment_id` Int64,
    `created_at` DateTime,
    `state` String,
    `amount` Int64,
    `paid_at` DateTime,
    `refunded_at` DateTime,
    `refunded_amount` Int64,
    `from` String,
    `product_ids` String
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/citizen_payments_dir_mobile/citizen_payments_dir_mobile.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.citizen_payments_dir_mobile_ch
    (
    `citizen_id` Int32,
    `payment_id` Int64,
    `created_at` DateTime,
    `state` String,
    `amount` Int64,
    `paid_at` DateTime,
    `refunded_at` DateTime,
    `refunded_amount` Int64,
    `from` String,
    `product_ids` String
    )
    ENGINE = MergeTree()
    ORDER BY citizen_id
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.citizen_payments_dir_mobile_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.citizen_payments_dir_mobile_ch AS
    SELECT
        *
    FROM db1.citizen_payments_dir_mobile
    """

ch.query_run(query_text)
```

```python

```

```python
query_text = """--sql
    SELECT
        *
    FROM db1.citizen_payments_dir_mobile_ch
    limit 100
    """

ch.query_run(query_text)
```

___

## [[flats_dir_partner]]


```python
# creating a table from s3

query_text = """--sql
    CREATE TABLE db1.flats_dir_partner
    (
    `created_at` DateTime,
    `number` Int32,
    `address_uuid` String,
    `installation_point_id` Int64,
    `flat_uuid` String
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/flats_dir_partner/flats_dir_partner.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
   CREATE TABLE db1.flats_dir_partner_ch
    (
    `created_at` DateTime,
    `number` Int32,
    `address_uuid` String,
    `installation_point_id` Int64,
    `flat_uuid` String
    )
    ENGINE = MergeTree()
    ORDER BY flat_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.flats_dir_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.flats_dir_partner_ch AS
    SELECT
        *
    FROM db1.flats_dir_partner
    """

ch.query_run(query_text)
```

___

## [[flats_st_partner]]

```python
# creating a table from s3

query_text = """--sql
    CREATE TABLE db1.flats_st_partner
    (
    `report_date` Date,
    `flat_uuid` String,
    `call_blocked` Int16,
    `blocked` Int16,
    `deleted` Int16
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/flats_st_partner/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
   CREATE TABLE db1.flats_st_partner_ch
    (
    `report_date` Date,
    `flat_uuid` String,
    `call_blocked` Int16,
    `blocked` Int16,
    `deleted` Int16
    )
    ENGINE = MergeTree()
    ORDER BY flat_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.flats_st_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.flats_st_partner_ch AS
    SELECT
        *
    FROM db1.flats_st_partner
    """

ch.query_run(query_text)
```

___

## [[entries_installation_points_dir_partner]]

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
        `city` String
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/entries_installation_points_dir_partner/entries_installation_points_dir_partner.csv','CSVWithNames');
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
        `flats_count` Int16,
        `address_uuid` String,
        `parent_uuid` String,
        `partner_uuid` String,
        `installation_point_id` Int64,
        `region` String,
        `country` String,
        `city` String
    )
    ENGINE = MergeTree()
    ORDER BY installation_point_id
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.entries_installation_points_dir_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.entries_installation_points_dir_partner_ch AS
    SELECT
        *
    FROM db1.entries_installation_points_dir_partner
    """

ch.query_run(query_text)
```

___

## [[installation_point_st_partner]]

```python
# creating a table from s3

query_text = """
    CREATE TABLE db1.installation_point_st_partner
    (
        `report_date` Date,
        `installation_point_id` Int64,
        `digital_keys_count` String,
        `device_keys_count` String,
        `monetization_is_allowed` Int16,
        `monetization` Int16,
        `partner_uuid` String
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/installation_point_st_partner/year=*/month=*/*.csv','CSVWithNames')
    PARTITION BY report_date;
    """

ch.query_run(query_text)
```

```python
query_text = """--sql 
    CREATE TABLE db1.installation_point_st_partner_ch
    (
        `report_date` Date,
        `installation_point_id` Int64,
        `digital_keys_count` String,
        `device_keys_count` String,
        `monetization_is_allowed` Int16,
        `monetization` Int16,
        `partner_uuid` String
    )
    ENGINE = MergeTree()
    ORDER BY installation_point_id
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.installation_point_st_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.installation_point_st_partner_ch AS
    SELECT
        *
    FROM db1.installation_point_st_partner
    """

ch.query_run(query_text)
```

___

## [[buildings_st_partner]]

```python
# creating a table from s3

query_text = """--sql 
    CREATE TABLE db1.buildings_st_partner
    (
        `parent_uuid` String,
        `installation_point_id` Int64,
        `report_date` Date
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/buildings_st_partner/year=*/month=*/*.csv','CSVWithNames')
    PARTITION BY report_date
    """

ch.query_run(query_text)
```

```python
query_text = """--sql 
    CREATE TABLE db1.buildings_st_partner_ch
    (
        `parent_uuid` String,
        `installation_point_id` Int64,
        `report_date` Date
    )
    ENGINE = MergeTree()
    ORDER BY installation_point_id
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.buildings_st_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.buildings_st_partner_ch AS
    SELECT
        *
    FROM db1.buildings_st_partner
    """

ch.query_run(query_text)
```

```python

```

___

## [[gates_st_partner]]

```python
# creating a table from s3

query_text = """--sql 
    CREATE TABLE db1.gates_st_partner
    (
        `parent_uuid` String,
        `installation_point_id` Int64,
        `report_date` Date
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/gates_st_partner/year=*/month=*/*.csv','CSVWithNames')
    PARTITION BY report_date
    """

ch.query_run(query_text)
```

```python
query_text = """--sql 
    CREATE TABLE db1.gates_st_partner_ch
    (
        `parent_uuid` String,
        `installation_point_id` Int64,
        `report_date` Date
    )
    ENGINE = MergeTree()
    ORDER BY installation_point_id
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.gates_st_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.gates_st_partner_ch AS
    SELECT
        *
    FROM db1.gates_st_partner
    """

ch.query_run(query_text)
```

___

## [[accruals_dir_partner]]

```python
# creating a table from s3

query_text = """--sql 
    CREATE TABLE db1.accruals_dir_partner
    (
        `partner_uuid` String,
        `created_at` Date,
        `state` String,
        `amount` Float64,
        `type` String,
        `comment` String
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/accruals_dir_partner/accruals_dir_partner.csv','CSVWithNames')
    PARTITION BY report_date
    """

ch.query_run(query_text)
```

```python
query_text = """
    CREATE TABLE db1.accruals_dir_partner_ch
    (
        `partner_uuid` String,
        `created_at` Date,
        `state` String,
        `amount` Float64,
        `type` String,
        `comment` String
    )
    ENGINE = MergeTree()
    ORDER BY partner_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.accruals_dir_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.accruals_dir_partner_ch AS
    SELECT
        *
    FROM db1.accruals_dir_partner
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    SELECT
        *
    FROM db1.accruals_dir_partner_ch
    limit 100
    """

ch.query_run(query_text)
```

___

## [[billing_orders_dir_partner]]

```python
# creating a table from s3

query_text = """--sql 
    CREATE TABLE db1.billing_orders_dir_partner
    (
        `created_at` DateTime,
        `state` String,
        `service` String,
        `cost` Float64,
        `total` Float64,
        `count` Int64,
        `billing_account_id` String,
        `partner_uuid` String
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/billing_orders_dir_partner/billing_orders_dir_partner.csv','CSVWithNames')
    PARTITION BY report_date
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.billing_orders_dir_partner_ch
    (
        `created_at` DateTime,
        `state` String,
        `service` String,
        `cost` Float64,
        `total` Float64,
        `count` Int64,
        `billing_account_id` String,
        `partner_uuid` String
    )
    ENGINE = MergeTree()
    ORDER BY partner_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.billing_orders_dir_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.billing_orders_dir_partner_ch AS
    SELECT
        *
    FROM db1.billing_orders_dir_partner
    """

ch.query_run(query_text)
```

```python
query_text = """--sql

SELECT
    *
FROM db1.billing_orders_dir_partner_ch
limit 100

"""

ch.query_run(query_text)
```

___

## [[billing_orders_devices_dir_partner]]



```python
# creating a table from s3

query_text = """--sql 
    CREATE TABLE db1.billing_orders_devices_st_partner
    (   
        `billing_account_id` Int64,
        `cost` Float64,
        `count` Int64,
        `created_at` DateTime,
        `device_type` String,
        `device_uuid` String,
        `partner_uuid` String,
        `report_date` Date,
        `service` String,
        `state` String,
        `total` Float64
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/billing_orders_devices_dir_partner/billing_orders_devices_dir_partner.csv','CSVWithNames')
    PARTITION BY billing_account_id
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.billing_orders_devices_st_partner_ch
    (   
        `billing_account_id` Int64,
        `cost` Float64,
        `count` Int64,
        `created_at` DateTime,
        `device_type` String,
        `device_uuid` String,
        `partner_uuid` String,
        `report_date` Date,
        `service` String,
        `state` String,
        `total` Float64
    )
    ENGINE = MergeTree()
    ORDER BY partner_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.billing_orders_devices_st_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.billing_orders_devices_st_partner_ch AS
    SELECT
        *
    FROM db1.billing_orders_devices_st_partner
    """

ch.query_run(query_text)
```

```python
query_text = """SELECT
    *
FROM db1.billing_orders_devices_st_partner_ch
limit 100

"""

ch.query_run(query_text)
```

___

## [[companies_dir_partner]]

```python
# creating a table from s3

query_text = """--sql 
    CREATE TABLE db1.companies_dir_partner
    (   
        `company_name` String,
        `partner_lk` String,
        `registration_date` Date,
        `partner_uuid` String,
        `tin` String,
        `kpp` String
    )
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/companies_dir_partner/companies_dir_partner.csv','CSVWithNames')
    PARTITION BY partner_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.companies_dir_partner_ch
    (   
        `company_name` String,
        `partner_lk` String,
        `registration_date` String,
        `partner_uuid` String,
        `tin` String,
        `kpp` String
    )
    ENGINE = MergeTree()
    ORDER BY partner_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.companies_dir_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.companies_dir_partner_ch AS
    SELECT
        *
    FROM db1.companies_dir_partner
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.companies_dir_partner_ch
limit 100

"""

ch.query_run(query_text)
```

___

## [[companies_st_partner]]

```python
# creating a table from s3

query_text = """--sql 
CREATE TABLE db1.companies_st_partner
(
    `report_date` Date,
    `partner_uuid` String,
    `is_blocked` Int16,
    `pro_subs` Int16,
    `enterprise_subs` Int16,
    `billing_pro` Int16,
    `enterprise_not_paid` Int16,
    `enterprise_test` Int16,
    `balance` Float64,
    `tariff` String,
    `kz_pro` Int16
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/companies_st_partner/year=*/month=*/*.csv', 'CSVWithNames')
PARTITION BY partner_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.companies_st_partner_ch
(
    `report_date` Date,
    `partner_uuid` String,
    `is_blocked` Int16,
    `pro_subs` Int16,
    `enterprise_subs` Int16,
    `billing_pro` Int16,
    `enterprise_not_paid` Int16,
    `enterprise_test` Int16,
    `balance` Float64,
    `tariff` String,
    `kz_pro` Int16
)
    ENGINE = MergeTree()
    ORDER BY partner_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.companies_st_partner_mv REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.companies_st_partner_ch AS
    SELECT
       *
    FROM db1.companies_st_partner
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.companies_st_partner_ch
ORDER BY report_date DESC
limit 10
"""
ch.query_run(query_text)
```

```python
query_text = """
DROP TABLE db1.companies_st_partner
"""

ch.query_run(query_text)
```

```python
query_text = """
SYSTEM REFRESH VIEW db1.companies_st_partner_mv
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.companies_st_partner_ch_test
(
    `report_date` Date,
    `partner_uuid` String,
    `is_blocked` Int16,
    `pro_subs` Int16,
    `enterprise_subs` Int16,
    `billing_pro` Int16,
    `enterprise_not_paid` Int16,
    `enterprise_test` Int16,
    `balance` Float64,
    `tariff` String,
    `kz_pro` Int16,
    `tariff_full` String
)
    ENGINE = MergeTree()
    ORDER BY partner_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.companies_st_partner_mv_test TO db1.companies_st_partner_ch_test AS
    SELECT
        *
    FROM db1.companies_st_partner_ch
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.companies_st_partner_ch_test
order by report_date DESC
limit 100

"""

ch.query_run(query_text)
```

___

## [[service_history_dir_partner]]

```python
# creating a table from s3

query_text = """--sql 
    CREATE TABLE db1.service_history_dir_partner
(
    `action` String,
    `actor_identifier` String,
    `actor_name` String,
    `actor_type` String,
    `comment` String,
    `created_at` DateTime,
    `id` Int64,
    `result` String,
    `service_identfier` String,
    `subject_type` String,
    `updated_at` DateTime,
    `partner_uuid` String
)
    ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/service_history_dir_partner/*.csv','CSVWithNames')
    PARTITION BY partner_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.service_history_dir_partner_ch
(
    `action` String,
    `actor_identifier` String,
    `actor_name` String,
    `actor_type` String,
    `comment` String,
    `created_at` DateTime,
    `id` Int64,
    `result` String,
    `service_identfier` String,
    `subject_type` String,
    `updated_at` DateTime,
    `partner_uuid` String
)
    ENGINE = MergeTree()
    ORDER BY partner_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.service_history_dir_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.service_history_dir_partner_ch AS
    SELECT
        *
    FROM db1.service_history_dir_partner
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.service_history_dir_partner_ch
limit 100

"""

ch.query_run(query_text)
```

```python

```

___

## [[cameras_st_partner]]

```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.cameras_st_partner
(
    `report_date` Date,
    `service_partner_uuid` String,
    `partner_uuid` String,
    `intercom_uuid` String,
    `installation_point_id` Int64,
    `camera_uuid` String,
    `camera_dvr_depth` Int32,
    `archive_from_partner` String,
    `included_by` String,
    `included_at` DateTime,
    `disabled_by` String,
    `disabled_at` DateTime
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/cameras_st_partner/year=*/month=*/*.csv',
 'CSVWithNames')
PARTITION BY report_date
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.cameras_st_partner_ch
(
    `report_date` Date,
    `service_partner_uuid` String,
    `partner_uuid` String,
    `intercom_uuid` String,
    `installation_point_id` Int64,
    `camera_uuid` String,
    `camera_dvr_depth` Int32,
    `archive_from_partner` String,
    `included_by` String,
    `included_at` DateTime,
    `disabled_by` String,
    `disabled_at` DateTime
)
    ENGINE = MergeTree()
    ORDER BY report_date
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.cameras_st_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.cameras_st_partner_ch AS
    SELECT
        *
    FROM db1.cameras_st_partner
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.cameras_st_partner_ch
order by report_date DESC
limit 100

"""

ch.query_run(query_text)
```

___

## [[cameras_dir_partner]]

```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.cameras_dir_partner
(
    `serial_number` String,
    `service_partner_uuid` String,
    `partner_uuid` String,
    `intercom_uuid` String,
    `installation_point_id` Int64,
    `camera_uuid` String,
    `foreign_camera` Int16
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/cameras_dir_partner/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.cameras_dir_partner_ch
(
    `serial_number` String,
    `service_partner_uuid` String,
    `partner_uuid` String,
    `intercom_uuid` String,
    `installation_point_id` Int64,
    `camera_uuid` String,
    `foreign_camera` Int16
)
    ENGINE = MergeTree()
    ORDER BY partner_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.cameras_dir_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.cameras_dir_partner_ch AS
    SELECT
        *
    FROM db1.cameras_dir_partner
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.cameras_dir_partner_ch
limit 100

"""

ch.query_run(query_text)
```

___

## [[intercoms_st_partner]]

```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.intercoms_st_partner
(
    `report_date` Date,
    `installation_date` DateTime,
    `service_partner_uuid` String,
    `partner_uuid` String,
    `intercom_uuid` String,
    `installation_point_id` Int64,
    `second_device_type` String,
    `second_device_state` Int16,
    `second_device_created_at` String,
    `model_identifier` String
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/intercoms_st_partner/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.intercoms_st_partner_ch
(
    `report_date` Date,
    `installation_date` DateTime,
    `service_partner_uuid` String,
    `partner_uuid` String,
    `intercom_uuid` String,
    `installation_point_id` Int64,
    `second_device_type` String,
    `second_device_state` Int16,
    `second_device_created_at` String,
    `model_identifier` String
)
    ENGINE = MergeTree()
    ORDER BY intercom_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.intercoms_st_partner_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.intercoms_st_partner_ch AS
    SELECT
        *
    FROM db1.intercoms_st_partner
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.intercoms_st_partner_ch
limit 100

"""

ch.query_run(query_text)
```

___

## [[intercoms_dir_partner]]

```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.intercoms_dir_partner
(
    `installation_date` DateTime,
    `service_partner_uuid` String,
    `partner_uuid` String,
    `intercom_uuid` String,
    `installation_point_id` Int64,
    `second_device_type` String,
    `second_device_state` Int16,
    `second_device_created_at` String,
    `model_identifier` String
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/intercoms_dir_partner/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.intercoms_dir_partner_ch
(
    `installation_date` DateTime,
    `service_partner_uuid` String,
    `partner_uuid` String,
    `intercom_uuid` String,
    `installation_point_id` Int64,
    `second_device_type` String,
    `second_device_state` Int16,
    `second_device_created_at` String,
    `model_identifier` String
)
    ENGINE = MergeTree()
    ORDER BY intercom_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.intercoms_dir_partner_mv 
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.intercoms_dir_partner_ch AS
    SELECT
        *
    FROM db1.intercoms_dir_partner
    """

ch.query_run(query_text)
```

```python
query_text = """
    DROP TABLE db1.intercoms_dir_partner_mv_test
    """
ch.query_run(query_text)

```

```python
query_text = """
SYSTEM REFRESH VIEW db1.citizens_st_mobile_parquet_mv
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.intercoms_dir_partner_ch
limit 1

"""

ch.query_run(query_text)
```

```python
query_text = """--sql
INSERT INTO db1.intercoms_dir_partner_ch
SELECT *
FROM db1.intercoms_dir_partner;
"""
ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.intercoms_dir_partner_ch_test
(
    `installation_date` DateTime,
    `service_partner_uuid` String,
    `partner_uuid` String,
    `intercom_uuid` String,
    `installation_point_id` Int64,
    `second_device_type` String,
    `second_device_state` Int16,
    `second_device_created_at` String,
    `model_identifier` String
)
    ENGINE = MergeTree()
    ORDER BY intercom_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.intercoms_dir_partner_mv_test TO db1.intercoms_dir_partner_ch_test AS
    SELECT
        *
    FROM db1.intercoms_dir_partner_ch
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.intercoms_dir_partner_ch_test
limit 1

"""
ch.query_run(query_text)
```

___

## [[cameras_dir_asgard]]

```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.cameras_dir_asgard
(
    `camera_serial` String,
    `intercom_uuid` String,
    `camera_uuid` String,
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/cameras_dir_asgard/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.cameras_dir_asgard_ch
(
    `camera_serial` String,
    `intercom_uuid` String,
    `camera_uuid` String,
)
    ENGINE = MergeTree()
    ORDER BY intercom_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.cameras_dir_asgard_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.cameras_dir_asgard_ch AS
    SELECT
        *
    FROM db1.cameras_dir_asgard
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.cameras_dir_asgard_ch
limit 100

"""

ch.query_run(query_text)
```

____

## [[cameras_st_asgard]]


```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.cameras_st_asgard
(
    `report_date` Date,
    `camera_uuid` String,
    `camera_dvr_depth` Int16,
    `camera_is_permanent_stream` Int16,
    `camera_with_intercom` Int16,
    `partner_uuid` String,
    `camera_fw_version` String,
    `camera_model` String,
    `camera_streamer` String
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/cameras_st_asgard/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.cameras_st_asgard_ch
(
    `report_date` Date,
    `camera_uuid` String,
    `camera_dvr_depth` Int16,
    `camera_is_permanent_stream` Int16,
    `camera_with_intercom` Int16,
    `partner_uuid` String,
    `camera_fw_version` String,
    `camera_model` String,
    `camera_streamer` String
)
    ENGINE = MergeTree()
    ORDER BY camera_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.cameras_st_asgard_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.cameras_st_asgard_ch AS
    SELECT
        *
    FROM db1.cameras_st_asgard
    WHERE partner_uuid not like '%main:tokens:%'
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.cameras_st_asgard_ch
limit 100

"""

ch.query_run(query_text)
```

____

## [[intercoms_dir_asgard]]


```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.intercoms_dir_asgard
(
    `created_at` Date,
    `first_online_at` Date,
    `first_open_door_at` Date,
    `flat_range` Int16,
    `flat_count` Int16,
    `hardware_version` String,
    `motherboard_id` String,
    `partner_uuid` String,
    `intercom_uuid` String
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/intercoms_dir_asgard/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.intercoms_dir_asgard_ch
(
    `created_at` Date,
    `first_online_at` Date,
    `first_open_door_at` Date,
    `flat_range` Int16,
    `flat_count` Int16,
    `hardware_version` String,
    `motherboard_id` String,
    `partner_uuid` String,
    `intercom_uuid` String
)
    ENGINE = MergeTree()
    ORDER BY intercom_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.intercoms_dir_asgard_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.intercoms_dir_asgard_ch AS
    SELECT
        *
    FROM db1.intercoms_dir_asgard
    WHERE partner_uuid not like '%main:tokens:%'
    """

ch.query_run(query_text)
```

```python

```

```python
query_text = """--sql
SELECT
    *
FROM db1.intercoms_dir_asgard_ch
limit 100

"""

ch.query_run(query_text)
```

____

## [[intercoms_st_asgard]]


```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.intercoms_st_asgard
(
    `report_date` Date,
    `intercom_uuid` String,
    `is_online` Int16,
    `last_online` DateTime,
    `last_offline` DateTime,
    `software_version` String,
    `digital_keys_count` Int16,
    `device_keys_count` Int16,
    `partner_uuid` String,
    `flat_range` String
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/intercoms_st_asgard/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.intercoms_st_asgard_ch
(
    `report_date` Date,
    `intercom_uuid` String,
    `is_online` Int16,
    `last_online` DateTime,
    `last_offline` DateTime,
    `software_version` String,
    `digital_keys_count` Int16,
    `device_keys_count` Int16,
    `partner_uuid` String,
    `flat_range` String
)
    ENGINE = MergeTree()
    ORDER BY intercom_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.intercoms_st_asgard_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.intercoms_st_asgard_ch AS
    SELECT
        *
    FROM db1.intercoms_st_asgard
    WHERE partner_uuid not like '%main:tokens:%'
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.intercoms_st_asgard_ch
limit 100

"""

ch.query_run(query_text)
```

____

## [[flussonic_stats_st_asgard]]


```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.flussonic_stats_st_asgard
(
    `report_date` DateTime,
    `stream_name` String,
    `disabled` String,
    `status` String,
    `alive` String,
    `lifetime` Int64,
    `bitrate` Int32,
    `agent_status` String,
    `server` String,
    `dvr_depth` Int16,
    `dvr_depth_minute` Int64
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/flussonic_stats_st_asgard/year=*/month=*/day=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.flussonic_stats_st_asgard_ch
(
    `report_date` DateTime,
    `stream_name` String,
    `disabled` String,
    `status` String,
    `alive` String,
    `lifetime` Int64,
    `bitrate` Int32,
    `agent_status` String,
    `server` String,
    `dvr_depth` Int16,
    `dvr_depth_minute` Int64
)
    ENGINE = MergeTree()
    ORDER BY stream_name
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.flussonic_stats_st_asgard_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.flussonic_stats_st_asgard_ch AS
    SELECT
        *
    FROM db1.flussonic_stats_st_asgard
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.flussonic_stats_st_asgard_ch
limit 100

"""

ch.query_run(query_text)
```

____

## [[cameras_daily_percentage_online_st_asgard]]


```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.cameras_daily_percentage_online_st_asgard
(
    `report_date` Date,
    `camera_uuid` String,
    `onlinePercent` Int16
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/cameras_daily_percentage_online_st_asgard/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.cameras_daily_percentage_online_st_asgard_ch
(
    `report_date` Date,
    `camera_uuid` String,
    `onlinePercent` Int16
)
    ENGINE = MergeTree()
    ORDER BY camera_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.cameras_daily_percentage_online_st_asgard_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.cameras_daily_percentage_online_st_asgard_ch AS
    SELECT
        *
    FROM db1.cameras_daily_percentage_online_st_asgard
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.cameras_daily_percentage_online_st_asgard_ch
limit 100

"""

ch.query_run(query_text)
```

____

## [[reconnects_intercoms_st_asgard]]


```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.reconnects_intercoms_st_asgard
(
    `report_date` Date,
    `intercom_uuid` String,
    `count` Int16
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/reconnects_intercoms_st_asgard/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.reconnects_intercoms_st_asgard_ch
(
    `report_date` Date,
    `intercom_uuid` String,
    `count` Int16
)
    ENGINE = MergeTree()
    ORDER BY intercom_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.reconnects_intercoms_st_asgard_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.reconnects_intercoms_st_asgard_ch AS
    SELECT
        *
    FROM db1.reconnects_intercoms_st_asgard
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.reconnects_intercoms_st_asgard_ch
limit 100

"""

ch.query_run(query_text)
```

____

## [[intercoms_daily_percentage_online_st_asgard]]


```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.intercoms_daily_percentage_online_st_asgard
(
    `report_date` Date,
    `intercom_uuid` String,
    `onlinePercent` Int16
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/intercoms_daily_percentage_online_st_asgard/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.intercoms_daily_percentage_online_st_asgard_ch
(
    `report_date` Date,
    `intercom_uuid` String,
    `onlinePercent` Int16
)
    ENGINE = MergeTree()
    ORDER BY intercom_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.intercoms_daily_percentage_online_st_asgard_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.intercoms_daily_percentage_online_st_asgard_ch AS
    SELECT
        *
    FROM db1.intercoms_daily_percentage_online_st_asgard
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.intercoms_daily_percentage_online_st_asgard_ch
ORDER BY report_date DESC
limit 100

"""

ch.query_run(query_text)
```




____

## [[opendoor_types_mobile_st_asgard]]


```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.opendoor_types_mobile_st_asgard
(
    `report_date` Date,
    `opendoor_type` String,
    `count` Int16
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/opendoor_types_mobile_st_asgard/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.opendoor_types_mobile_st_asgard_ch
(
    `report_date` Date,
    `opendoor_type` String,
    `count` Int16
)
    ENGINE = MergeTree()
    ORDER BY opendoor_type
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.opendoor_types_mobile_st_asgard_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.opendoor_types_mobile_st_asgard_ch AS
    SELECT
        *
    FROM db1.opendoor_types_mobile_st_asgard
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.opendoor_types_mobile_st_asgard_ch
ORDER BY report_date DESC
limit 100

"""

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    DISTINCT
    opendoor_type
FROM db1.opendoor_types_mobile_st_asgard_ch
ORDER BY report_date DESC
limit 100

"""

ch.query_run(query_text)
```

____

## [[hex_metrics_parquet_asgard]]


```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.hex_metrics_parquet_asgard
(
    `report_date` Date,
    `intercom_uuid` String,
    `key_hex` String,
    `count` Int64
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/hex_metrics_parquet_asgard/year=*/month=*/*.parquet','Parquet')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.hex_metrics_parquet_asgard_ch
(
    `report_date` Date,
    `intercom_uuid` String,
    `key_hex` String,
    `count` Int64
)
    ENGINE = MergeTree()
    ORDER BY intercom_uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.hex_metrics_parquet_asgard_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.hex_metrics_parquet_asgard_ch AS
    SELECT
        *
    FROM db1.hex_metrics_parquet_asgard
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.hex_metrics_parquet_asgard_ch
WHERE report_date = '2025-03-01'
ORDER BY report_date DESC
limit 100

"""

ch.query_run(query_text)
```

____

## [[metrics_all_intercoms_asgard]]


```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.metrics_all_intercoms_asgard
(
    `report_date` Date,
    `uuid` String,
    `call_stop_type_finish_handset` Int32,
    `ring_type_ring_cloud` Int32,
    `talk_open_door_type_analog` Int32,
    `call_stop_type_cancel_cloud` Int32,
    `key_state_valid` Int32,
    `ring_cluster_error_type_entrance_offline` Int32,
    `connection` Int32,
    `ring_type_ring_info` Int32,
    `ring_error_type_cancel` Int32,
    `call_success_true` Int32,
    `digital_key_success_false` Int32,
    `key_state_invalid` Int32,
    `ring_cluster_error_type_wrong_flat` Int32,
    `ring_error_type_cancel_handset` Int32,
    `talk_type_sip` Int32,
    `call_stop_type_cancel_button` Int32,
    `call_success_false` Int32,
    `open_door_type_api` Int32,
    `call_stop_type_speak_timeout` Int32,
    `ring_type_analog` Int32,
    `talk_type_analog`  Int32,
    `digital_key_success_true` Int32,
    `open_door_type_analog` Int32,
    `ring_type_sip` Int32,
    `talk_type_flat` Int32,
    `key_state_auth_err` Int32,
    `open_door_type_DTMF` Int32,
    `ring_cluster` Int32,
    `talk_open_door_type_api` Int32
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/metrics_all_intercoms_asgard/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.metrics_all_intercoms_asgard_ch
(
    `report_date` Date,
    `uuid` String,
    `call_stop_type_finish_handset` Int32,
    `ring_type_ring_cloud` Int32,
    `talk_open_door_type_analog` Int32,
    `call_stop_type_cancel_cloud` Int32,
    `key_state_valid` Int32,
    `ring_cluster_error_type_entrance_offline` Int32,
    `connection` Int32,
    `ring_type_ring_info` Int32,
    `ring_error_type_cancel` Int32,
    `call_success_true` Int32,
    `digital_key_success_false` Int32,
    `key_state_invalid` Int32,
    `ring_cluster_error_type_wrong_flat` Int32,
    `ring_error_type_cancel_handset` Int32,
    `talk_type_sip` Int32,
    `call_stop_type_cancel_button` Int32,
    `call_success_false` Int32,
    `open_door_type_api` Int32,
    `call_stop_type_speak_timeout` Int32,
    `ring_type_analog` Int32,
    `talk_type_analog`  Int32,
    `digital_key_success_true` Int32,
    `open_door_type_analog` Int32,
    `ring_type_sip` Int32,
    `talk_type_flat` Int32,
    `key_state_auth_err` Int32,
    `open_door_type_DTMF` Int32,
    `ring_cluster` Int32,
    `talk_open_door_type_api` Int32
)
    ENGINE = MergeTree()
    ORDER BY uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.metrics_all_intercoms_asgard_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.metrics_all_intercoms_asgard_ch AS
    SELECT
        *
    FROM db1.metrics_all_intercoms_asgard
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.metrics_all_intercoms_asgard_ch
WHERE report_date = '2025-03-01'
ORDER BY report_date DESC
limit 100

"""

ch.query_run(query_text)
```

```python

```

____

## [[metrics_asgard]]


```python
# creating a table from s3

query_text = """--sql 
   CREATE TABLE db1.metrics_asgard
(
    `report_date` Date,
    `uuid` String,
    `call_stop_type_finish_handset` Int32,
    `ring_type_ring_cloud` Int32,
    `talk_open_door_type_analog` Int32,
    `call_stop_type_cancel_cloud` Int32,
    `key_state_valid` Int32,
    `ring_cluster_error_type_entrance_offline` Int32,
    `connection` Int32,
    `ring_type_ring_info` Int32,
    `ring_error_type_cancel` Int32,
    `call_success_true` Int32,
    `digital_key_success_false` Int32,
    `key_state_invalid` Int32,
    `ring_cluster_error_type_wrong_flat` Int32,
    `ring_error_type_cancel_handset` Int32,
    `talk_type_sip` Int32,
    `call_stop_type_cancel_button` Int32,
    `call_success_false` Int32,
    `open_door_type_api` Int32,
    `call_stop_type_speak_timeout` Int32,
    `ring_type_analog` Int32,
    `talk_type_analog`  Int32,
    `digital_key_success_true` Int32,
    `open_door_type_analog` Int32,
    `ring_type_sip` Int32,
    `talk_type_flat` Int32,
    `key_state_auth_err` Int32,
    `open_door_type_DTMF` Int32,
    `ring_cluster` Int32,
    `talk_open_door_type_api` Int32
)
ENGINE = S3('https://storage.yandexcloud.net/dwh-asgard/metrics_asgard/year=*/month=*/*.csv','CSVWithNames')
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE TABLE db1.metrics_asgard_ch
(
    `report_date` Date,
    `uuid` String,
    `call_stop_type_finish_handset` Int32,
    `ring_type_ring_cloud` Int32,
    `talk_open_door_type_analog` Int32,
    `call_stop_type_cancel_cloud` Int32,
    `key_state_valid` Int32,
    `ring_cluster_error_type_entrance_offline` Int32,
    `connection` Int32,
    `ring_type_ring_info` Int32,
    `ring_error_type_cancel` Int32,
    `call_success_true` Int32,
    `digital_key_success_false` Int32,
    `key_state_invalid` Int32,
    `ring_cluster_error_type_wrong_flat` Int32,
    `ring_error_type_cancel_handset` Int32,
    `talk_type_sip` Int32,
    `call_stop_type_cancel_button` Int32,
    `call_success_false` Int32,
    `open_door_type_api` Int32,
    `call_stop_type_speak_timeout` Int32,
    `ring_type_analog` Int32,
    `talk_type_analog`  Int32,
    `digital_key_success_true` Int32,
    `open_door_type_analog` Int32,
    `ring_type_sip` Int32,
    `talk_type_flat` Int32,
    `key_state_auth_err` Int32,
    `open_door_type_DTMF` Int32,
    `ring_cluster` Int32,
    `talk_open_door_type_api` Int32
)
    ENGINE = MergeTree()
    ORDER BY uuid
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.metrics_asgard_mv
    REFRESH EVERY 1 DAY OFFSET 3 HOUR RANDOMIZE FOR 1 HOUR TO db1.metrics_asgard_ch AS
    SELECT
        *
    FROM db1.metrics_asgard
    """

ch.query_run(query_text)
```

```python
query_text = """--sql
SELECT
    *
FROM db1.metrics_asgard_ch
WHERE report_date = '2025-03-01'
ORDER BY report_date DESC
limit 100

"""

ch.query_run(query_text)
```


