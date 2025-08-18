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
    display_name: Python (myenv)
    language: python
    name: myenv
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
### Tags: #Mobile_Report

### Links:

[[units_on_sk_platform_rep_mobile_total]]

[[subscriptions_report_comerce_rep_mobile_total]]

[[total_active_users_per_day_rep_mobile_total]]

[[total_active_users_rep_mobile_total]]

[[mobile_report_rep_mobile_full]]

[[total_activated_account_rep_mobile_full]]

[[new_users_pd_rep_mobile_total]]

[[maf_rep_mobile_total]]

[[mobile_report_cum_rep_mobile_full]]
___

```python
query_text = """--sql
CREATE TABLE db1.mobile_report_total
(
    `report_date` Date,
    `partner_uuid` String,
    `city` String,
    `android_sub` UInt64,
    `android_sub_extended_new` UInt64,
    `android_sub_extended_new_cart` UInt64,
    `android_sub_first_new` UInt64,
    `android_sub_first_new_cart` UInt64,
    `android_sub_from_cart` UInt64,
    `failed_subscript_last_28_days` UInt64,
    `ios_sub` UInt64,
    `ios_sub_extended_new` UInt64,
    `ios_sub_extended_new_cart` UInt64,
    `ios_sub_first_new` UInt64,
    `ios_sub_first_new_cart` UInt64,
    `ios_sub_first_new_cart_transition` UInt64,
    `ios_sub_from_cart` UInt64,
    `paying_new_users_in_last_28_days` UInt64,
    `sub_first_new_month` UInt64,
    `sub_extended_month` UInt64,
    `paying_users` UInt64,
    `paying_users_day` UInt64,
    `paying_users_partner_pro` UInt64,
    `paying_users_standart` UInt64,
    `paying_users_standart_appstore` UInt64,
    `paying_users_standart_ios_from_cart` UInt64,
    `paying_users_standart_yakassa` UInt64,
    `renew_failed_at` UInt64,
    `renew_failed_at_month` UInt64,
    `renew_failed_at_andeoid_cart` UInt64,
    `renew_failed_at_android` UInt64,
    `renew_failed_at_ios` UInt64,
    `renew_failed_at_ios_cart` UInt64,
    `renew_stopped_at` UInt64,
    `renew_stopped_at_month` UInt64,
    `renew_stopped_at_android` UInt64,
    `renew_stopped_at_android_cart` UInt64,
    `renew_stopped_at_ios` UInt64,
    `renew_stopped_at_ios_cart` UInt64,
    `renewed_subscriptions_last_28_days` UInt64,
    `stoped_subscript_last_28_days` UInt64,
    `units_free_monetization_start` UInt64,
    `units_free_monetization` UInt64,
    `units_free_monetization_pro` UInt64,
    `units_on_platform` UInt64,
    `units_stricted monetization` UInt64,
    `total_active_user_monetization_per_day` Int32,
    `total_active_user_subscribed_monetization_per_day` Int32,
    `total_active_users_per_day` Int32,
    `new_active_users` UInt32,
    `total_active_user_subscribed_monetization` UInt32,
    `total_active_users` UInt32,
    `total_active_users_ble_available` UInt32,
    `total_active_users_ble_available_monetization` UInt32,
    `total_active_users_ble_available_subscribed_monetization` UInt32,
    `total_active_users_monetization` UInt32,
    `Android_PL` Int64,
    `IOS_PL` Int64,
    `appstore_count_1` UInt64,
    `appstore_count_1_refunded` UInt64,
    `appstore_count_2390` UInt64,
    `appstore_count_2390_refunded` UInt64,
    `appstore_count_499` UInt64,
    `appstore_count_499_refunded` UInt64,
    `appstore_count_69` UInt64,
    `appstore_count_69_refunded` UInt64,
    `refunded_amount_appstore` Int64,
    `refunded_amount_yookassa` Int64,
    `yookassa_count_1` UInt64,
    `yookassa_count_1_refunded` UInt64,
    `yookassa_count_2390` UInt64,
    `yookassa_count_2390_refunded` UInt64,
    `yookassa_count_249` UInt64,
    `yookassa_count_249_refunded` UInt64,
    `yookassa_count_35` UInt64,
    `yookassa_count_35_refunded` UInt64,
    `yookassa_count_499` UInt64,
    `yookassa_count_499_refunded` UInt64,
    `yookassa_count_69` UInt64,
    `yookassa_count_69_refunded` UInt64,
    `appstore_count_85` UInt64,
    `appstore_count_85_refunded` UInt64,
    `yookassa_count_85` UInt64,
    `yookassa_count_85_refunded` UInt64,
    `Android_PL_cum` Int64,
    `IOS_PL_cum` Int64,
    `appstore_count_1_cum` UInt64,
    `appstore_count_1_refunded_cum` UInt64,
    `appstore_count_2390_cum` UInt64,
    `appstore_count_2390_refunded_cum` UInt64,
    `appstore_count_499_cum` UInt64,
    `appstore_count_499_refunded_cum` UInt64,
    `appstore_count_69_cum` UInt64,
    `appstore_count_69_refunded_cum` UInt64,
    `refunded_amount_appstore_cum` Int64,
    `refunded_amount_yookassa_cum` Int64,
    `yookassa_count_1_cum` UInt64,
    `yookassa_count_1_refunded_cum` UInt64,
    `yookassa_count_2390_cum` UInt64,
    `yookassa_count_2390_refunded_cum` UInt64,
    `yookassa_count_249_cum` UInt64,
    `yookassa_count_249_refunded_cum` UInt64,
    `yookassa_count_35_cum` UInt64,
    `yookassa_count_35_refunded_cum` UInt64,
    `yookassa_count_499_cum` UInt64,
    `yookassa_count_499_refunded_cum` UInt64,
    `yookassa_count_69_cum` UInt64,
    `yookassa_count_69_refunded_cum` UInt64,
    `appstore_count_85_cum` UInt64,
    `appstore_count_85_refunded_cum` UInt64,
    `yookassa_count_85_cum` UInt64,
    `yookassa_count_85_refunded_cum` UInt64,
    `total_activated_account` UInt64,
    `total_activated_account_monetization` UInt64,
    `total_activated_account_ble_available_monetization` UInt64,
    `total_activated_account_ble_available` UInt64,
    `new_created_account` UInt64,
    `new_activated_account` UInt64,
    `new_created_account_day` UInt64,
    `new_activated_account_day` UInt64,
    `MAF` UInt64,
    `stricted_MAF` UInt64,
    `freemonetization_MAF` UInt64
)
ENGINE = MergeTree()
ORDER BY report_date
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.mobile_report_total_mv
    REFRESH EVERY 1 DAY OFFSET 6 HOUR 15 MINUTE TO db1.mobile_report_total AS
    WITH t_dec AS(
        SELECT
            t1.report_date  AS report_date,
            t2.city  AS city,
            t2.partner_uuid  AS partner_uuid
        FROM
            (SELECT DISTINCT report_date FROM db1.installation_point_st_partner_ch) AS t1
        CROSS JOIN
            (SELECT DISTINCT city,inst.partner_uuid AS partner_uuid
            FROM db1.installation_point_st_partner_ch AS inst
            LEFT JOIN db1.entries_installation_points_dir_partner AS eipdp ON inst.installation_point_id  = eipdp.installation_point_id ) AS t2
        )
    --
    SELECT
        t_dec.report_date AS report_date,
        t_dec.partner_uuid AS partner_uuid,
        t_dec.city AS city,
        --
        `android_sub`,
        `android_sub_extended_new`,
        `android_sub_extended_new_cart`,
        `android_sub_first_new`,
        `android_sub_first_new_cart`,
        `android_sub_from_cart`,
        `failed_subscript_last_28_days`,
        `ios_sub`,
        `ios_sub_extended_new`,
        `ios_sub_extended_new_cart`,
        `ios_sub_first_new`,
        `ios_sub_first_new_cart`,
        `ios_sub_first_new_cart_transition`,
        `ios_sub_from_cart`,
        `sub_first_new_month` ,
        `sub_extended_month` ,
        `paying_new_users_in_last_28_days`,
        `paying_users`,
        `paying_users_day`,
        `paying_users_partner_pro`,
        `paying_users_standart`,
        `paying_users_standart_appstore`,
        `paying_users_standart_ios_from_cart`,
        `paying_users_standart_yakassa`,
        `renew_failed_at_c` AS `renew_failed_at`,
        `renew_failed_at_month`,
        `renew_failed_at_andeoid_cart`,
        `renew_failed_at_android`,
        `renew_failed_at_ios`,
        `renew_failed_at_ios_cart`,
        `renew_stopped_at_c` AS `renew_stopped_at`,
        `renew_stopped_at_month`,
        `renew_stopped_at_android`,
        `renew_stopped_at_android_cart`,
        `renew_stopped_at_ios`,
        `renew_stopped_at_ios_cart`,
        `renewed_subscriptions_last_28_days`,
        `stoped_subscript_last_28_days`,
        --
        `units_free_monetization_start`,
        `units_free_monetization`,
        `units_free_monetization_pro`,
        `units_on_platform`,
        `units_stricted monetization`,
        --
        `total_active_user_monetization_per_day`,
        `total_active_user_subscribed_monetization_per_day`,
        `total_active_users_per_day`,
        --
        `new_active_users`,
        `total_active_user_subscribed_monetization`,
        `total_active_users`,
        `total_active_users_ble_available`,
        `total_active_users_ble_available_monetization`,
        `total_active_users_ble_available_subscribed_monetization`,
        `total_active_users_monetization`,
        --
        `Android_PL`,
        `IOS_PL`,
        `appstore_count_1`,
        `appstore_count_1_refunded`,
        `appstore_count_2390`,
        `appstore_count_2390_refunded`,
        `appstore_count_499`,
        `appstore_count_499_refunded`,
        `appstore_count_69`,
        `appstore_count_69_refunded`,
        `refunded_amount_appstore`,
        `refunded_amount_yookassa`,
        `yookassa_count_1`,
        `yookassa_count_1_refunded`,
        `yookassa_count_2390`,
        `yookassa_count_2390_refunded`,
        `yookassa_count_249`,
        `yookassa_count_249_refunded`,
        `yookassa_count_35`,
        `yookassa_count_35_refunded`,
        `yookassa_count_499`,
        `yookassa_count_499_refunded`,
        `yookassa_count_69`,
        `yookassa_count_69_refunded`,
        `appstore_count_85`,
        `appstore_count_85_refunded`,
        `yookassa_count_85`,
        `yookassa_count_85_refunded`,
        --
        `Android_PL_cum`,
        `IOS_PL_cum`,
        `appstore_count_1_cum`,
        `appstore_count_1_refunded_cum`,
        `appstore_count_2390_cum`,
        `appstore_count_2390_refunded_cum`,
        `appstore_count_499_cum`,
        `appstore_count_499_refunded_cum`,
        `appstore_count_69_cum`,
        `appstore_count_69_refunded_cum`,
        `refunded_amount_appstore_cum`,
        `refunded_amount_yookassa_cum`,
        `yookassa_count_1_cum`,
        `yookassa_count_1_refunded_cum`,
        `yookassa_count_2390_cum`,
        `yookassa_count_2390_refunded_cum`,
        `yookassa_count_249_cum`,
        `yookassa_count_249_refunded_cum`,
        `yookassa_count_35_cum`,
        `yookassa_count_35_refunded_cum`,
        `yookassa_count_499_cum`,
        `yookassa_count_499_refunded_cum`,
        `yookassa_count_69_cum`,
        `yookassa_count_69_refunded_cum`,
        `appstore_count_85_cum`,
        `appstore_count_85_refunded_cum`,
        `yookassa_count_85_cum`,
        `yookassa_count_85_refunded_cum`,
        --
        `total_activated_account`,
        `total_activated_account_monetization`,
        `total_activated_account_ble_available_monetization`,
        `total_activated_account_ble_available`,
        --
        --new_users_pd_rep_mobile
        `new_created_account`,
        `new_activated_account`,
        `new_created_account_day`,
        `new_activated_account_day`,
        --maf_rep_mobile_total
        --
        `MAF`,
        `stricted_MAF`,
        `freemonetization_MAF`
        --
    FROM t_dec
    LEFT JOIN db1.`units_on_sk_platform_rep_mobile_total` AS uosprmt 
        ON uosprmt.report_date = t_dec.report_date 
        AND uosprmt.city = t_dec.city
        AND uosprmt.partner_uuid = t_dec.partner_uuid
    LEFT JOIN db1.`subscriptions_report_comerce_rep_mobile_total` AS srcrmt
        ON srcrmt.report_date = t_dec.report_date 
        AND srcrmt.city = t_dec.city
        AND srcrmt.partner_uuid = t_dec.partner_uuid
    LEFT JOIN db1.`total_active_users_per_day_rep_mobile_total` AS taupdrmt
        ON taupdrmt.report_date = t_dec.report_date 
        AND taupdrmt.city = t_dec.city
        AND taupdrmt.partner_uuid = t_dec.partner_uuid
    LEFT JOIN db1.`total_active_users_rep_mobile_total` AS taurmt
        ON taurmt.report_date = t_dec.report_date 
        AND taurmt.city = t_dec.city
        AND taurmt.partner_uuid = t_dec.partner_uuid
    LEFT JOIN db1.`mobile_report_rep_mobile_full` AS mrrmf
        ON mrrmf.report_date = t_dec.report_date 
        AND mrrmf.city = t_dec.city
        AND mrrmf.partner_uuid = t_dec.partner_uuid
    LEFT JOIN db1.`mobile_report_cum_rep_mobile_full` AS mrcrmf
        ON mrcrmf.report_date = t_dec.report_date 
        AND mrcrmf.city = t_dec.city
        AND mrcrmf.partner_uuid = t_dec.partner_uuid
    LEFT JOIN db1.`total_activated_account_rep_mobile_full` AS taarmf
        ON taarmf.report_date = t_dec.report_date 
        AND taarmf.city = t_dec.city
        AND taarmf.partner_uuid = t_dec.partner_uuid
    LEFT JOIN db1.`new_users_pd_rep_mobile_total` AS nwprm
        ON nwprm.report_date = t_dec.report_date 
        AND nwprm.city = t_dec.city
        AND nwprm.partner_uuid = t_dec.partner_uuid
    LEFT JOIN db1.`maf_rep_mobile_total` AS mrmt
        ON mrmt.report_date = t_dec.report_date 
        AND mrmt.city = t_dec.city
        AND mrmt.partner_uuid = t_dec.partner_uuid
    -- WHERE t_dec.report_date  = toLastDayOfMonth(report_date)
    ORDER BY report_date DESC
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
    FROM db1.mobile_report_total
    ORDER BY report_date desc
    limit 10
    """

ch.query_run(query_text)

```

```python
query_text = """--sql
    SELECT
        report_date,
        sum(new_active_users)
    FROM db1.mobile_report_total
    GROUP BY report_date
    ORDER BY report_date desc
    limit 10
    """

ch.query_run(query_text)

```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.mobile_report_total DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)

```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.mobile_report_total_mv
    """

ch.query_run(query_text)
```

### drop table

```python
query_text = """--sql
    DROP TABLE db1.mobile_report_total
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.mobile_report_total_mv
"""

ch.query_run(query_text)
```
