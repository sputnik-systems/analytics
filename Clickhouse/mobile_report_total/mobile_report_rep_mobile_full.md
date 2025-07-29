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
### Tags: #Mobile_Report

### Links:  

[[citizen_payments_st_mobile]]

[[citizens_st_mobile]]

[[citizens_dir_mobile]]

[[flats_dir_partner]]

[[entries_installation_points_dir_partner]]

[[installation_point_st_partner]]

[[companies_st_partner]]

___
### Table

```python
query_text = """--sql
CREATE TABLE db1.t_mobile_report_rep_mobile_full
    (
        `report_date` Date,
        `partner_uuid` String,
        `city` String,
        `IOS_PL` Int64,
        `appstore_count_85` UInt64,
        `appstore_count_85_refunded` UInt64,
        `appstore_count_69` UInt64,
        `appstore_count_69_refunded` UInt64,
        `appstore_count_499` UInt64,
        `appstore_count_499_refunded` UInt64,
        `appstore_count_2390` UInt64,
        `appstore_count_2390_refunded` UInt64,
        `appstore_count_1` UInt64,
        `appstore_count_1_refunded` UInt64,
        `refunded_amount_appstore` Int64,
        `Android_PL` Int64,
        `yookassa_count_85` UInt64,
        `yookassa_count_85_refunded` UInt64,
        `yookassa_count_69` UInt64,
        `yookassa_count_69_refunded` UInt64,
        `yookassa_count_35` UInt64,
        `yookassa_count_35_refunded` UInt64,
        `yookassa_count_1` UInt64,
        `yookassa_count_1_refunded` UInt64,
        `yookassa_count_499` UInt64,
        `yookassa_count_499_refunded` UInt64,
        `yookassa_count_249` UInt64,
        `yookassa_count_249_refunded` UInt64,
        `yookassa_count_2390` UInt64,
        `yookassa_count_2390_refunded` UInt64,
        `refunded_amount_yookassa` Int64
    )
    ENGINE = MergeTree()
    ORDER BY report_date
"""

ch.query_run(query_text)
```
### Materialized view

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.t_mobile_report_rep_mobile_full_mv
    REFRESH EVERY 1 DAY OFFSET 5 HOUR 40 MINUTES TO db1.t_mobile_report_rep_mobile_full AS
    SELECT
        report_date ,
        partner_uuid,
        city,
        ((`appstore_count_85` * 85 + `appstore_count_69` * 69 + `appstore_count_499` * 499 + `appstore_count_2390` * 2390 + `appstore_count_1` * 1) - `refunded_amount_appstore`)  as IOS_PL,
        --
        `appstore_count_85`,
        `appstore_count_85_refunded`,
        `appstore_count_69`,
        `appstore_count_69_refunded`,
        `appstore_count_499`,
        `appstore_count_499_refunded`,
        `appstore_count_2390`,
        `appstore_count_2390_refunded`,
        `appstore_count_1`,
        `appstore_count_1_refunded`,
        `refunded_amount_appstore`,
        --
        ((`yookassa_count_85` * 85 + `yookassa_count_69` * 69 + `yookassa_count_35` * 35 + `yookassa_count_1` * 1 + `yookassa_count_499` * 499 + `yookassa_count_249` * 249 +`yookassa_count_2390` * 2390) - `refunded_amount_yookassa`) as Android_PL,
        --
        `yookassa_count_85`,
        `yookassa_count_85_refunded`,
        `yookassa_count_69`, 
        `yookassa_count_69_refunded`,
        `yookassa_count_35`, 
        `yookassa_count_35_refunded`,
        `yookassa_count_1`, 
        `yookassa_count_1_refunded`, 
        `yookassa_count_499`, 
        `yookassa_count_499_refunded`,
        `yookassa_count_249`, 
        `yookassa_count_249_refunded`,  
        `yookassa_count_2390`, 
        `yookassa_count_2390_refunded`,  
        --
        `refunded_amount_yookassa`
    FROM    
        (SELECT
            citizen_payments_st_mobile_ch.report_date AS report_date,
            installation_point_st_partner.partner_uuid  AS partner_uuid,
            entries_installation_points_dir_partner.city AS city,
            count(if(`from` = 'appstore' and amount = 85 and citizen_payments_st_mobile.state = 'success', citizen_payments_st_mobile.citizen_id,null)) as appstore_count_85,
            count(if(`from` = 'appstore' and amount = 85 and citizen_payments_st_mobile.state = 'refunded', citizen_payments_st_mobile.citizen_id,null)) as appstore_count_85_refunded,
            count(if(`from` = 'appstore' and amount = 69 and citizen_payments_st_mobile.state = 'success', citizen_payments_st_mobile.citizen_id,null)) as appstore_count_69,
            count( if(`from` = 'appstore' and amount = 69 and citizen_payments_st_mobile.state = 'refunded', citizen_payments_st_mobile.citizen_id,null)) as appstore_count_69_refunded,
            count( if(`from` = 'appstore' and amount = 499 and citizen_payments_st_mobile.state = 'success', citizen_payments_st_mobile.citizen_id,null)) as appstore_count_499,
            count( if(`from` = 'appstore' and amount = 499 and citizen_payments_st_mobile.state = 'refunded', citizen_payments_st_mobile.citizen_id,null)) as appstore_count_499_refunded,
            count( if(`from` = 'appstore' and amount = 2390 and citizen_payments_st_mobile.state = 'success', citizen_payments_st_mobile.citizen_id,null)) as appstore_count_2390,
            count( if(`from` = 'appstore' and amount = 2390 and citizen_payments_st_mobile.state = 'refunded', citizen_payments_st_mobile.citizen_id,null)) as appstore_count_2390_refunded,
            count( if(`from` = 'appstore' and amount = 1 and citizen_payments_st_mobile.state = 'success', citizen_payments_st_mobile.citizen_id,null)) as appstore_count_1,
            count( if(`from` = 'appstore' and amount = 1 and citizen_payments_st_mobile.state = 'refunded', citizen_payments_st_mobile.citizen_id,null)) as appstore_count_1_refunded,
            --
            COALESCE(sum(if(`from` = 'appstore',`refunded_amount`,null)),0) as refunded_amount_appstore,
            --
            count( if(`from` = 'yookassa' and amount = 85 and citizen_payments_st_mobile.state = 'success', citizen_payments_st_mobile.citizen_id,null)) as yookassa_count_85,
            count( if(`from` = 'yookassa' and amount = 85 and citizen_payments_st_mobile.state = 'refunded', citizen_payments_st_mobile.citizen_id,null)) as yookassa_count_85_refunded,    
            count( if(`from` = 'yookassa' and amount = 69 and citizen_payments_st_mobile.state = 'success', citizen_payments_st_mobile.citizen_id,null)) as yookassa_count_69, 
            count( if(`from` = 'yookassa' and amount = 69 and citizen_payments_st_mobile.state = 'refunded', citizen_payments_st_mobile.citizen_id,null)) as yookassa_count_69_refunded,
            count( if(`from` = 'yookassa' and amount = 35 and citizen_payments_st_mobile.state = 'success', citizen_payments_st_mobile.citizen_id,null)) as yookassa_count_35, 
            count( if(`from` = 'yookassa' and amount = 35 and citizen_payments_st_mobile.state = 'refunded', citizen_payments_st_mobile.citizen_id,null)) as yookassa_count_35_refunded,
            count( if(`from` = 'yookassa' and amount = 1 and citizen_payments_st_mobile.state = 'success', citizen_payments_st_mobile.citizen_id,null)) as yookassa_count_1, 
            count( if(`from` = 'yookassa' and amount = 1 and citizen_payments_st_mobile.state = 'refunded', citizen_payments_st_mobile.citizen_id,null)) as yookassa_count_1_refunded, 
            count( if(`from` = 'yookassa' and amount = 499 and citizen_payments_st_mobile.state = 'success', citizen_payments_st_mobile.citizen_id,null)) as yookassa_count_499, 
            count( if(`from` = 'yookassa' and amount = 499 and citizen_payments_st_mobile.state = 'refunded', citizen_payments_st_mobile.citizen_id,null)) as yookassa_count_499_refunded,
            count( if(`from` = 'yookassa' and amount = 249 and citizen_payments_st_mobile.state = 'success', citizen_payments_st_mobile.citizen_id,null)) as yookassa_count_249, 
            count( if(`from` = 'yookassa' and amount = 249 and citizen_payments_st_mobile.state = 'refunded', citizen_payments_st_mobile.citizen_id,null)) as yookassa_count_249_refunded,  
            count( if(`from` = 'yookassa' and amount = 2390 and citizen_payments_st_mobile.state = 'success', citizen_payments_st_mobile.citizen_id,null)) as yookassa_count_2390, 
            count( if(`from` = 'yookassa' and amount = 2390 and citizen_payments_st_mobile.state = 'refunded', citizen_payments_st_mobile.citizen_id,null)) as yookassa_count_2390_refunded,  
            --
            COALESCE(sum(if(`from` = 'yookassa',refunded_amount,null)),0) as refunded_amount_yookassa,
            --
            max(citizen_payments_st_mobile.`report_date`) as max_report_date,
            min(citizen_payments_st_mobile.`report_date`) as min_report_date
        FROM db1.`citizen_payments_st_mobile_ch` AS citizen_payments_st_mobile
        LEFT JOIN db1.citizens_st_mobile_ch AS citizens_st_mobile
            ON citizens_st_mobile.`citizen_id` = citizen_payments_st_mobile.`citizen_id`
            AND  citizen_payments_st_mobile.`report_date` = citizens_st_mobile.`report_date`  
        LEFT JOIN db1.`citizens_dir_mobile_ch` AS citizens_dir_mobile 
            ON citizen_payments_st_mobile.`citizen_id` = citizens_dir_mobile.`citizen_id`
        LEFT JOIN db1.`flats_dir_partner_ch` AS flats_dir_partner 
            ON flats_dir_partner.`flat_uuid` = citizens_st_mobile_ch.`flat_uuid`
        LEFT JOIN db1.`entries_installation_points_dir_partner_ch` AS entries_installation_points_dir_partner 
            ON flats_dir_partner.`address_uuid` = entries_installation_points_dir_partner.`address_uuid`
        LEFT JOIN `db1`.`installation_point_st_partner_ch` AS installation_point_st_partner
            ON `installation_point_st_partner`.`installation_point_id` = entries_installation_points_dir_partner.`installation_point_id`
            AND installation_point_st_partner.`report_date` = citizen_payments_st_mobile.`report_date`
        LEFT JOIN db1.`companies_st_partner_ch` AS companies_st_partner
            ON installation_point_st_partner.`partner_uuid`= companies_st_partner.`partner_uuid`
            AND  companies_st_partner.`report_date` = citizen_payments_st_mobile.`report_date`
        GROUP by 
            report_date,
            partner_uuid,
            city
            ) as t1
        SETTINGS join_algorithm = 'partial_merge'


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
    FROM db1.t_mobile_report_rep_mobile_full_mv
    ORDER BY report_date desc
    limit 10
    """

ch.query_run(query_text)

```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.t_mobile_report_rep_mobile_full DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)

```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.t_mobile_report_rep_mobile_full_mv
    """

ch.query_run(query_text)
```

### drop table

```python
query_text = """--sql
    DROP TABLE db1.t_mobile_report_rep_mobile_full
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.t_mobile_report_rep_mobile_full_mv
"""

ch.query_run(query_text)
```
