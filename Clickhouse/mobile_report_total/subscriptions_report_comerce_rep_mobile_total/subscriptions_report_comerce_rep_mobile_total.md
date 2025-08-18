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
### Tags: #Mobile_Report

### Links:  

[[subscriptions_report_citizens_flats_comerce_rep_mobile_total]]

### Table

```python
query_text = """--sql
CREATE TABLE db1.subscriptions_report_comerce_rep_mobile_total
(
    `report_date` Date,
    `partner_uuid` String,
    `city` String,
    `paying_users` UInt64,
    `paying_users_standart` UInt64,
    `paying_users_standart_yakassa` UInt64,
    `paying_users_partner_pro` UInt64,
    `paying_users_standart_appstore` UInt64,
    `paying_users_standart_ios_from_cart` UInt64,
    `paying_users_day` UInt64,
    `paying_new_users_in_last_28_days` UInt64,
    `failed_subscript_last_28_days` UInt64,
    `stoped_subscript_last_28_days` UInt64,
    `renewed_subscriptions_last_28_days` UInt64,
    `sub_first_new_month` UInt64,
    `sub_extended_month` UInt64,
    `android_sub` UInt64,
    `android_sub_first_new` UInt64,
    `android_sub_extended_new` UInt64,
    `renew_stopped_at_android` UInt64,
    `renew_failed_at_android` UInt64,
    `android_sub_from_cart` UInt64,
    `android_sub_first_new_cart` UInt64,
    `android_sub_extended_new_cart` UInt64,
    `renew_stopped_at_android_cart` UInt64,
    `renew_failed_at_andeoid_cart` UInt64,
    `ios_sub` UInt64,
    `ios_sub_first_new` UInt64,
    `ios_sub_extended_new` UInt64,
    `renew_stopped_at_ios` UInt64,
    `renew_failed_at_ios` UInt64,
    `ios_sub_from_cart` UInt64,
    `ios_sub_first_new_cart` UInt64,
    `ios_sub_first_new_cart_transition` UInt64,
    `ios_sub_extended_new_cart` UInt64,
    `renew_stopped_at_ios_cart` UInt64,
    `renew_failed_at_ios_cart` UInt64,
    `renew_failed_at_c` UInt64,
    `renew_failed_at_month` UInt64,
    `renew_stopped_at_c` UInt64,
    `renew_stopped_at_month` UInt64
)
ENGINE = MergeTree()
ORDER BY partner_uuid
"""

ch.query_run(query_text)
```

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.subscriptions_report_comerce_rep_mobile_total_mv
    REFRESH EVERY 1 DAY OFFSET 5 HOUR 55 MINUTE TO db1.subscriptions_report_comerce_rep_mobile_total AS
        SELECT
        report_date,
        partner_uuid,
        city,
--        Общие значения
--        Все подписки
        COUNT(if(state = 'activated', `citizen_id` ,NULL)) as paying_users,
        /* Все подписки стандарт */
        COUNT(if((state= 'activated' and plan = 'standard_p1m'), citizen_id,NULL)) as paying_users_standart,
--        /* Все подписки  подписки Андройд*/    
        COUNT(distinct if((state= 'activated' AND subscribed_from = 'yakassa' and  started_from != 'ios'), citizen_id,NULL)) as paying_users_standart_yakassa,
        COUNT(distinct if((state= 'activated'  and pro_subs = 1), citizen_id,NULL)) as paying_users_partner_pro,
--        /* Все подписки  подписки IOS*/    
        COUNT(distinct if((state= 'activated' and subscribed_from = 'ios'), citizen_id,NULL)) as paying_users_standart_appstore,
--        /* Все подписки подписки IOS c карты*/    
        COUNT(distinct if(state = 'activated' and started_from = 'ios' AND subscribed_from = 'yakassa' ,citizen_id,null)) AS paying_users_standart_ios_from_cart,
        COUNT(distinct if((state= 'activated') and report_date = `activated_at`, citizen_id,NULL)) as paying_users_day,
--        /* Новые оплаты за последние 28 дней*/    
        COUNT(if(date_diff('day', created_at, activated_at) <= 28 AND state = 'activated', citizen_id, null)) AS paying_new_users_in_last_28_days,
--        /* Ошибки оплаты за последние 28 дней*/    
        COUNT(if(date_diff('day', created_at, activated_at) <= 28, citizen_id, null)) AS failed_subscript_last_28_days,
--        /* Отмены подписок за последние 28 дней*/    
        COUNT(if(date_diff('day', created_at, activated_at) <= 28, citizen_id, null)) AS stoped_subscript_last_28_days,
--        /* Продленные подписки за последние 28 дней*/  
        COUNT(if(date_diff('day', created_at, activated_at) > 28 AND state= 'activated', citizen_id, null)) AS renewed_subscriptions_last_28_days,
        --
        count(if(created_at >= toStartOfMonth(report_date) AND state = 'activated', citizen_id, null)) AS `sub_first_new_month`,
        count(if(created_at < toStartOfMonth(report_date)
                AND state = 'activated', citizen_id, null)) AS `sub_extended_month`,
--    --    /* Android для старых данных *
--    --    /* Подписки Android для старых данных */
        count(if(subscribed_from = 'yakassa' 
                AND report_date = `activated_at` 
                AND state = 'activated' 
                AND (started_from  is null or started_from ='')
                ,citizen_id,null)) AS `android_sub`,
--        /* Новые подписки Android для старых данных */
        count(if(subscribed_from = 'yakassa' 
                AND date_diff('day', created_at, activated_at)  <= 28
                AND (started_from  is null or started_from ='')
                AND report_date = `activated_at` 
                AND state = 'activated', citizen_id, null)) AS `android_sub_first_new`,
--    --    /* Продленные подписки Android для старых данных */
        count(if(subscribed_from = 'yakassa' 
                AND date_diff('day', created_at, activated_at)  > 28
                AND (started_from  is null or started_from ='')
                AND report_date = `activated_at` 
                AND state = 'activated', citizen_id, null)) AS `android_sub_extended_new`,
--    --    /* Отмененные подписки Android для старых данных */
        count(if(subscribed_from = 'yakassa' and report_date = `renew_stopped_at` 
        	AND (started_from  is null or started_from =''), citizen_id, null)) as `renew_stopped_at_android`,
--    --    /* Неудачные подписки Android для старых данных */
        count(if(subscribed_from = 'yakassa' and report_date = `renew_failed_at` AND (started_from  is null or started_from ='') 
        	and `renew_stopped_at` IS NULL, citizen_id, null)) as renew_failed_at_android,
--    --    /* Android */
--    --    /* Подписки Android */
        count(if((started_from = 'android' OR started_from = 'hms') AND state = 'activated' 
			AND report_date = `activated_at` ,citizen_id,null)) AS `android_sub_from_cart`,
--    --    /* Новые подписки Android */
        count(if((started_from = 'android' OR started_from = 'hms')
                AND date_diff('day', created_at, activated_at)  <= 28
                AND report_date = `activated_at` 
                AND state = 'activated', citizen_id, null)) AS `android_sub_first_new_cart`,
    --    /* Продленные подписки Android */
        count(if((started_from = 'android' OR started_from = 'hms')
                AND date_diff('day', created_at, activated_at)  > 28
                AND report_date = `activated_at` 
                AND state = 'activated', citizen_id, null)) AS `android_sub_extended_new_cart`,
    --    /* Отмененные подписки Android */
        count(if((started_from = 'android' OR started_from = 'hms') and report_date = `renew_stopped_at`, citizen_id, null)) as `renew_stopped_at_android_cart`,
    --    /* неудачные подписки Android */
        count(if((started_from = 'android' OR started_from = 'hms') and report_date = `renew_failed_at`  
        		and `renew_stopped_at` IS NULL, citizen_id, null)) as `renew_failed_at_andeoid_cart`,
    --    /* IOS с appstore */
    --    /* Подписки IOS с appstore */
        count(if(subscribed_from = 'ios' 
                AND report_date = `activated_at` 
                AND state = 'activated' 
                ,citizen_id,null)) AS `ios_sub`,
    --    /* Новые подписки IOS с appstore */
        count(if(subscribed_from = 'ios' 
                AND date_diff('day', created_at, activated_at) <= 28
                AND report_date = `activated_at` 
                AND state = 'activated', citizen_id, null)) AS `ios_sub_first_new`,
    --    /* Продленные подписки IOS с appstore */
        count(if(subscribed_from = 'ios' 
                AND date_diff('day', created_at, activated_at) > 28
                AND report_date = `activated_at` 
                AND state = 'activated', citizen_id, null)) AS `ios_sub_extended_new`,
    --    /* Отмененные подписки IOS с appstore */
        count(if(subscribed_from = 'ios' and report_date = `renew_stopped_at`, citizen_id, null)) as `renew_stopped_at_ios`,
    --    /* Неудачные подписки  IOS с appstore */
        count(if(subscribed_from = 'ios' and report_date = `renew_failed_at`  and `renew_stopped_at` is NULL, citizen_id, null)) as `renew_failed_at_ios`,   
    --    /* IOS c карты */
    --    /* Подписки IOS c карты */
        count(if(started_from = 'ios' AND subscribed_from = 'yakassa' AND state = 'activated' AND report_date = `activated_at`,citizen_id,null)) AS `ios_sub_from_cart`, 
        /* Новые подписки IOS c карты */
        count(if(started_from = 'ios' and subscribed_from = 'yakassa'
                AND date_diff('day', created_at, activated_at)  <= 28
                AND report_date = `activated_at` 
                AND state = 'activated', citizen_id, null)) AS `ios_sub_first_new_cart`,
    --    /* Новые подписки IOS c карты (переход из appstore)*/
        count(if(started_from = 'ios' and subscribed_from = 'yakassa'
                AND date_diff('day', created_at, activated_at)  <= 28
                AND report_date = `activated_at` 
                AND state = 'activated' and lag_subscribed_from = 'ios', citizen_id, null)) AS `ios_sub_first_new_cart_transition`,
    --    /* Продленные подписки IOS c карты */
        count(if(started_from = 'ios' and subscribed_from = 'yakassa'
                AND date_diff('day', created_at, activated_at)  > 28
                AND report_date = `activated_at` 
                AND state = 'activated', citizen_id, null)) AS `ios_sub_extended_new_cart`,
    --    /* Отмененные подписки IOS c карты */
        count(if(started_from = 'ios' and subscribed_from = 'yakassa' and report_date = `renew_stopped_at`, citizen_id, null)) as `renew_stopped_at_ios_cart`,
    --    /* Неудачны подписки IOS c карты */
        count(if(started_from = 'ios' and subscribed_from = 'yakassa' and report_date = `renew_failed_at`   
        		and `renew_stopped_at` IS NULL, citizen_id, null)) as `renew_failed_at_ios_cart`,
        /* Доп */
        /* Общее количество неудачных подписок */
        count(if(report_date = `renew_failed_at`  and `renew_stopped_at` is NULL,citizen_id, null)) AS `renew_failed_at_c`,
        count(if(`renew_failed_at` BETWEEN toStartOfMonth(report_date) AND  report_date and `renew_stopped_at` is NULL ,citizen_id, null)) AS `renew_failed_at_month`,
    --    /* Общее отмененных подписок */
        count(if(report_date = `renew_stopped_at`,citizen_id, null)) AS `renew_stopped_at_c`,
        count(if(`renew_stopped_at` BETWEEN toStartOfMonth(report_date) AND  report_date ,citizen_id, null)) AS `renew_stopped_at_month`
    FROM
        (SELECT
		    partner_uuid,
		    `report_date`,
		    `citizen_id`,
		    state,
		    lagInFrame(subscribed_from, 1) OVER (
		            PARTITION BY citizen_id
		            ORDER BY report_date
		            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
		    ) AS lag_subscribed_from,
		    toDateOrNull(`created_at`) AS `created_at`,
		    `subscribed_from`,
		    `plan`,
		    toDateOrNull(`activated_at`) AS `activated_at`,
		    toDateOrNull(`expires_date`) AS `expires_date`,
		    toDateOrNull(`renew_stopped_at`) AS `renew_stopped_at`,
		    toDateOrNull(`renew_failed_at`) AS `renew_failed_at`,
		    `started_from`,
		    `pro_subs`,
		    `city`,
		    IF(COUNT(IF(state='activated',1,NULL)) 
		            OVER (PARTITION BY citizen_id, report_date) > 1 
		            AND subscribed_from='ios', 1, 0) AS filter_ios
		FROM db1.subscriptions_report_citizens_flats_comerce_rep_mobile_total
        )
    WHERE filter_ios = 0
    group by report_date , partner_uuid, city
--
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
    FROM db1.subscriptions_report_comerce_rep_mobile_total
    limit 2
    """

ch.query_run(query_text)
```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.subscriptions_report_comerce_rep_mobile_total DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)
```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.subscriptions_report_comerce_rep_mobile_total_mv
    """

ch.query_run(query_text)
```

### drop ch

```python
query_text = """--sql
    DROP TABLE db1.subscriptions_report_comerce_rep_mobile_total
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.subscriptions_report_comerce_rep_mobile_total_mv
"""

ch.query_run(query_text)
```
