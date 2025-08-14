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
    display_name: Python 3 (ipykernel)
    language: python
    name: python3
---

```python jupyter={"is_executing": false}
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

[[mobile_report_rep_mobile_full]]
___
### Table

```python
query_text = """
WITH t_dec AS(
SELECT
	t1.report_date  AS report_date,
	t2.city  AS city,
	t2.partner_uuid  AS partner_uuid
FROM
	(SELECT DISTINCT report_date FROM db1.mobile_report_rep_mobile_full) AS t1
	CROSS JOIN
	(SELECT DISTINCT city,partner_uuid FROM db1.mobile_report_rep_mobile_full) AS t2
),
--
mob_t AS(
	SELECT
		t_dec.`report_date` AS report_date,
        t_dec.`partner_uuid` AS partner_uuid,
        t_dec.`city` AS city,
        `IOS_PL` ,
        `appstore_count_85` ,
        `appstore_count_85_refunded` ,
        `appstore_count_69` ,
        `appstore_count_69_refunded` ,
        `appstore_count_499` ,
        `appstore_count_499_refunded` ,
        `appstore_count_2390` ,
        `appstore_count_2390_refunded` ,
        `appstore_count_1` ,
        `appstore_count_1_refunded` ,
        `refunded_amount_appstore` ,
        `Android_PL` ,
        `yookassa_count_85` ,
        `yookassa_count_85_refunded` ,
        `yookassa_count_69` ,
        `yookassa_count_69_refunded` ,
        `yookassa_count_35` ,
        `yookassa_count_35_refunded` ,
        `yookassa_count_1` ,
        `yookassa_count_1_refunded` ,
        `yookassa_count_499` ,
        `yookassa_count_499_refunded` ,
        `yookassa_count_249` ,
        `yookassa_count_249_refunded` ,
        `yookassa_count_2390` ,
        `yookassa_count_2390_refunded` ,
        `refunded_amount_yookassa` 
	FROM t_dec
	LEFT JOIN db1.mobile_report_rep_mobile_full AS mob_t
		ON t_dec.report_date = mob_t.report_date
		AND t_dec.partner_uuid = mob_t.partner_uuid
		AND t_dec.city = mob_t.city
)
--
SELECT 
		report_date,
	    partner_uuid,
	    city,
	   	SUM(IOS_PL) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS IOS_PL_cum,
		SUM(appstore_count_85) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS appstore_count_85_cum,
		SUM(appstore_count_85_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS appstore_count_85_refunded_cum,
		SUM(appstore_count_69) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS appstore_count_69_cum,
		SUM(appstore_count_69_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS appstore_count_69_refunded_cum,
		SUM(appstore_count_499) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS appstore_count_499_cum,
		SUM(appstore_count_499_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS appstore_count_499_refunded_cum,
		SUM(appstore_count_2390) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS appstore_count_2390_cum,
		SUM(appstore_count_2390_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS appstore_count_2390_refunded_cum,  
		SUM(appstore_count_1) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS appstore_count_1_cum,
		SUM(appstore_count_1_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS appstore_count_1_refunded_cum,  
		SUM(refunded_amount_appstore) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS refunded_amount_appstore_1_cum,
		--
		SUM(Android_PL) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Android_PL_cum,
		SUM(yookassa_count_85) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_85_cum,
		SUM(yookassa_count_85_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_85_refunded_cum,
		SUM(yookassa_count_69) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_69_cum,
		SUM(yookassa_count_69_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_69_refunded_cum,
		SUM(yookassa_count_35) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_35_cum,
		SUM(yookassa_count_35_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_35_refunded_cum,			          
		SUM(yookassa_count_1) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_1_cum,
		SUM(yookassa_count_1_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_1_refunded_cum,		          
		SUM(yookassa_count_499) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_499_cum,
		SUM(yookassa_count_499_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_499_refunded_cum,	
		SUM(yookassa_count_249) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_249_cum,
		SUM(yookassa_count_249_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_249_refunded_cum,	 
		SUM(yookassa_count_2390) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_2390_cum,
		SUM(yookassa_count_2390_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_2390_refunded_cum,	 
		SUM(refunded_amount_yookassa) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS refunded_amount_yookassa_cum
	FROM mob_t as mob_t
ORDER BY report_date DESC
"""

ch.get_schema(query_text)












```

```python
query_text = """--sql
CREATE TABLE db1.mobile_report_cum_rep_mobile_full
    (
        `report_date` Date,
        `partner_uuid` String,
        `city` String,
        `IOS_PL_cum` Int64,
        `appstore_count_85_cum` UInt64,
        `appstore_count_85_refunded_cum` UInt64,
        `appstore_count_69_cum` UInt64,
        `appstore_count_69_refunded_cum` UInt64,
        `appstore_count_499_cum` UInt64,
        `appstore_count_499_refunded_cum` UInt64,
        `appstore_count_2390_cum` UInt64,
        `appstore_count_2390_refunded_cum` UInt64,
        `appstore_count_1_cum` UInt64,
        `appstore_count_1_refunded_cum` UInt64,
        `refunded_amount_appstore_cum` Int64,
        `Android_PL_cum` Int64,
        `yookassa_count_85_cum` UInt64,
        `yookassa_count_85_refunded_cum` UInt64,
        `yookassa_count_69_cum` UInt64,
        `yookassa_count_69_refunded_cum` UInt64,
        `yookassa_count_35_cum` UInt64,
        `yookassa_count_35_refunded_cum` UInt64,
        `yookassa_count_1_cum` UInt64,
        `yookassa_count_1_refunded_cum` UInt64,
        `yookassa_count_499_cum` UInt64,
        `yookassa_count_499_refunded_cum` UInt64,
        `yookassa_count_249_cum` UInt64,
        `yookassa_count_249_refunded_cum` UInt64,
        `yookassa_count_2390_cum` UInt64,
        `yookassa_count_2390_refunded_cum` UInt64,
        `refunded_amount_yookassa_cum` Int64
    )
    ENGINE = MergeTree()
    ORDER BY report_date
"""

ch.query_run(query_text)
```
### Materialized view

```python
query_text = """--sql
    CREATE MATERIALIZED VIEW db1.mobile_report_cum_rep_mobile_full_mv
    REFRESH EVERY 1 DAY OFFSET 5 HOUR 43 MINUTES TO db1.mobile_report_cum_rep_mobile_full AS
	WITH t_dec AS(
	SELECT
		t1.report_date  AS report_date,
		t2.city  AS city,
		t2.partner_uuid  AS partner_uuid
	FROM
		(SELECT DISTINCT report_date FROM db1.mobile_report_rep_mobile_full) AS t1
		CROSS JOIN
		(SELECT DISTINCT city,partner_uuid FROM db1.mobile_report_rep_mobile_full) AS t2
	),
	--
	mob_t AS(
		SELECT
			t_dec.`report_date` AS report_date,
			t_dec.`partner_uuid` AS partner_uuid,
			t_dec.`city` AS city,
			`IOS_PL` ,
			`appstore_count_85` ,
			`appstore_count_85_refunded` ,
			`appstore_count_69` ,
			`appstore_count_69_refunded` ,
			`appstore_count_499` ,
			`appstore_count_499_refunded` ,
			`appstore_count_2390` ,
			`appstore_count_2390_refunded` ,
			`appstore_count_1` ,
			`appstore_count_1_refunded` ,
			`refunded_amount_appstore` ,
			`Android_PL` ,
			`yookassa_count_85` ,
			`yookassa_count_85_refunded` ,
			`yookassa_count_69` ,
			`yookassa_count_69_refunded` ,
			`yookassa_count_35` ,
			`yookassa_count_35_refunded` ,
			`yookassa_count_1` ,
			`yookassa_count_1_refunded` ,
			`yookassa_count_499` ,
			`yookassa_count_499_refunded` ,
			`yookassa_count_249` ,
			`yookassa_count_249_refunded` ,
			`yookassa_count_2390` ,
			`yookassa_count_2390_refunded` ,
			`refunded_amount_yookassa` 
		FROM t_dec
		LEFT JOIN db1.mobile_report_rep_mobile_full AS mob_t
			ON t_dec.report_date = mob_t.report_date
			AND t_dec.partner_uuid = mob_t.partner_uuid
			AND t_dec.city = mob_t.city
	)
	--
	SELECT 
		report_date,
	    partner_uuid,
	    city,
	   	SUM(IOS_PL) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS IOS_PL_cum,
		SUM(appstore_count_85) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS appstore_count_85_cum,
		SUM(appstore_count_85_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS appstore_count_85_refunded_cum,
		SUM(appstore_count_69) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS appstore_count_69_cum,
		SUM(appstore_count_69_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS appstore_count_69_refunded_cum,
		SUM(appstore_count_499) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS appstore_count_499_cum,
		SUM(appstore_count_499_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS appstore_count_499_refunded_cum,
		SUM(appstore_count_2390) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS appstore_count_2390_cum,
		SUM(appstore_count_2390_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS appstore_count_2390_refunded_cum,  
		SUM(appstore_count_1) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS appstore_count_1_cum,
		SUM(appstore_count_1_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS appstore_count_1_refunded_cum,  
		SUM(refunded_amount_appstore) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS refunded_amount_appstore_cum,
		--
		SUM(Android_PL) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Android_PL_cum,
		SUM(yookassa_count_85) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_85_cum,
		SUM(yookassa_count_85_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_85_refunded_cum,
		SUM(yookassa_count_69) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_69_cum,
		SUM(yookassa_count_69_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_69_refunded_cum,
		SUM(yookassa_count_35) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_35_cum,
		SUM(yookassa_count_35_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_35_refunded_cum,			          
		SUM(yookassa_count_1) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_1_cum,
		SUM(yookassa_count_1_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_1_refunded_cum,		          
		SUM(yookassa_count_499) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_499_cum,
		SUM(yookassa_count_499_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_499_refunded_cum,	
		SUM(yookassa_count_249) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_249_cum,
		SUM(yookassa_count_249_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_249_refunded_cum,	 
		SUM(yookassa_count_2390) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_2390_cum,
		SUM(yookassa_count_2390_refunded) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS yookassa_count_2390_refunded_cum,	 
		SUM(refunded_amount_yookassa) OVER (PARTITION BY partner_uuid, city, toStartOfMonth(report_date)
			          ORDER BY report_date
			          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS refunded_amount_yookassa_cum
	FROM mob_t as mob_t
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
        report_date,
        sum(yookassa_count_69_cum)
    FROM db1.mobile_report_cum_rep_mobile_full
    GROUP BY report_date
    ORDER BY report_date desc
    limit 10
    """

ch.query_run(query_text)

```

### delete a part


```python
query_text = """--sql
    ALTER TABLE db1.mobile_report_cum_rep_mobile_full DELETE WHERE report_date = '2025-07-17'
    """

ch.query_run(query_text)

```

### drop mv

```python
query_text = """--sql
    DROP TABLE db1.mobile_report_cum_rep_mobile_full_mv
    """

ch.query_run(query_text)
```

### drop table

```python
query_text = """--sql
    DROP TABLE db1.mobile_report_cum_rep_mobile_full
    """

ch.query_run(query_text)
```

### refresh mv

```python
query_text = """
SYSTEM REFRESH VIEW db1.mobile_report_cum_rep_mobile_full_mv
"""

ch.query_run(query_text)
```
