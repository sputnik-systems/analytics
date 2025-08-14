__
### Tags: #YandexFunctions

### Links: 

[[citizens_st_mobile]]

https://console.yandex.cloud/folders/b1gb310irjlk6b99e14g/functions/functions/d4edcea0td54cgl3crd0/editor
____
## citizens_st_mobile_ch

```sql
INSERT INTO db1.citizens_st_mobile_ch
SELECT
report_date,

citizen_id,

trial_available,

state,

toDateTimeOrZero(activated_at) AS activated_at,

flat_uuid,

address_uuid
FROM db1.citizens_st_mobile_ch
WHERE report_date = '{0}'
```

___
[[categories_sc_support]]

```

```

https://console.yandex.cloud/folders/b1gb310irjlk6b99e14g/functions/functions/d4e51m57ib7qdnaim7fa/testing
___
### billing-and-accruals-clickhouse

[[billing_and_accruals]]

https://console.yandex.cloud/folders/b1gb310irjlk6b99e14g/functions/functions/d4esbmrease57pfpvc2u/editor
___
### count-of-nuw-addresses-clickhouse

[[count_of_nuw_addresses]]

https://console.yandex.cloud/folders/b1gb310irjlk6b99e14g/functions/functions/d4efr5qcsursk7liivor/editor
___
### device-history-by-company-and-city-clickhouse

[[all_order_accumulated_data]]
[[device_history_by_company_and_city]]

https://console.yandex.cloud/folders/b1gb310irjlk6b99e14g/functions/functions/d4e0t1jnr7j5q2sg7jb9/editor