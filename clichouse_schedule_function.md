___
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