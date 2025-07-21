---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.17.2
---

```python
INSERT INTO FUNCTION s3(
    'https://storage.yandexcloud.net/aggregated-data/billing_and_accruals/billing_and_accruals.csv',
    'CSVWithNames'
)
SETTINGS s3_truncate_on_insert = 1
SELECT DISTINCT
    report_date,
    report_date_month,
    report_date_week,
    report_date_year,
    company_name,
    partner_uuid,
    partner_lk,
    pro_subs,
    tin,
    billing_account_id,
    kpp,
    service,
    billing_cost,
    partner_program_accruals_amount2 AS partner_program_accruals_amount,
    billing_sum,
    partner_in_lk_list,
    status,
    billing_pro,
    enterprise_not_paid,
    enterprise_subs,
    enterprise_test,
    is_blocked,
    kz_pro,
    billing_count_new
FROM
(SELECT
	report_date,
	report_date_week,
	report_date_month,
	report_date_year,
	company_name,
	partner_uuid,
	partner_lk,
	tin,
	billing_account_id,
	comment,
	comment_type,
	kpp,
	service,
	billing_cost,
	pro_subs,
	partner_program_accruals_amount2,
	CAST(billing_sum AS Float64) / if(billing_cost = 0, 1, billing_cost) AS billing_count_new,
	billing_sum - if(type_of_acruals IN ('компенсация', 'скидка', 'бонус', 'ошибочное', 'ошибка'), accruals_amount, 0) AS billing_sum,
	if(partner_lk IN (
	    '152762','120345','149485','154071','151909','120377','122220','123181',
	    '148015','124591','129406','128416','123267','126056','126798','126800',
	    '124775','122241','122914','120505','120225','120220','123278','120602',
	    '151366','138824','150508','122009','149528','131796','121764','120192',
	    '121849','121211','125108','120198','136989','121014','128003','124731',
	    '120697','121584','122485','132770','123752','120859','137236','123905',
	    '121833','137838','123676','120604','120863','120696','120202','122916',
	    '140661','150523','144411','138838','137802','148339','142519','144701',
	    '123799','143782','123468','142109','153694','149591','137654','145091',
	    '144050','131589','122015','144666','140376','132882','120268','123849',
	    '142889','122024','122227','124808','142655','132902','131396','130630',
	    '123453','125916','131613','152045','124797','128118','120685','140945',
	    '122031','122818','129454','132160','151739','122794','120668','141274',
	    '124860','123830','120417','136909','150953','147012','120642','121772'
	), 1, 0) AS partner_in_lk_list,
	`status`,
	`billing_pro`,
	`enterprise_not_paid`,
	`enterprise_subs`,
	`enterprise_test`,
	`is_blocked`,
	`kz_pro`
FROM
    (SELECT
		billing_orders_dir_partner.`report_date` AS report_date,
		toStartOfWeek(billing_orders_dir_partner.report_date) AS report_date_week,
		toStartOfMonth(billing_orders_dir_partner.report_date) AS report_date_month,
		toStartOfYear(billing_orders_dir_partner.report_date) AS report_date_year,
		companies_st_partner.`company_name` AS company_name,
		billing_orders_dir_partner.`partner_uuid` as partner_uuid,
		`partner_lk`,
		`tin`,
		`billing_account_id`,
		`comment`,
		lower(arrayElement(splitByString(' ', comment), 1)) AS comment_type,
		`kpp`,
		`service`,
		`billing_cost`,
		`billing_sum`,
		`amount` as accruals_amount,
		`type` as type_of_acruals,
		`pro_subs`,
		`status`,
		`billing_pro`,
		`enterprise_not_paid`,
		`enterprise_subs`,
		`enterprise_test`,
		`is_blocked`,
		`kz_pro`,
		if(`type`= 'partner_program', amount, null) AS partner_program_accruals_amount,
		sumIf(amount, type = 'partner_program') OVER (
		    PARTITION BY companies_st_partner.partner_lk, toStartOfMonth(billing_orders_dir_partner.report_date)
		) AS partner_program_accruals_amount2
	FROM
		(SELECT
			SUM(`total`) as billing_sum,
			toDate(created_at) AS report_date,
			`billing_account_id`,
		    `partner_uuid`,
		    `service`,
		    `cost` as billing_cost
		FROM db1.`billing_orders_dir_partner`
		WHERE state = 'success' 
		GROUP BY  toDate(`created_at`),`partner_uuid`,`billing_account_id`,`service` ,`cost`
		) AS billing_orders_dir_partner
		LEFT JOIN db1.`accruals_dir_partner` AS accruals_dir_partner 
	                ON billing_orders_dir_partner.partner_uuid = accruals_dir_partner.partner_uuid 
	                AND accruals_dir_partner.created_at =  billing_orders_dir_partner.report_date
		LEFT JOIN
		(SELECT 
		    `report_date`, 
		    `partner_uuid`, 
		    `is_blocked`,
		    `pro_subs`,
		    `enterprise_subs`,
		    `billing_pro`,
		    `enterprise_not_paid`,
		    `enterprise_test`,
		    `company_name`, 
		    `partner_lk`, 
		    `tin`,
		    `kpp`,
		    `kz_pro`,
		    CAST(CASE 
		        WHEN (`enterprise_subs` = 1 or `enterprise_not_paid` = 1) THEN 'Enterprise' 
		        WHEN (`pro_subs` = 1) THEN 'PRO' 
		        ELSE 'Start' 
		    END  AS String) AS `status`
		FROM db1.`companies_st_partner`
		LEFT JOIN db1.`companies_dir_partner` ON companies_dir_partner.partner_uuid = companies_st_partner.partner_uuid
		) AS companies_st_partner
			        ON billing_orders_dir_partner.partner_uuid = companies_st_partner.partner_uuid
	                AND billing_orders_dir_partner.report_date = companies_st_partner.report_date
		)
	)
WHERE 
    partner_uuid not in (
    '0e7236ac-b8bd-4ab0-8634-d165ad17190e',/*цифрал сервис ук 120345*/
    'fadd874c-d976-4710-9502-332e8eb21f26', --Тест NikolayCorporation
    'af1dd075-8be1-4b9c-ac96-a329a41e115a', -- ООО Антей-Сервис
    'adeffb47-1707-4976-a4a6-e6eacf19b050', -- город Эксперт 120417
    'e7e86c75-fc8b-4710-839d-56ad86f43211',/*Цифрал Тольятти 152762*/
    'b1782e4f-9198-49d1-b5aa-7bdba9c87d21', -- Спутник
    '01e62154-20f3-4da5-a604-898837d07026',
    '5ade3d03-bb22-4949-bdb3-4487bb74c614',
    '71158d5e-acf9-442f-97eb-2734e262a3c3',
    '6d02e1a5-cc82-4002-b0e3-0d8ee13b453c',
    '2e521201-02bf-47e9-8e5c-e1cb5e210b6e',
    '95924db0-03ba-4ce5-9bfb-24acd59932f3',
    '4b87ac6d-6478-4bb2-b630-3d06abb8ef28',
    'fecd4132-1d4e-410f-b17b-0c7c31d7350e',
    'd3efacc6-8c92-4bac-beb7-03a77b1bc6d3',
    '72269b49-e10c-44a1-b3f4-a588893cd5da',
    '3759e965-ef59-44dd-973d-9defb789e1e2',
    'b3b9b143-a78d-4ad8-bb9f-06613c5db6fd',
    '3d369790-9565-43b9-9646-3ed63be84e8c',
    '59e2b308-bfc1-467e-b677-8b70b3edcaf3',
    '76075a43-5b8f-4b67-ac2b-23fd82abc26c',
    '5ce291d3-9672-4858-99fb-c42ff14b16b0',
    '115b2856-5719-4ea1-8119-075788962eb0',
    'a2c3cb55-8369-46f7-b091-04ef226f46dc',
    '573e40f8-86dc-4228-9301-477bbb8c0ed3',
    '3bbd7565-a843-4b0f-96fc-232df48baf75',
    'e4be161c-bd52-464e-8120-2815befdd23c',
    )


```
