CREATE SCHEMA IF NOT EXISTS datamart AUTHORIZATION interns_4;

CREATE TABLE IF NOT EXISTS datamart.avg_purchase (
	recorded_on date NULL,
	pos_name varchar(255) NULL,
	revenue_daily numeric NULL,
	avg_amount numeric NULL
);

CREATE TABLE IF NOT EXISTS datamart.avg_purchase_month (
	"month" varchar(255) NULL,
	avg_amount_month numeric NULL,
	pos_name varchar(255) NULL
);

CREATE TABLE IF NOT EXISTS datamart.revenue_month (
	"month" varchar(255) NULL,
	"year" int4 NULL,
	category_name varchar(255) NULL,
	revenue numeric NULL,
	pos_name varchar(255) NULL
);

CREATE TABLE IF NOT EXISTS datamart.sold_item (
	recorded_on date NULL,
	qnt int4 NULL,
	pos_name varchar(255) NULL,
	category_name varchar(255) NULL
);

CREATE TABLE IF NOT EXISTS datamart.sold_item_month (
	"month" varchar(255) NULL,
	qnt_month int4 NULL,
	pos_name varchar(255) NULL,
	category_name varchar(255) NULL
);

CREATE TABLE IF NOT EXISTS datamart.to_order (
	name_short varchar(255) NULL,
	category_name varchar(255) NULL,
	available_quantity int4 NULL,
	pos_name varchar(255) NULL,
	"order" bool NULL,
	"limit" int4 NULL
);

CREATE TABLE IF NOT EXISTS datamart.top_5 (
	name_short varchar(255) NULL,
	category_name varchar(255) NULL,
	"month" varchar(255) NULL,
	"year" int4 NULL,
	qnt_sum int4 NULL,
	pos_name varchar(255) NULL
);

