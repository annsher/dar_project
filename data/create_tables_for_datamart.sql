CREATE SCHEMA IF NOT EXISTS datamart AUTHORIZATION interns_4;

CREATE TABLE IF NOT EXISTS datamart.pos (
	pos varchar(255) NULL,
	pos_name varchar(255) NOT NULL,
	CONSTRAINT pos_pk PRIMARY KEY (pos_name)
);

CREATE TABLE IF NOT EXISTS datamart.category (
	category_name varchar(255) NOT NULL,
	category_id varchar(255) NULL,
	CONSTRAINT category_pk PRIMARY KEY (category_name)
);

CREATE TABLE IF NOT EXISTS datamart.months (
	"month" varchar NOT NULL,
	CONSTRAINT months_pk PRIMARY KEY (month)
);

CREATE TABLE IF NOT EXISTS datamart.years (
	"year" int4 NOT NULL,
	CONSTRAINT years_pk PRIMARY KEY (year)
);

CREATE TABLE IF NOT EXISTS datamart.avg_purchase (
	recorded_on date NULL,
	pos_name varchar(255) NULL,
	revenue_daily numeric NULL,
	avg_amount numeric NULL,
	category_name varchar(255) NULL,
	CONSTRAINT avg_purchase_fk FOREIGN KEY (pos_name) REFERENCES datamart.pos(pos_name),
	CONSTRAINT avg_purchase_fk_1 FOREIGN KEY (category_name) REFERENCES datamart.category(category_name)
);

CREATE TABLE IF NOT EXISTS datamart.avg_purchase_month (
	"month" varchar(255) NULL,
	avg_amount_month numeric NULL,
	pos_name varchar(255) NULL,
	"year" int4 NULL,
	CONSTRAINT avg_purchase_month_fk FOREIGN KEY (pos_name) REFERENCES datamart.pos(pos_name),
	CONSTRAINT avg_purchase_month_fk_1 FOREIGN KEY ("year") REFERENCES datamart.years("year"),
	CONSTRAINT avg_purchase_month_fk_2 FOREIGN KEY ("month") REFERENCES datamart.months("month")
);

CREATE TABLE IF NOT EXISTS datamart.revenue_month (
	"month" varchar(255) NULL,
	"year" int4 NULL,
	category_name varchar(255) NULL,
	revenue numeric NULL,
	pos_name varchar(255) NULL,
	CONSTRAINT revenue_month_fk FOREIGN KEY (pos_name) REFERENCES datamart.pos(pos_name),
	CONSTRAINT revenue_month_fk_1 FOREIGN KEY (category_name) REFERENCES datamart.category(category_name),
	CONSTRAINT revenue_month_fk_2 FOREIGN KEY ("month") REFERENCES datamart.months("month"),
	CONSTRAINT revenue_month_fk_3 FOREIGN KEY ("year") REFERENCES datamart.years("year")
);

CREATE TABLE IF NOT EXISTS datamart.sold_item (
	recorded_on date NULL,
	qnt int4 NULL,
	pos_name varchar(255) NULL,
	category_name varchar(255) NULL,
	CONSTRAINT sold_item_fk FOREIGN KEY (pos_name) REFERENCES datamart.pos(pos_name),
	CONSTRAINT sold_item_fk_1 FOREIGN KEY (category_name) REFERENCES datamart.category(category_name)
);

CREATE TABLE IF NOT EXISTS datamart.sold_item_month (
	"month" varchar(255) NULL,
	qnt_month int4 NULL,
	pos_name varchar(255) NULL,
	category_name varchar(255) NULL,
	"year" int4 NULL,
	CONSTRAINT sold_item_month_fk FOREIGN KEY (pos_name) REFERENCES datamart.pos(pos_name),
	CONSTRAINT sold_item_month_fk_1 FOREIGN KEY (category_name) REFERENCES datamart.category(category_name),
	CONSTRAINT sold_item_month_fk_2 FOREIGN KEY ("month") REFERENCES datamart.months("month"),
	CONSTRAINT sold_item_month_fk_3 FOREIGN KEY ("year") REFERENCES datamart.years("year")
);

CREATE TABLE IF NOT EXISTS datamart.to_order (
	name_short varchar(255) NULL,
	category_name varchar(255) NULL,
	available_quantity int4 NULL,
	pos_name varchar(255) NULL,
	"order" varchar NULL,
	"limit" int4 NULL,
	brand varchar(255) NULL
);

CREATE TABLE IF NOT EXISTS datamart.top_5 (
	name_short varchar(255) NULL,
	category_name varchar(255) NULL,
	"month" varchar(255) NULL,
	"year" int4 NULL,
	qnt_sum int4 NULL,
	pos_name varchar(255) NULL,
	brand varchar(255) NULL,
	CONSTRAINT top_5_fk FOREIGN KEY (pos_name) REFERENCES datamart.pos(pos_name),
	CONSTRAINT top_5_fk_1 FOREIGN KEY (category_name) REFERENCES datamart.category(category_name),
	CONSTRAINT top_5_fk_2 FOREIGN KEY ("month") REFERENCES datamart.months("month"),
	CONSTRAINT top_5_fk_3 FOREIGN KEY ("year") REFERENCES datamart.years("year")
);



CREATE TABLE IF NOT EXISTS datamart.updated_on (
	"date" date NULL
);


