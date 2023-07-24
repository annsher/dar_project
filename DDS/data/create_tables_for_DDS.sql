CREATE SCHEMA IF NOT EXISTS "DDS";

CREATE TABLE IF NOT EXISTS "DDS".error (
	"data" varchar(255) NULL,
	error varchar(255) NULL,
	table_name varchar(255) NULL
);

CREATE TABLE IF NOT EXISTS "DDS".brand (
	brand varchar(255) NULL,
	brand_id int4 NOT NULL,
	CONSTRAINT brand_pk PRIMARY KEY (brand_id)
);

CREATE TABLE IF NOT EXISTS "DDS".category (
	category_id varchar(255) NOT NULL,
	category_name varchar(255) NULL,
	CONSTRAINT category_pk PRIMARY KEY (category_id)
);

CREATE TABLE IF NOT EXISTS "DDS".pos (
	pos varchar(255) NOT NULL,
	pos_name varchar(255) NULL,
	CONSTRAINT pos_pk PRIMARY KEY (pos)
);

CREATE TABLE IF NOT EXISTS "DDS".transaction_pos (
	transaction_id varchar(255) NULL,
	pos_name varchar(255) NULL
);

CREATE TABLE IF NOT EXISTS "DDS".product (
	product_id int4 NOT NULL,
	name_short varchar(255) NULL,
	category_id varchar(255) NULL,
	pricing_line_id int4 NULL,
	brand_id int4 NULL,
	CONSTRAINT product_pk PRIMARY KEY (product_id),
	CONSTRAINT product_fk_brand FOREIGN KEY (brand_id) REFERENCES "DDS".brand(brand_id),
	CONSTRAINT product_fk_category FOREIGN KEY (category_id) REFERENCES "DDS".category(category_id)
);

CREATE TABLE IF NOT EXISTS "DDS".stock (
	available_on date NOT NULL,
	product_id int4 NOT NULL,
	pos varchar(255) NOT NULL,
	available_quantity int4 NULL,
	cost_per_item numeric NULL,
	CONSTRAINT stock_pk PRIMARY KEY (available_on, product_id, pos),
	CONSTRAINT stock_fk_pos FOREIGN KEY (pos) REFERENCES "DDS".pos(pos),
	CONSTRAINT stock_fk_product FOREIGN KEY (product_id) REFERENCES "DDS".product(product_id)
);

CREATE TABLE IF NOT EXISTS "DDS"."transaction" (
	transaction_id varchar(255) NOT NULL,
	product_id int4 NOT NULL,
	recorded_on date NULL,
	quantity int4 NULL,
	price numeric NULL,
	price_full numeric NULL,
	order_type_id varchar(255) NULL,
	CONSTRAINT transaction_pk PRIMARY KEY (transaction_id, product_id),
	CONSTRAINT transaction_fk FOREIGN KEY (product_id) REFERENCES "DDS".product(product_id)
);