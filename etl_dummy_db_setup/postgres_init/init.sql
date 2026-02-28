CREATE SCHEMA IF NOT EXISTS sales_db;

CREATE TABLE sales_db.customers (
	cust_id int4 NOT NULL,
	cust_name varchar(100) NULL,
	region varchar(2) NULL,
	join_date date NULL,
	CONSTRAINT customers_pkey PRIMARY KEY (cust_id)
);

INSERT INTO sales_db.customers (cust_id, cust_name, region, join_date) VALUES 
(1, NULL, 'SR', NULL),
(2, NULL, 'SR', NULL),
(3, NULL, 'NR', NULL),
(4, NULL, 'ER', NULL),
(5, NULL, 'SR', NULL),
(6, NULL, 'SR', NULL),
(7, NULL, 'SR', NULL),
(8, NULL, 'SR', NULL),
(9, NULL, 'SR', NULL),
(10, NULL, 'SR', NULL);

CREATE TABLE sales_db.sales (
	sale_id int8 NOT NULL,
	cust_id int4 NULL,
	prod_id int4 NULL,
	qty int4 NULL,
	unit_price numeric(10, 2) NULL,
	sale_date date NULL,
	disc_pct numeric(5, 4) NULL,
	created_ts timestamp NULL DEFAULT now(),
	CONSTRAINT sales_pkey PRIMARY KEY (sale_id)
);

INSERT INTO sales_db.sales (sale_id, cust_id, prod_id, qty, unit_price, sale_date, disc_pct, created_ts) VALUES
(1, 7, 4, 5, 63.32, '2025-12-01', 0.1368, '2026-02-22 16:06:58.151'),
(2, 4, 8, 1, 14.18, '2025-12-06', 0.0880, '2026-02-22 16:06:58.151'),
(3, 8, 8, 10, 64.68, '2025-12-11', 0.0244, '2026-02-22 16:06:58.151'),
(4, 5, 3, 6, 25.35, '2025-12-16', 0.0990, '2026-02-22 16:06:58.151'),
(5, 7, 6, 9, 15.85, '2025-12-21', 0.0069, '2026-02-22 16:06:58.151'),
(6, 10, 5, 1, 95.40, '2025-12-26', 0.1819, '2026-02-22 16:06:58.151'),
(7, 3, 2, 10, 96.91, '2025-12-31', 0.0518, '2026-02-22 16:06:58.151'),
(8, 7, 8, 3, 82.76, '2026-01-05', 0.1325, '2026-02-22 16:06:58.151'),
(9, 8, 6, 7, 37.42, '2026-01-10', 0.0623, '2026-02-22 16:06:58.151'),
(10, 5, 2, 4, 18.79, '2026-01-15', 0.1040, '2026-02-22 16:06:58.151');

CREATE TABLE sales_db.products (
	prod_id int4 NOT NULL,
	prod_name varchar(100) NULL,
	category varchar(30) NULL,
	unit_price numeric(10, 2) NULL,
	created_date date NULL,
	CONSTRAINT products_pkey PRIMARY KEY (prod_id)
);

INSERT INTO sales_db.products (prod_id, prod_name, category, unit_price, created_date) VALUES 
(1, NULL, 'Cat_1', NULL, NULL),
(2, NULL, 'Cat_2', NULL, NULL),
(3, NULL, 'Cat_3', NULL, NULL),
(4, NULL, 'Cat_4', NULL, NULL),
(5, NULL, 'Cat_5', NULL, NULL),
(6, NULL, 'Cat_6', NULL, NULL),
(7, NULL, 'Cat_7', NULL, NULL),
(8, NULL, 'Cat_8', NULL, NULL),
(9, NULL, 'Cat_9', NULL, NULL),
(10, NULL, 'Cat_10', NULL, NULL);
