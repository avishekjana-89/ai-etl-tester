CREATE DATABASE IF NOT EXISTS target_db;
USE target_db;

CREATE TABLE `sale_fact` (
  `sale_sk` char(32) NOT NULL,
  `cust_sk` bigint DEFAULT NULL,
  `prod_sk` bigint DEFAULT NULL,
  `period_key` int DEFAULT NULL,
  `gross_amt` decimal(12,2) DEFAULT NULL,
  `net_amt` decimal(12,2) DEFAULT NULL,
  `geo_sk` tinyint DEFAULT NULL,
  `etl_batch` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`sale_sk`)
);

INSERT INTO `sale_fact` (`sale_sk`, `cust_sk`, `prod_sk`, `period_key`, `gross_amt`, `net_amt`, `geo_sk`, `etl_batch`) VALUES 
('00ac8ed3b4327bdd4ebbebcb2ba10a00', 10, 5, 202512, 95.4, 78.05, NULL, NULL),
('1ff1de774005f8da13f42943881c655f', 4, 8, 202512, 14.18, 12.93, NULL, NULL),
('65b9eea6e1cc6bb9f0cd2a47751a186f', 5, 2, 202601, 75.16, 67.34, NULL, NULL),
('6c8349cc7260ae62e3b1396831a8398f', 5, 3, 202512, 152.1, 137.04, NULL, NULL),
('70efdf2ec9b086079795c442636b55fb', 7, 4, 202512, 316.6, 273.27, NULL, NULL),
('72b32a1f754ba1c09b3695e0cb6cde7f', 7, 6, 202512, 142.65, 141.67, NULL, NULL),
('a5771bce93e200c36f7cd9dfd0e5deaa', 8, 8, 202512, 646.8, 631.01, NULL, NULL),
('c7e1249ffc03eb9ded908c236bd1996d', 7, 8, 202601, 248.28, 215.38, NULL, NULL),
('d2ddea18f00665ce8623e36bd4e3c7c5', 3, 2, 202512, 969.1, 918.94, NULL, NULL),
('ed3d2c21991e3bef5e069713af9fa6ca', 8, 6, 202601, 261.94, 245.61, NULL, NULL);

CREATE TABLE `products_dim` (
  `prod_sk` bigint NOT NULL AUTO_INCREMENT,
  `prod_id` int DEFAULT NULL,
  `prod_name` varchar(100) DEFAULT NULL,
  `cat_name` varchar(20) DEFAULT NULL,
  `base_price` decimal(10,2) DEFAULT NULL,
  PRIMARY KEY (`prod_sk`)
);

INSERT INTO `products_dim` (`prod_sk`, `prod_id`, `prod_name`, `cat_name`, `base_price`) VALUES 
(1, 1, NULL, 'Cat_1', NULL),
(2, 2, NULL, 'Cat_2', NULL),
(3, 3, NULL, 'Cat_3', NULL),
(4, 4, NULL, 'Cat_4', NULL),
(5, 5, NULL, 'Cat_5', NULL),
(6, 6, NULL, 'Cat_6', NULL),
(7, 7, NULL, 'Cat_7', NULL),
(8, 8, NULL, 'Cat_8', NULL),
(9, 9, NULL, 'Cat_9', NULL),
(10, 10, NULL, 'Cat_10', NULL);

CREATE TABLE `geo_dim` (
  `geo_sk` tinyint NOT NULL AUTO_INCREMENT,
  `region_name` varchar(20) DEFAULT NULL,
  `zone_code` char(2) DEFAULT NULL,
  PRIMARY KEY (`geo_sk`)
);

INSERT INTO `geo_dim` (`geo_sk`, `region_name`, `zone_code`) VALUES 
(1, 'North', 'NR'),
(2, 'South', 'SR'),
(3, 'East', 'ER');

CREATE TABLE `customers_dim` (
  `cust_sk` bigint NOT NULL AUTO_INCREMENT,
  `cust_id` int DEFAULT NULL,
  `cust_name` varchar(100) DEFAULT NULL,
  `region` varchar(2) DEFAULT NULL,
  `valid_from` date DEFAULT NULL,
  `valid_to` date DEFAULT '9999-12-31',
  `is_current` tinyint(1) DEFAULT '1',
  PRIMARY KEY (`cust_sk`)
);

INSERT INTO `customers_dim` (`cust_sk`, `cust_id`, `cust_name`, `region`, `valid_from`, `valid_to`, `is_current`) VALUES 
(1, 1, 'Acme Corp', 'NR', '2025-12-01', '9999-12-31', 1),
(2, 2, 'Tech Ltd', 'SR', '2025-12-01', '9999-12-31', 1),
(3, 3, 'Global Inc', 'ER', '2025-12-01', '9999-12-31', 1),
(4, 4, 'RetailMax', 'NR', '2025-12-01', '9999-12-31', 1),
(5, 5, 'DataWorks', 'SR', '2025-12-01', '9999-12-31', 1),
(6, 6, 'CloudPeak', 'ER', '2025-12-01', '9999-12-31', 1),
(7, 7, 'SysNova', 'NR', '2025-12-01', '9999-12-31', 1),
(8, 8, 'InfoGrid', 'SR', '2025-12-01', '9999-12-31', 1),
(9, 9, 'NetForge', 'ER', '2025-12-01', '9999-12-31', 1),
(10, 10, 'ByteZone', 'NR', '2025-12-01', '9999-12-31', 1),
(11, 11, 'Acme Corp Updated', 'NR', '2026-02-01', '9999-12-31', 1),
(12, 12, 'Acme Corp', 'NR', '2025-12-01', '2026-02-01', 0);
