CREATE TABLE promotion_dim (
  promotion_id INTEGER UNSIGNED  NOT NULL   AUTO_INCREMENT,
  promotion_name VARCHAR(200)  NOT NULL  ,
  discount_pct FLOAT  NOT NULL    ,
PRIMARY KEY(promotion_id));



CREATE TABLE sales_channel_dim (
  sales_channel_id INTEGER UNSIGNED  NOT NULL   AUTO_INCREMENT,
  sales_channel_name VARCHAR(100)  NOT NULL    ,
PRIMARY KEY(sales_channel_id)  ,
UNIQUE INDEX sales_channel_dim_index1048(sales_channel_name));



CREATE TABLE supplier_dim (
  supplier_id INTEGER UNSIGNED  NOT NULL   AUTO_INCREMENT,
  supplier_name VARCHAR(200)  NOT NULL  ,
  supplier_address VARCHAR(500)  NULL  ,
  supplier_phone VARCHAR(100)  NULL    ,
PRIMARY KEY(supplier_id)  ,
UNIQUE INDEX supplier_dim_index1043(supplier_name));



CREATE TABLE product_dim (
  product_id INTEGER UNSIGNED  NOT NULL   AUTO_INCREMENT,
  product_name VARCHAR(200)  NOT NULL  ,
  product_model VARCHAR(100)  NOT NULL  ,
  product_category VARCHAR(100)  NOT NULL  ,
  product_cost VARCHAR(10)  NOT NULL    ,
PRIMARY KEY(product_id)  ,
UNIQUE INDEX product_dim_index1010(product_name, product_category, product_model, product_cost));



CREATE TABLE customer_dim (
  customer_id INTEGER UNSIGNED  NOT NULL   AUTO_INCREMENT,
  customer_address VARCHAR(500)  NOT NULL  ,
  customer_name VARCHAR(200)  NOT NULL  ,
  customer_country VARCHAR(200)  NOT NULL  ,
  customer_phone VARCHAR(100)  NULL    ,
PRIMARY KEY(customer_id)  ,
UNIQUE INDEX customer_dim_index1018(customer_name, customer_address, customer_country, customer_phone));



CREATE TABLE date_dim (
  date_id INTEGER UNSIGNED  NOT NULL   AUTO_INCREMENT,
  date_val DATE  NOT NULL  ,
  month_num INTEGER UNSIGNED  NOT NULL  ,
  year_num INTEGER UNSIGNED  NOT NULL    ,
PRIMARY KEY(date_id));

insert into date_dim values (20140101,str_to_date(20140101,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140102,str_to_date(20140102,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140103,str_to_date(20140103,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140104,str_to_date(20140104,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140105,str_to_date(20140105,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140106,str_to_date(20140106,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140107,str_to_date(20140107,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140108,str_to_date(20140108,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140109,str_to_date(20140109,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140110,str_to_date(20140110,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140111,str_to_date(20140111,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140112,str_to_date(20140112,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140113,str_to_date(20140113,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140114,str_to_date(20140114,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140115,str_to_date(20140115,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140116,str_to_date(20140116,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140117,str_to_date(20140117,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140118,str_to_date(20140118,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140119,str_to_date(20140119,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140120,str_to_date(20140120,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140121,str_to_date(20140121,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140122,str_to_date(20140122,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140123,str_to_date(20140123,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140124,str_to_date(20140124,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140125,str_to_date(20140125,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140126,str_to_date(20140126,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140127,str_to_date(20140127,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140128,str_to_date(20140128,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140129,str_to_date(20140129,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140130,str_to_date(20140130,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140131,str_to_date(20140131,'%Y%m%d'), 1, 2014);
insert into date_dim values (20140201,str_to_date(20140201,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140202,str_to_date(20140202,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140203,str_to_date(20140203,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140204,str_to_date(20140204,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140205,str_to_date(20140205,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140206,str_to_date(20140206,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140207,str_to_date(20140207,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140208,str_to_date(20140208,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140209,str_to_date(20140209,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140210,str_to_date(20140210,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140211,str_to_date(20140211,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140212,str_to_date(20140212,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140213,str_to_date(20140213,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140214,str_to_date(20140214,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140215,str_to_date(20140215,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140216,str_to_date(20140216,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140217,str_to_date(20140217,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140218,str_to_date(20140218,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140219,str_to_date(20140219,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140220,str_to_date(20140220,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140221,str_to_date(20140221,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140222,str_to_date(20140222,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140223,str_to_date(20140223,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140224,str_to_date(20140224,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140225,str_to_date(20140225,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140226,str_to_date(20140226,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140227,str_to_date(20140227,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140228,str_to_date(20140228,'%Y%m%d'), 2, 2014);
insert into date_dim values (20140301,str_to_date(20140301,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140302,str_to_date(20140302,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140303,str_to_date(20140303,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140304,str_to_date(20140304,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140305,str_to_date(20140305,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140306,str_to_date(20140306,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140307,str_to_date(20140307,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140308,str_to_date(20140308,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140309,str_to_date(20140309,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140310,str_to_date(20140310,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140311,str_to_date(20140311,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140312,str_to_date(20140312,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140313,str_to_date(20140313,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140314,str_to_date(20140314,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140315,str_to_date(20140315,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140316,str_to_date(20140316,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140317,str_to_date(20140317,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140318,str_to_date(20140318,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140319,str_to_date(20140319,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140320,str_to_date(20140320,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140321,str_to_date(20140321,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140322,str_to_date(20140322,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140323,str_to_date(20140323,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140324,str_to_date(20140324,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140325,str_to_date(20140325,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140326,str_to_date(20140326,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140327,str_to_date(20140327,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140328,str_to_date(20140328,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140329,str_to_date(20140329,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140330,str_to_date(20140330,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140331,str_to_date(20140331,'%Y%m%d'), 3, 2014);
insert into date_dim values (20140401,str_to_date(20140401,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140402,str_to_date(20140402,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140403,str_to_date(20140403,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140404,str_to_date(20140404,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140405,str_to_date(20140405,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140406,str_to_date(20140406,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140407,str_to_date(20140407,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140408,str_to_date(20140408,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140409,str_to_date(20140409,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140410,str_to_date(20140410,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140411,str_to_date(20140411,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140412,str_to_date(20140412,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140413,str_to_date(20140413,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140414,str_to_date(20140414,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140415,str_to_date(20140415,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140416,str_to_date(20140416,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140417,str_to_date(20140417,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140418,str_to_date(20140418,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140419,str_to_date(20140419,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140420,str_to_date(20140420,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140421,str_to_date(20140421,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140422,str_to_date(20140422,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140423,str_to_date(20140423,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140424,str_to_date(20140424,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140425,str_to_date(20140425,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140426,str_to_date(20140426,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140427,str_to_date(20140427,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140428,str_to_date(20140428,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140429,str_to_date(20140429,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140430,str_to_date(20140430,'%Y%m%d'), 4, 2014);
insert into date_dim values (20140501,str_to_date(20140501,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140502,str_to_date(20140502,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140503,str_to_date(20140503,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140504,str_to_date(20140504,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140505,str_to_date(20140505,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140506,str_to_date(20140506,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140507,str_to_date(20140507,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140508,str_to_date(20140508,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140509,str_to_date(20140509,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140510,str_to_date(20140510,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140511,str_to_date(20140511,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140512,str_to_date(20140512,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140513,str_to_date(20140513,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140514,str_to_date(20140514,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140515,str_to_date(20140515,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140516,str_to_date(20140516,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140517,str_to_date(20140517,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140518,str_to_date(20140518,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140519,str_to_date(20140519,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140520,str_to_date(20140520,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140521,str_to_date(20140521,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140522,str_to_date(20140522,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140523,str_to_date(20140523,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140524,str_to_date(20140524,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140525,str_to_date(20140525,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140526,str_to_date(20140526,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140527,str_to_date(20140527,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140528,str_to_date(20140528,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140529,str_to_date(20140529,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140530,str_to_date(20140530,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140531,str_to_date(20140531,'%Y%m%d'), 5, 2014);
insert into date_dim values (20140601,str_to_date(20140601,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140602,str_to_date(20140602,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140603,str_to_date(20140603,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140604,str_to_date(20140604,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140605,str_to_date(20140605,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140606,str_to_date(20140606,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140607,str_to_date(20140607,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140608,str_to_date(20140608,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140609,str_to_date(20140609,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140610,str_to_date(20140610,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140611,str_to_date(20140611,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140612,str_to_date(20140612,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140613,str_to_date(20140613,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140614,str_to_date(20140614,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140615,str_to_date(20140615,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140616,str_to_date(20140616,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140617,str_to_date(20140617,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140618,str_to_date(20140618,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140619,str_to_date(20140619,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140620,str_to_date(20140620,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140621,str_to_date(20140621,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140622,str_to_date(20140622,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140623,str_to_date(20140623,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140624,str_to_date(20140624,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140625,str_to_date(20140625,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140626,str_to_date(20140626,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140627,str_to_date(20140627,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140628,str_to_date(20140628,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140629,str_to_date(20140629,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140630,str_to_date(20140630,'%Y%m%d'), 6, 2014);
insert into date_dim values (20140701,str_to_date(20140701,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140702,str_to_date(20140702,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140703,str_to_date(20140703,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140704,str_to_date(20140704,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140705,str_to_date(20140705,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140706,str_to_date(20140706,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140707,str_to_date(20140707,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140708,str_to_date(20140708,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140709,str_to_date(20140709,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140710,str_to_date(20140710,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140711,str_to_date(20140711,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140712,str_to_date(20140712,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140713,str_to_date(20140713,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140714,str_to_date(20140714,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140715,str_to_date(20140715,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140716,str_to_date(20140716,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140717,str_to_date(20140717,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140718,str_to_date(20140718,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140719,str_to_date(20140719,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140720,str_to_date(20140720,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140721,str_to_date(20140721,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140722,str_to_date(20140722,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140723,str_to_date(20140723,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140724,str_to_date(20140724,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140725,str_to_date(20140725,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140726,str_to_date(20140726,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140727,str_to_date(20140727,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140728,str_to_date(20140728,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140729,str_to_date(20140729,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140730,str_to_date(20140730,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140731,str_to_date(20140731,'%Y%m%d'), 7, 2014);
insert into date_dim values (20140801,str_to_date(20140801,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140802,str_to_date(20140802,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140803,str_to_date(20140803,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140804,str_to_date(20140804,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140805,str_to_date(20140805,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140806,str_to_date(20140806,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140807,str_to_date(20140807,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140808,str_to_date(20140808,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140809,str_to_date(20140809,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140810,str_to_date(20140810,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140811,str_to_date(20140811,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140812,str_to_date(20140812,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140813,str_to_date(20140813,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140814,str_to_date(20140814,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140815,str_to_date(20140815,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140816,str_to_date(20140816,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140817,str_to_date(20140817,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140818,str_to_date(20140818,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140819,str_to_date(20140819,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140820,str_to_date(20140820,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140821,str_to_date(20140821,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140822,str_to_date(20140822,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140823,str_to_date(20140823,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140824,str_to_date(20140824,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140825,str_to_date(20140825,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140826,str_to_date(20140826,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140827,str_to_date(20140827,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140828,str_to_date(20140828,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140829,str_to_date(20140829,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140830,str_to_date(20140830,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140831,str_to_date(20140831,'%Y%m%d'), 8, 2014);
insert into date_dim values (20140901,str_to_date(20140901,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140902,str_to_date(20140902,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140903,str_to_date(20140903,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140904,str_to_date(20140904,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140905,str_to_date(20140905,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140906,str_to_date(20140906,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140907,str_to_date(20140907,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140908,str_to_date(20140908,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140909,str_to_date(20140909,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140910,str_to_date(20140910,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140911,str_to_date(20140911,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140912,str_to_date(20140912,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140913,str_to_date(20140913,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140914,str_to_date(20140914,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140915,str_to_date(20140915,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140916,str_to_date(20140916,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140917,str_to_date(20140917,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140918,str_to_date(20140918,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140919,str_to_date(20140919,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140920,str_to_date(20140920,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140921,str_to_date(20140921,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140922,str_to_date(20140922,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140923,str_to_date(20140923,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140924,str_to_date(20140924,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140925,str_to_date(20140925,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140926,str_to_date(20140926,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140927,str_to_date(20140927,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140928,str_to_date(20140928,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140929,str_to_date(20140929,'%Y%m%d'), 9, 2014);
insert into date_dim values (20140930,str_to_date(20140930,'%Y%m%d'), 9, 2014);
insert into date_dim values (20141001,str_to_date(20141001,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141002,str_to_date(20141002,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141003,str_to_date(20141003,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141004,str_to_date(20141004,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141005,str_to_date(20141005,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141006,str_to_date(20141006,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141007,str_to_date(20141007,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141008,str_to_date(20141008,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141009,str_to_date(20141009,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141010,str_to_date(20141010,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141011,str_to_date(20141011,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141012,str_to_date(20141012,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141013,str_to_date(20141013,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141014,str_to_date(20141014,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141015,str_to_date(20141015,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141016,str_to_date(20141016,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141017,str_to_date(20141017,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141018,str_to_date(20141018,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141019,str_to_date(20141019,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141020,str_to_date(20141020,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141021,str_to_date(20141021,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141022,str_to_date(20141022,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141023,str_to_date(20141023,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141024,str_to_date(20141024,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141025,str_to_date(20141025,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141026,str_to_date(20141026,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141027,str_to_date(20141027,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141028,str_to_date(20141028,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141029,str_to_date(20141029,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141030,str_to_date(20141030,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141031,str_to_date(20141031,'%Y%m%d'), 10, 2014);
insert into date_dim values (20141101,str_to_date(20141101,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141102,str_to_date(20141102,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141103,str_to_date(20141103,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141104,str_to_date(20141104,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141105,str_to_date(20141105,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141106,str_to_date(20141106,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141107,str_to_date(20141107,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141108,str_to_date(20141108,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141109,str_to_date(20141109,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141110,str_to_date(20141110,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141111,str_to_date(20141111,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141112,str_to_date(20141112,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141113,str_to_date(20141113,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141114,str_to_date(20141114,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141115,str_to_date(20141115,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141116,str_to_date(20141116,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141117,str_to_date(20141117,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141118,str_to_date(20141118,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141119,str_to_date(20141119,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141120,str_to_date(20141120,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141121,str_to_date(20141121,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141122,str_to_date(20141122,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141123,str_to_date(20141123,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141124,str_to_date(20141124,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141125,str_to_date(20141125,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141126,str_to_date(20141126,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141127,str_to_date(20141127,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141128,str_to_date(20141128,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141129,str_to_date(20141129,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141130,str_to_date(20141130,'%Y%m%d'), 11, 2014);
insert into date_dim values (20141201,str_to_date(20141201,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141202,str_to_date(20141202,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141203,str_to_date(20141203,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141204,str_to_date(20141204,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141205,str_to_date(20141205,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141206,str_to_date(20141206,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141207,str_to_date(20141207,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141208,str_to_date(20141208,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141209,str_to_date(20141209,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141210,str_to_date(20141210,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141211,str_to_date(20141211,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141212,str_to_date(20141212,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141213,str_to_date(20141213,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141214,str_to_date(20141214,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141215,str_to_date(20141215,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141216,str_to_date(20141216,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141217,str_to_date(20141217,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141218,str_to_date(20141218,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141219,str_to_date(20141219,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141220,str_to_date(20141220,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141221,str_to_date(20141221,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141222,str_to_date(20141222,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141223,str_to_date(20141223,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141224,str_to_date(20141224,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141225,str_to_date(20141225,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141226,str_to_date(20141226,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141227,str_to_date(20141227,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141228,str_to_date(20141228,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141229,str_to_date(20141229,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141230,str_to_date(20141230,'%Y%m%d'), 12, 2014);
insert into date_dim values (20141231,str_to_date(20141231,'%Y%m%d'), 12, 2014);



CREATE TABLE employee_dim (
  employee_id INTEGER UNSIGNED  NOT NULL   AUTO_INCREMENT,
  employee_name VARCHAR(100)  NOT NULL  ,
  employee_number INTEGER UNSIGNED  NOT NULL    ,
PRIMARY KEY(employee_id)  ,
INDEX employee_dim_index1036(employee_name));



CREATE TABLE sales_fact (
  employee_id INTEGER UNSIGNED  NOT NULL  ,
  customer_id INTEGER UNSIGNED  NOT NULL  ,
  product_id INTEGER UNSIGNED  NOT NULL  ,
  sales_channel_id INTEGER UNSIGNED  NOT NULL  ,
  promotion_id INTEGER UNSIGNED  NOT NULL  ,
  supplier_id INTEGER UNSIGNED  NOT NULL  ,
  booking_date_id INTEGER UNSIGNED  NOT NULL  ,
  sales_date_id INTEGER UNSIGNED  NOT NULL  ,
  delivery_date_id INTEGER UNSIGNED  NOT NULL  ,
  priceBeforeDiscount FLOAT  NOT NULL  ,
  priceAfterDiscount FLOAT  NOT NULL  ,
  saleCosts FLOAT  NOT NULL);

create table sh_metrics (
servername varchar(100) null,
instancenumber integer null,
instancestarted varchar(100) null,
eventName varchar(100) null,
    fileReceived bigint null,
etlThreadID integer null,
fileName varchar(100) null,
    fileProcessingStart bigint null,
    fileProcessingFinish bigint null,
    fileJdbcInsertStart bigint null,
    fileJdbcInsertFinish bigint null,
bulkFileSubmitted varchar(100) null,
dbThreadID integer null,
bulkFilePath varchar(200) null,
bulkFileName varchar(100) null,
fileRecordCount integer null,
    bulkFileReceived bigint null,
    bulkFileProcessingStart bigint null,
    bulkFileProcessingFinish bigint null,
completionFlag varchar(100) null,
errorDescription varchar(1000) null
);


