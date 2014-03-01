/*

NOTE: if you wish to set your data owner please uncomment first command of this script
NOTE: create user with adequate privileges in your Oracle database and execute the script below (just in case you need create user statement: create USER streamhorizon identified by streamhorizon;  GRANT ALL PRIVILEGES TO StreamHorizon;)  please verify with your DBA that this privilege statement complies with your data access policy
NOTE: change create table statement for sales_fact if you wish to load datae in tablespace other than USER tablespace
NOTE: maximum number of StreamHorizon db threads (<bulkProcessingThreadID>) which this model supports is 50. If you wish to run more than 50 db threads please extend subpartition definition of fact table given below 
NOTE: fact table has single partition created for value booking_date_id=20140107. if you wish to run your own rather than StreamHorizon sample data set you may want to create additional partitions. to do so run following statement: ALTER TABLE  WAREHOUSE.RWH_RISK_FACT_SMALL_SUB ADD PARTITION P_20130910 VALUES  (20130910)  COMPRESS BASIC
NOTE: for test purposes fact table sales_fact is created in NOLOGGING mode as low number of LGRW  (logwriters) may impact performance unnecessarily

*/

/* set your dataowner if desired...*/
--alter session set current_schema = <YourDataOwner>; 

CREATE TABLE promotion_dim (
  promotion_id INTEGER   NOT NULL ,
  promotion_name VARCHAR2(200)   NOT NULL ,
  discount_pct FLOAT   NOT NULL   ,
PRIMARY KEY(promotion_id));

CREATE UNIQUE INDEX promotion_nk ON promotion_dim (promotion_name,discount_pct);

CREATE TABLE sales_channel_dim (
  sales_channel_id INTEGER   NOT NULL ,
  sales_channel_name VARCHAR2(100)   NOT NULL   ,
PRIMARY KEY(sales_channel_id)  );

CREATE UNIQUE INDEX sales_channe_nk ON sales_channel_dim (sales_channel_name);

CREATE TABLE supplier_dim (
  supplier_id INTEGER   NOT NULL ,
  supplier_name VARCHAR2(200)   NOT NULL ,
  supplier_address VARCHAR2(500)   NULL  ,
  supplier_phone VARCHAR2(100)  NULL    ,
PRIMARY KEY(supplier_id)  );

CREATE UNIQUE INDEX supplier_nk ON supplier_dim (supplier_name);

CREATE TABLE product_dim (
  product_id INTEGER   NOT NULL ,
  product_name VARCHAR2(200)   NOT NULL ,
  product_model VARCHAR2(100)   NOT NULL ,
  product_category VARCHAR2(100)   NOT NULL ,
  product_cost VARCHAR2(10)   NOT NULL   ,
PRIMARY KEY(product_id)  );

CREATE UNIQUE INDEX product_nk ON product_dim (product_name,product_model,product_category,product_cost);

CREATE TABLE customer_dim (
  customer_id INTEGER   NOT NULL ,
  customer_address VARCHAR2(500)   NOT NULL ,
  customer_name VARCHAR2(200)   NOT NULL ,
  customer_country VARCHAR2(200)   NOT NULL ,
  customer_phone VARCHAR2(100)   NULL  ,
PRIMARY KEY(customer_id)  );

CREATE UNIQUE INDEX customer_nk ON customer_dim (customer_name,customer_address,customer_country,customer_phone);

CREATE TABLE date_dim (
  date_id INTEGER   NOT NULL ,
  date_val DATE   NOT NULL ,    
  month_num INTEGER   NOT NULL ,
  year_num INTEGER   NOT NULL   ,
PRIMARY KEY(date_id)  );

CREATE UNIQUE INDEX date_nk ON date_dim (date_val);

CREATE TABLE employee_dim (
  employee_id INTEGER   NOT NULL ,
  employee_name VARCHAR2(100)   NOT NULL ,
  employee_number INTEGER   NOT NULL   ,
PRIMARY KEY(employee_id)  );

CREATE UNIQUE INDEX employee_nk ON employee_dim (employee_name,employee_number);


CREATE SEQUENCE promotion_dim_seq;

CREATE SEQUENCE sales_channel_dim_seq;

CREATE SEQUENCE supplier_dim_seq;

CREATE SEQUENCE product_dim_seq;

CREATE SEQUENCE customer_dim_seq;

CREATE SEQUENCE employee_dim_seq;
  
CREATE TABLE  sales_fact (
  employee_id NUMBER   NOT NULL ,
  customer_id NUMBER   NOT NULL ,
  product_id NUMBER   NOT NULL ,
  sales_channel_id NUMBER   NOT NULL ,
  promotion_id NUMBER   NOT NULL ,
  supplier_id NUMBER   NOT NULL ,
  booking_date_id NUMBER   NOT NULL ,
  sales_date_id NUMBER   NOT NULL ,
  delivery_date_id NUMBER   NOT NULL ,
  priceBeforeDiscount FLOAT   NOT NULL ,
  priceAfterDiscount FLOAT   NOT NULL ,
  saleCosts FLOAT   NOT NULL,
  sub    NUMBER   NOT NULL  /* used to acheive parallel loads, maps to streamHorizon context <bulkProcessingThreadID> parameter*/
)
PCTFREE    0
INITRANS   1
MAXTRANS   255
STORAGE    (
            INITIAL          256K
            NEXT             1M
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
           )
NOLOGGING
PARTITION BY LIST (booking_date_id)
SUBPARTITION BY LIST (sub) SUBPARTITION TEMPLATE (
    SUBPARTITION SP_0 VALUES (0)  COMPRESS BASIC,
    SUBPARTITION SP_1 VALUES (1)  COMPRESS BASIC,
    SUBPARTITION SP_2 VALUES (2)  COMPRESS BASIC,
    SUBPARTITION SP_3 VALUES (3)  COMPRESS BASIC,
    SUBPARTITION SP_4 VALUES (4)  COMPRESS BASIC,
    SUBPARTITION SP_5 VALUES (5)  COMPRESS BASIC,
    SUBPARTITION SP_6 VALUES (6)  COMPRESS BASIC,
    SUBPARTITION SP_7 VALUES (7)  COMPRESS BASIC,
    SUBPARTITION SP_8 VALUES (8)  COMPRESS BASIC,
    SUBPARTITION SP_9 VALUES (9)  COMPRESS BASIC,
    SUBPARTITION SP_10 VALUES (10)  COMPRESS BASIC,
    SUBPARTITION SP_11 VALUES (11)  COMPRESS BASIC,
    SUBPARTITION SP_12 VALUES (12)  COMPRESS BASIC,
    SUBPARTITION SP_13 VALUES (13)  COMPRESS BASIC,
    SUBPARTITION SP_14 VALUES (14)  COMPRESS BASIC,
    SUBPARTITION SP_15 VALUES (15)  COMPRESS BASIC,
    SUBPARTITION SP_16 VALUES (16)  COMPRESS BASIC,
    SUBPARTITION SP_17 VALUES (17)  COMPRESS BASIC,
    SUBPARTITION SP_18 VALUES (18)  COMPRESS BASIC,
    SUBPARTITION SP_19 VALUES (19)  COMPRESS BASIC,
    SUBPARTITION SP_20 VALUES (20)  COMPRESS BASIC,
    SUBPARTITION SP_21 VALUES (21)  COMPRESS BASIC,
    SUBPARTITION SP_22 VALUES (22)  COMPRESS BASIC,
    SUBPARTITION SP_23 VALUES (23)  COMPRESS BASIC,
    SUBPARTITION SP_24 VALUES (24)  COMPRESS BASIC,        
    SUBPARTITION SP_25 VALUES (25)  COMPRESS BASIC,
    SUBPARTITION SP_26 VALUES (26)  COMPRESS BASIC,
    SUBPARTITION SP_27 VALUES (27)  COMPRESS BASIC,
    SUBPARTITION SP_28 VALUES (28)  COMPRESS BASIC,
    SUBPARTITION SP_29 VALUES (29)  COMPRESS BASIC,
    SUBPARTITION SP_30 VALUES (30)  COMPRESS BASIC,
    SUBPARTITION SP_31 VALUES (31)  COMPRESS BASIC,
    SUBPARTITION SP_32 VALUES (32)  COMPRESS BASIC,
    SUBPARTITION SP_33 VALUES (33)  COMPRESS BASIC,
    SUBPARTITION SP_34 VALUES (34)  COMPRESS BASIC,
    SUBPARTITION SP_35 VALUES (35)  COMPRESS BASIC,
    SUBPARTITION SP_36 VALUES (36)  COMPRESS BASIC,
    SUBPARTITION SP_37 VALUES (37)  COMPRESS BASIC,
    SUBPARTITION SP_38 VALUES (38)  COMPRESS BASIC,
    SUBPARTITION SP_39 VALUES (39)  COMPRESS BASIC,
    SUBPARTITION SP_40 VALUES (40)  COMPRESS BASIC,
    SUBPARTITION SP_41 VALUES (41)  COMPRESS BASIC,
    SUBPARTITION SP_42 VALUES (42)  COMPRESS BASIC,
    SUBPARTITION SP_43 VALUES (43)  COMPRESS BASIC,
    SUBPARTITION SP_44 VALUES (44)  COMPRESS BASIC,
    SUBPARTITION SP_45 VALUES (45)  COMPRESS BASIC,
    SUBPARTITION SP_46 VALUES (46)  COMPRESS BASIC,
    SUBPARTITION SP_47 VALUES (47)  COMPRESS BASIC,
    SUBPARTITION SP_48 VALUES (48)  COMPRESS BASIC,
    SUBPARTITION SP_49 VALUES (49)  COMPRESS BASIC
)
(  
  PARTITION P_20140107 VALUES (20140107)
    NOLOGGING
    COMPRESS      
    PCTFREE    0
    INITRANS   1
    MAXTRANS   255
    STORAGE    (
                INITIAL          256K
                NEXT             1M
                MINEXTENTS       1
                MAXEXTENTS       UNLIMITED
                PCTINCREASE      0
                BUFFER_POOL      DEFAULT
               )
)
COMPRESS BASIC 
NOCACHE
NOPARALLEL
MONITORING;




insert into date_dim values (20140101,to_date(20140101,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140102,to_date(20140102,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140103,to_date(20140103,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140104,to_date(20140104,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140105,to_date(20140105,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140106,to_date(20140106,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140107,to_date(20140107,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140108,to_date(20140108,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140109,to_date(20140109,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140110,to_date(20140110,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140111,to_date(20140111,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140112,to_date(20140112,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140113,to_date(20140113,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140114,to_date(20140114,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140115,to_date(20140115,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140116,to_date(20140116,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140117,to_date(20140117,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140118,to_date(20140118,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140119,to_date(20140119,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140120,to_date(20140120,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140121,to_date(20140121,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140122,to_date(20140122,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140123,to_date(20140123,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140124,to_date(20140124,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140125,to_date(20140125,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140126,to_date(20140126,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140127,to_date(20140127,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140128,to_date(20140128,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140129,to_date(20140129,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140130,to_date(20140130,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140131,to_date(20140131,'YYYYMMDD'), 1, 2014);
insert into date_dim values (20140201,to_date(20140201,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140202,to_date(20140202,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140203,to_date(20140203,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140204,to_date(20140204,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140205,to_date(20140205,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140206,to_date(20140206,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140207,to_date(20140207,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140208,to_date(20140208,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140209,to_date(20140209,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140210,to_date(20140210,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140211,to_date(20140211,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140212,to_date(20140212,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140213,to_date(20140213,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140214,to_date(20140214,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140215,to_date(20140215,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140216,to_date(20140216,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140217,to_date(20140217,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140218,to_date(20140218,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140219,to_date(20140219,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140220,to_date(20140220,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140221,to_date(20140221,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140222,to_date(20140222,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140223,to_date(20140223,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140224,to_date(20140224,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140225,to_date(20140225,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140226,to_date(20140226,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140227,to_date(20140227,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140228,to_date(20140228,'YYYYMMDD'), 2, 2014);
insert into date_dim values (20140301,to_date(20140301,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140302,to_date(20140302,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140303,to_date(20140303,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140304,to_date(20140304,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140305,to_date(20140305,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140306,to_date(20140306,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140307,to_date(20140307,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140308,to_date(20140308,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140309,to_date(20140309,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140310,to_date(20140310,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140311,to_date(20140311,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140312,to_date(20140312,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140313,to_date(20140313,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140314,to_date(20140314,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140315,to_date(20140315,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140316,to_date(20140316,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140317,to_date(20140317,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140318,to_date(20140318,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140319,to_date(20140319,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140320,to_date(20140320,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140321,to_date(20140321,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140322,to_date(20140322,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140323,to_date(20140323,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140324,to_date(20140324,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140325,to_date(20140325,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140326,to_date(20140326,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140327,to_date(20140327,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140328,to_date(20140328,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140329,to_date(20140329,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140330,to_date(20140330,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140331,to_date(20140331,'YYYYMMDD'), 3, 2014);
insert into date_dim values (20140401,to_date(20140401,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140402,to_date(20140402,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140403,to_date(20140403,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140404,to_date(20140404,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140405,to_date(20140405,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140406,to_date(20140406,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140407,to_date(20140407,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140408,to_date(20140408,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140409,to_date(20140409,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140410,to_date(20140410,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140411,to_date(20140411,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140412,to_date(20140412,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140413,to_date(20140413,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140414,to_date(20140414,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140415,to_date(20140415,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140416,to_date(20140416,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140417,to_date(20140417,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140418,to_date(20140418,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140419,to_date(20140419,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140420,to_date(20140420,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140421,to_date(20140421,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140422,to_date(20140422,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140423,to_date(20140423,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140424,to_date(20140424,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140425,to_date(20140425,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140426,to_date(20140426,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140427,to_date(20140427,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140428,to_date(20140428,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140429,to_date(20140429,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140430,to_date(20140430,'YYYYMMDD'), 4, 2014);
insert into date_dim values (20140501,to_date(20140501,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140502,to_date(20140502,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140503,to_date(20140503,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140504,to_date(20140504,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140505,to_date(20140505,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140506,to_date(20140506,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140507,to_date(20140507,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140508,to_date(20140508,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140509,to_date(20140509,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140510,to_date(20140510,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140511,to_date(20140511,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140512,to_date(20140512,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140513,to_date(20140513,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140514,to_date(20140514,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140515,to_date(20140515,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140516,to_date(20140516,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140517,to_date(20140517,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140518,to_date(20140518,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140519,to_date(20140519,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140520,to_date(20140520,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140521,to_date(20140521,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140522,to_date(20140522,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140523,to_date(20140523,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140524,to_date(20140524,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140525,to_date(20140525,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140526,to_date(20140526,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140527,to_date(20140527,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140528,to_date(20140528,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140529,to_date(20140529,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140530,to_date(20140530,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140531,to_date(20140531,'YYYYMMDD'), 5, 2014);
insert into date_dim values (20140601,to_date(20140601,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140602,to_date(20140602,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140603,to_date(20140603,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140604,to_date(20140604,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140605,to_date(20140605,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140606,to_date(20140606,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140607,to_date(20140607,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140608,to_date(20140608,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140609,to_date(20140609,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140610,to_date(20140610,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140611,to_date(20140611,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140612,to_date(20140612,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140613,to_date(20140613,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140614,to_date(20140614,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140615,to_date(20140615,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140616,to_date(20140616,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140617,to_date(20140617,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140618,to_date(20140618,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140619,to_date(20140619,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140620,to_date(20140620,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140621,to_date(20140621,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140622,to_date(20140622,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140623,to_date(20140623,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140624,to_date(20140624,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140625,to_date(20140625,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140626,to_date(20140626,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140627,to_date(20140627,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140628,to_date(20140628,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140629,to_date(20140629,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140630,to_date(20140630,'YYYYMMDD'), 6, 2014);
insert into date_dim values (20140701,to_date(20140701,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140702,to_date(20140702,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140703,to_date(20140703,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140704,to_date(20140704,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140705,to_date(20140705,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140706,to_date(20140706,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140707,to_date(20140707,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140708,to_date(20140708,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140709,to_date(20140709,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140710,to_date(20140710,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140711,to_date(20140711,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140712,to_date(20140712,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140713,to_date(20140713,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140714,to_date(20140714,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140715,to_date(20140715,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140716,to_date(20140716,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140717,to_date(20140717,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140718,to_date(20140718,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140719,to_date(20140719,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140720,to_date(20140720,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140721,to_date(20140721,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140722,to_date(20140722,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140723,to_date(20140723,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140724,to_date(20140724,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140725,to_date(20140725,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140726,to_date(20140726,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140727,to_date(20140727,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140728,to_date(20140728,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140729,to_date(20140729,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140730,to_date(20140730,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140731,to_date(20140731,'YYYYMMDD'), 7, 2014);
insert into date_dim values (20140801,to_date(20140801,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140802,to_date(20140802,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140803,to_date(20140803,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140804,to_date(20140804,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140805,to_date(20140805,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140806,to_date(20140806,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140807,to_date(20140807,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140808,to_date(20140808,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140809,to_date(20140809,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140810,to_date(20140810,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140811,to_date(20140811,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140812,to_date(20140812,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140813,to_date(20140813,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140814,to_date(20140814,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140815,to_date(20140815,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140816,to_date(20140816,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140817,to_date(20140817,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140818,to_date(20140818,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140819,to_date(20140819,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140820,to_date(20140820,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140821,to_date(20140821,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140822,to_date(20140822,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140823,to_date(20140823,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140824,to_date(20140824,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140825,to_date(20140825,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140826,to_date(20140826,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140827,to_date(20140827,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140828,to_date(20140828,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140829,to_date(20140829,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140830,to_date(20140830,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140831,to_date(20140831,'YYYYMMDD'), 8, 2014);
insert into date_dim values (20140901,to_date(20140901,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140902,to_date(20140902,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140903,to_date(20140903,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140904,to_date(20140904,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140905,to_date(20140905,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140906,to_date(20140906,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140907,to_date(20140907,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140908,to_date(20140908,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140909,to_date(20140909,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140910,to_date(20140910,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140911,to_date(20140911,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140912,to_date(20140912,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140913,to_date(20140913,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140914,to_date(20140914,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140915,to_date(20140915,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140916,to_date(20140916,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140917,to_date(20140917,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140918,to_date(20140918,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140919,to_date(20140919,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140920,to_date(20140920,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140921,to_date(20140921,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140922,to_date(20140922,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140923,to_date(20140923,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140924,to_date(20140924,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140925,to_date(20140925,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140926,to_date(20140926,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140927,to_date(20140927,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140928,to_date(20140928,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140929,to_date(20140929,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20140930,to_date(20140930,'YYYYMMDD'), 9, 2014);
insert into date_dim values (20141001,to_date(20141001,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141002,to_date(20141002,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141003,to_date(20141003,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141004,to_date(20141004,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141005,to_date(20141005,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141006,to_date(20141006,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141007,to_date(20141007,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141008,to_date(20141008,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141009,to_date(20141009,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141010,to_date(20141010,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141011,to_date(20141011,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141012,to_date(20141012,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141013,to_date(20141013,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141014,to_date(20141014,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141015,to_date(20141015,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141016,to_date(20141016,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141017,to_date(20141017,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141018,to_date(20141018,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141019,to_date(20141019,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141020,to_date(20141020,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141021,to_date(20141021,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141022,to_date(20141022,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141023,to_date(20141023,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141024,to_date(20141024,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141025,to_date(20141025,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141026,to_date(20141026,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141027,to_date(20141027,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141028,to_date(20141028,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141029,to_date(20141029,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141030,to_date(20141030,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141031,to_date(20141031,'YYYYMMDD'), 10, 2014);
insert into date_dim values (20141101,to_date(20141101,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141102,to_date(20141102,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141103,to_date(20141103,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141104,to_date(20141104,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141105,to_date(20141105,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141106,to_date(20141106,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141107,to_date(20141107,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141108,to_date(20141108,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141109,to_date(20141109,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141110,to_date(20141110,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141111,to_date(20141111,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141112,to_date(20141112,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141113,to_date(20141113,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141114,to_date(20141114,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141115,to_date(20141115,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141116,to_date(20141116,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141117,to_date(20141117,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141118,to_date(20141118,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141119,to_date(20141119,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141120,to_date(20141120,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141121,to_date(20141121,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141122,to_date(20141122,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141123,to_date(20141123,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141124,to_date(20141124,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141125,to_date(20141125,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141126,to_date(20141126,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141127,to_date(20141127,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141128,to_date(20141128,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141129,to_date(20141129,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141130,to_date(20141130,'YYYYMMDD'), 11, 2014);
insert into date_dim values (20141201,to_date(20141201,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141202,to_date(20141202,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141203,to_date(20141203,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141204,to_date(20141204,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141205,to_date(20141205,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141206,to_date(20141206,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141207,to_date(20141207,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141208,to_date(20141208,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141209,to_date(20141209,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141210,to_date(20141210,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141211,to_date(20141211,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141212,to_date(20141212,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141213,to_date(20141213,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141214,to_date(20141214,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141215,to_date(20141215,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141216,to_date(20141216,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141217,to_date(20141217,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141218,to_date(20141218,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141219,to_date(20141219,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141220,to_date(20141220,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141221,to_date(20141221,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141222,to_date(20141222,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141223,to_date(20141223,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141224,to_date(20141224,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141225,to_date(20141225,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141226,to_date(20141226,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141227,to_date(20141227,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141228,to_date(20141228,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141229,to_date(20141229,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141230,to_date(20141230,'YYYYMMDD'), 12, 2014);
insert into date_dim values (20141231,to_date(20141231,'YYYYMMDD'), 12, 2014);
commit;

create table sh_metrics (
servername varchar2(100) null,
instancenumber number null,
instancestarted varchar2(100) null,
eventName varchar2(100) null,
fileReceived timestamp null,
etlThreadID number null,
fileName varchar2(100) null,
fileProcessingStart timestamp null,
fileProcessingFinish timestamp null,
fileProcessingMillis number null,    
fileJdbcInsertStart timestamp null,
fileJdbcInsertFinish timestamp null,    
jdbcProcessingMillis number null,
bulkFileSubmitted varchar2(100) null,
dbThreadID number null,
bulkFilePath varchar2(200) null,
bulkFileName varchar2(100) null,
fileRecordCount number null,
bulkFileReceived timestamp null,
bulkFileProcessingStart timestamp null,
bulkFileProcessingFinish timestamp null,
bulkProcessingMillis number null,
etlCompletionFlag varchar2(100) null,
bulkCompletionFlag varchar2(100) null,
etlErrorDescription varchar2(1000) null,
bulkErrorDescription varchar2(1000) null,
recordInserted timestamp null
); 

create or replace view sh_dashboard as
select 
servername,instancestarted,
round(
            "total file records processed"/
            (
                to_number(extract( second from "processing window" ))  + 
                to_number(extract( minute from "processing window" )*60) + 
                to_number(extract( hour from "processing window" )*3600)
            )
) as "througput records/second",
 "processing window",
 "total file records processed"
from
(
select etl.servername,etl.instancestarted,sum(filerecordcount) as "total file records processed",nvl(max(bulkfileprocessingfinish),max(filejdbcinsertfinish)) - min(fileprocessingstart)  as "processing window"
from 
(
select 
servername,instancestarted,filerecordcount,filejdbcinsertfinish,fileprocessingstart,bulkFileName
from sh_metrics
where  instancestarted = (select max(instancestarted) from sh_metrics) and filename is not null and eventname='<afterFeedProcessingCompletion>'
)etl,
(
select 
servername,instancestarted,bulkfileprocessingfinish,bulkFileName
from sh_metrics
where  instancestarted = (select max(instancestarted) from sh_metrics) and filename is null and eventname='<onBulkLoadCompletion>'
)db
where etl.servername=db.servername and etl.instancestarted = db.instancestarted and etl.bulkFileName = db.bulkFileName
group by  etl.servername,etl.instancestarted
);


CREATE OR REPLACE procedure log_sh_metrics
(
servername varchar2,instancenumber number,instancestarted number ,eventName varchar2,fileReceived number ,etlThreadID number ,
fileName varchar2,fileProcessingStart number ,fileProcessingFinish number ,fileJdbcInsertStart number ,fileJdbcInsertFinish number ,bulkFileSubmitted varchar2,dbThreadID number,bulkFilePath varchar2,
bulkFileName varchar2,fileRecordCount number ,bulkFileReceived number,bulkFileProcessingStart number,bulkFileProcessingFinish number, etlCompletionFlag varchar2, etlErrorDescription varchar2
)
is
    instancestarted_ts  timestamp;
    fileReceived_ts timestamp;
    fileProcessingStart_ts timestamp;
    fileProcessingFinish_ts timestamp;
    fileJdbcInsertStart_ts timestamp;
    fileJdbcInsertFinish_ts timestamp;
    bulkFileReceived_ts timestamp;
    fileProcessingMillis number;
    jdbcProcessingMillis number;
    bulkProcessingMillis number;    
    function milliseconddiff (p_d1 in timestamp,   p_d2 in timestamp)
       return number as
       l_result   number;
    begin
       l_result   :=
             (extract(day    from p_d2 - p_d1)) * 86400000
           + (extract(hour   from p_d2 - p_d1)) * 3600000
           + (extract(minute from p_d2 - p_d1)) * 60000
           + (extract(second from p_d2 - p_d1)) * 1000;
       return l_result;
    end;    
begin

instancestarted_ts := timestamp '1970-01-01 00:00:00' + numtodsinterval((instancestarted)/1000/60, 'MINUTE');
fileReceived_ts := timestamp '1970-01-01 00:00:00' + numtodsinterval((fileReceived)/1000/60, 'MINUTE');
fileProcessingStart_ts := timestamp '1970-01-01 00:00:00' + numtodsinterval((fileProcessingStart)/1000/60, 'MINUTE');
fileProcessingFinish_ts := timestamp '1970-01-01 00:00:00' + numtodsinterval((fileProcessingFinish)/1000/60, 'MINUTE');
fileJdbcInsertStart_ts := timestamp '1970-01-01 00:00:00' + numtodsinterval((fileJdbcInsertStart)/1000/60, 'MINUTE');
fileJdbcInsertFinish_ts := timestamp '1970-01-01 00:00:00' + numtodsinterval((fileJdbcInsertFinish)/1000/60, 'MINUTE');
bulkFileReceived_ts := timestamp '1970-01-01 00:00:00' + numtodsinterval((bulkFileReceived)/1000/60, 'MINUTE');

fileProcessingMillis := milliseconddiff(fileProcessingStart_ts,fileProcessingFinish_ts);
jdbcProcessingMillis := milliseconddiff(fileJdbcInsertStart_ts,fileJdbcInsertFinish_ts);

insert into sh_metrics 
(servername,instancenumber,instancestarted,eventName,fileReceived,etlThreadID,fileName,fileProcessingStart,fileProcessingFinish,fileProcessingMillis,fileJdbcInsertStart,
fileJdbcInsertFinish,jdbcProcessingMillis,bulkFileSubmitted,dbThreadID,bulkFilePath,bulkFileName,fileRecordCount,bulkFileReceived,etlCompletionFlag,etlErrorDescription,recordInserted)
values(servername,instancenumber,instancestarted_ts,'<'||eventName||'>',fileReceived_ts,etlThreadID,
fileName,fileProcessingStart_ts,fileProcessingFinish_ts,fileProcessingMillis,fileJdbcInsertStart_ts,fileJdbcInsertFinish_ts,jdbcProcessingMillis,bulkFileSubmitted,dbThreadID,bulkFilePath,
bulkFileName,fileRecordCount,bulkFileReceived_ts,etlCompletionFlag,etlErrorDescription,systimestamp);
commit;       
end log_sh_metrics;
/




CREATE OR REPLACE procedure log_sh_metrics_bulk
(
servername varchar2,instancenumber number,instancestarted number ,eventName varchar2,bulkFile varchar2,bulkFileProcessingStart number,bulkFileProcessingFinish number, bulkCompletionFlag varchar2, bulkErrorDesc varchar2
)
is
    instancestarted_ts  timestamp;
    bulkFileReceived_ts timestamp;
    bulkFileProcessingStart_ts timestamp;
    bulkFileProcessingFinish_ts timestamp;
    bulkProcessingMillis number;    
    function milliseconddiff (p_d1 in timestamp,   p_d2 in timestamp)
       return number as
       l_result   number;
    begin
       l_result   :=
             (extract(day    from p_d2 - p_d1)) * 86400000
           + (extract(hour   from p_d2 - p_d1)) * 3600000
           + (extract(minute from p_d2 - p_d1)) * 60000
           + (extract(second from p_d2 - p_d1)) * 1000;
       return l_result;
    end;    
begin

bulkFileProcessingStart_ts:= timestamp '1970-01-01 00:00:00' + numtodsinterval((bulkFileProcessingStart)/1000/60, 'MINUTE');
bulkFileProcessingFinish_ts:= timestamp '1970-01-01 00:00:00' + numtodsinterval((bulkFileProcessingFinish)/1000/60, 'MINUTE');

bulkProcessingMillis := milliseconddiff(bulkFileProcessingStart_ts,bulkFileProcessingFinish_ts);
instancestarted_ts := timestamp '1970-01-01 00:00:00' + numtodsinterval((instancestarted)/1000/60, 'MINUTE');

insert into sh_metrics 
(servername,instancenumber,instancestarted,eventName,bulkErrorDescription,bulkCompletionFlag,bulkFileProcessingStart,bulkFileProcessingFinish,bulkProcessingMillis,bulkFileName,recordInserted) 
values(log_sh_metrics_bulk.servername,log_sh_metrics_bulk.instancenumber,instancestarted_ts,'<'||log_sh_metrics_bulk.eventName||'>',log_sh_metrics_bulk.bulkErrorDesc,log_sh_metrics_bulk.bulkCompletionFlag,bulkFileProcessingStart_ts,bulkFileProcessingFinish_ts,bulkProcessingMillis,log_sh_metrics_bulk.bulkFile,systimestamp);
commit;       
end log_sh_metrics_bulk;
/

create or replace view sh_etl as
select etl.servername,etl.instancestarted, fileProcessingStart,fileProcessingFinish,"etl recordInserted",bulkFileProcessingStart,bulkFileProcessingFinish,"bulk recordInserted"
from 
(
select 
servername,instancestarted,filerecordcount,filejdbcinsertfinish,fileprocessingstart,bulkFileName,fileProcessingFinish,recordInserted as "etl recordInserted"
from sh_metrics
where  instancestarted = (select max(instancestarted) from sh_metrics) and filename is not null and eventname='<afterFeedProcessingCompletion>'
)etl,
(
select 
servername,instancestarted,bulkfileprocessingfinish,bulkFileName,bulkFileProcessingStart,recordInserted as "bulk recordInserted"
from sh_metrics
where  instancestarted = (select max(instancestarted) from sh_metrics) and filename is null and eventname='<onBulkLoadCompletion>'
)db
where etl.servername=db.servername and etl.instancestarted = db.instancestarted and etl.bulkFileName = db.bulkFileName;

