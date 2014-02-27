CREATE TABLE promotion_dim (
  promotion_id INTEGER   NOT NULL ,
  promotion_name VARCHAR(200)   NOT NULL ,
  discount_pct FLOAT   NOT NULL   ,
PRIMARY KEY(promotion_id));




CREATE TABLE sales_channel_dim (
  sales_channel_id INTEGER   NOT NULL ,
  sales_channel_name VARCHAR(100)   NOT NULL   ,
PRIMARY KEY(sales_channel_id)  );




CREATE TABLE supplier_dim (
  supplier_id INTEGER   NOT NULL ,
  supplier_name VARCHAR(200)   NOT NULL ,
  supplier_address VARCHAR(500)    ,
  supplier_phone VARCHAR(100)      ,
PRIMARY KEY(supplier_id)  );




CREATE TABLE product_dim (
  product_id INTEGER   NOT NULL ,
  product_name VARCHAR(200)   NOT NULL ,
  product_model VARCHAR(100)   NOT NULL ,
  product_category VARCHAR(100)   NOT NULL ,
  product_cost VARCHAR(10)   NOT NULL   ,
PRIMARY KEY(product_id)  );




CREATE TABLE customer_dim (
  customer_id INTEGER   NOT NULL ,
  customer_address VARCHAR(500)   NOT NULL ,
  customer_name VARCHAR(200)   NOT NULL ,
  customer_country VARCHAR(200)   NOT NULL ,
  customer_phone VARCHAR(100)      ,
PRIMARY KEY(customer_id)  );




CREATE TABLE date_dim (
  date_id INTEGER   NOT NULL ,
  date_val DATE   NOT NULL ,
  dateMMDDYYYY VARCHAR(12)   NOT NULL ,
  day_of_week INTEGER   NOT NULL ,
  month_num INTEGER   NOT NULL ,
  year_num INTEGER   NOT NULL   ,
PRIMARY KEY(date_id)  );




CREATE TABLE employee_dim (
  employee_id INTEGER   NOT NULL ,
  employee_name VARCHAR(100)   NOT NULL ,
  employee_number INTEGER   NOT NULL   ,
PRIMARY KEY(employee_id)  );

CREATE SEQUENCE promotion_dim_seq;
CREATE SEQUENCE sales_channel_dim_seq;
CREATE SEQUENCE supplier_dim_seq;
CREATE SEQUENCE product_dim_seq;
CREATE SEQUENCE customer_dim_seq;
CREATE SEQUENCE employee_dim_seq;
CREATE SEQUENCE date_dim_seq;




CREATE TABLE sales_fact (
  employee_id INTEGER   NOT NULL ,
  customer_id INTEGER   NOT NULL ,
  product_id INTEGER   NOT NULL ,
  sales_channel_id INTEGER   NOT NULL ,
  promotion_id INTEGER   NOT NULL ,
  supplier_id INTEGER   NOT NULL ,
  booking_date_id INTEGER   NOT NULL ,
  sales_date_id INTEGER   NOT NULL ,
  delivery_date_id INTEGER   NOT NULL ,
  priceBeforeDiscount FLOAT   NOT NULL ,
  priceAfterDiscount FLOAT   NOT NULL ,
  saleCosts FLOAT   NOT NULL);


create table streamhorizon_metrics (
servername varchar2(100) null,
instancenumber NUMBER null,
instancestarted varchar2(100) null,
eventName varchar2(100) null,
    fileReceived NUMBER null,
etlThreadID NUMBER null,
fileName varchar2(100) null,
    fileProcessingStart NUMBER null,
    fileProcessingFinish NUMBER null,
    fileJdbcInsertStart NUMBER null,
    fileJdbcInsertFinish NUMBER null,
bulkFileSubmitted varchar2(100) null,
dbThreadID NUMBER null,
bulkFilePath varchar2(200) null,
bulkFileName varchar2(100) null,
fileRecordCount NUMBER null,
    bulkFileReceived NUMBER null,
    bulkFileProcessingStart NUMBER null,
    bulkFileProcessingFinish NUMBER null,
completionFlag varchar2(100) null,
errorDescription varchar2(1000) null
);
