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
  dateMMDDYYYY VARCHAR(12)  NOT NULL  ,
  day_of_week INTEGER UNSIGNED  NOT NULL  ,
  month_num INTEGER UNSIGNED  NOT NULL  ,
  year_num INTEGER UNSIGNED  NOT NULL    ,
PRIMARY KEY(date_id)  ,
UNIQUE INDEX date_dim_index1027(dateMMDDYYYY));



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
  saleCosts FLOAT  NOT NULL  ,
  FOREIGN KEY(product_id)
    REFERENCES product_dim(product_id)
      ON DELETE NO ACTION
      ON UPDATE NO ACTION,
  FOREIGN KEY(customer_id)
    REFERENCES customer_dim(customer_id)
      ON DELETE NO ACTION
      ON UPDATE NO ACTION,
  FOREIGN KEY(employee_id)
    REFERENCES employee_dim(employee_id)
      ON DELETE NO ACTION
      ON UPDATE NO ACTION,
  FOREIGN KEY(supplier_id)
    REFERENCES supplier_dim(supplier_id)
      ON DELETE NO ACTION
      ON UPDATE NO ACTION,
  FOREIGN KEY(promotion_id)
    REFERENCES promotion_dim(promotion_id)
      ON DELETE NO ACTION
      ON UPDATE NO ACTION,
  FOREIGN KEY(sales_channel_id)
    REFERENCES sales_channel_dim(sales_channel_id)
      ON DELETE NO ACTION
      ON UPDATE NO ACTION);

create table streamhorizon_metrics (
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


