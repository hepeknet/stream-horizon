CREATE TABLE promotion_dim (
  promotion_sk INTEGER UNSIGNED  NOT NULL   AUTO_INCREMENT,
  promotion_name VARCHAR(200)  NOT NULL  ,
  discount_pct FLOAT  NOT NULL    ,
PRIMARY KEY(promotion_sk));



CREATE TABLE sales_channel_dim (
  sales_channel_sk INTEGER UNSIGNED  NOT NULL   AUTO_INCREMENT,
  sales_channel_name VARCHAR(100)  NOT NULL    ,
PRIMARY KEY(sales_channel_sk)  ,
UNIQUE INDEX sales_channel_dim_index1048(sales_channel_name));



CREATE TABLE supplier_dim (
  supplier_sk INTEGER UNSIGNED  NOT NULL   AUTO_INCREMENT,
  supplier_name VARCHAR(200)  NOT NULL  ,
  supplier_address VARCHAR(500)  NULL  ,
  supplier_phone VARCHAR(100)  NULL    ,
PRIMARY KEY(supplier_sk)  ,
UNIQUE INDEX supplier_dim_index1043(supplier_name));



CREATE TABLE product_dim (
  product_sk INTEGER UNSIGNED  NOT NULL   AUTO_INCREMENT,
  product_name VARCHAR(200)  NOT NULL  ,
  product_model VARCHAR(100)  NOT NULL  ,
  product_category VARCHAR(100)  NOT NULL  ,
  product_cost VARCHAR(10)  NOT NULL    ,
PRIMARY KEY(product_sk)  ,
UNIQUE INDEX product_dim_index1010(product_name, product_category, product_model, product_cost));



CREATE TABLE customer_dim (
  customer_sk INTEGER UNSIGNED  NOT NULL   AUTO_INCREMENT,
  customer_address VARCHAR(500)  NOT NULL  ,
  customer_name VARCHAR(200)  NOT NULL  ,
  customer_country VARCHAR(200)  NOT NULL  ,
  customer_phone VARCHAR(100)  NULL    ,
PRIMARY KEY(customer_sk)  ,
UNIQUE INDEX customer_dim_index1018(customer_name, customer_address, customer_country, customer_phone));



CREATE TABLE date_dim (
  date_sk INTEGER UNSIGNED  NOT NULL   AUTO_INCREMENT,
  date_val DATE  NOT NULL  ,
  dateMMDDYYYY VARCHAR(12)  NOT NULL  ,
  day_of_week INTEGER UNSIGNED  NOT NULL  ,
  month_num INTEGER UNSIGNED  NOT NULL  ,
  year_num INTEGER UNSIGNED  NOT NULL    ,
PRIMARY KEY(date_sk)  ,
UNIQUE INDEX date_dim_index1027(dateMMDDYYYY));



CREATE TABLE employee_dim (
  employee_sk INTEGER UNSIGNED  NOT NULL   AUTO_INCREMENT,
  employee_name VARCHAR(100)  NOT NULL  ,
  employee_id INTEGER UNSIGNED  NOT NULL    ,
PRIMARY KEY(employee_sk)  ,
INDEX employee_dim_index1036(employee_name));



CREATE TABLE sales_fact (
  sales_channel_sk INTEGER UNSIGNED  NOT NULL  ,
  promotion_sk INTEGER UNSIGNED  NOT NULL  ,
  supplier_sk INTEGER UNSIGNED  NOT NULL  ,
  employee_sk INTEGER UNSIGNED  NOT NULL  ,
  customer_sk INTEGER UNSIGNED  NOT NULL  ,
  product_sk INTEGER UNSIGNED  NOT NULL  ,
  booking_date INTEGER UNSIGNED  NOT NULL  ,
  sales_date INTEGER UNSIGNED  NOT NULL  ,
  delivery_date INTEGER UNSIGNED  NOT NULL  ,
  priceBeforeDiscount FLOAT  NOT NULL  ,
  priceAfterDiscount FLOAT  NOT NULL  ,
  saleCosts FLOAT  NOT NULL    ,
INDEX Table_08_FKIndex1(product_sk)  ,
INDEX sales_fact_FKIndex2(customer_sk)  ,
INDEX sales_fact_FKIndex3(employee_sk)  ,
INDEX sales_fact_FKIndex4(supplier_sk)  ,
INDEX sales_fact_FKIndex5(promotion_sk)  ,
INDEX sales_fact_FKIndex7(sales_channel_sk),
  FOREIGN KEY(product_sk)
    REFERENCES product_dim(product_sk)
      ON DELETE NO ACTION
      ON UPDATE NO ACTION,
  FOREIGN KEY(customer_sk)
    REFERENCES customer_dim(customer_sk)
      ON DELETE NO ACTION
      ON UPDATE NO ACTION,
  FOREIGN KEY(employee_sk)
    REFERENCES employee_dim(employee_sk)
      ON DELETE NO ACTION
      ON UPDATE NO ACTION,
  FOREIGN KEY(supplier_sk)
    REFERENCES supplier_dim(supplier_sk)
      ON DELETE NO ACTION
      ON UPDATE NO ACTION,
  FOREIGN KEY(promotion_sk)
    REFERENCES promotion_dim(promotion_sk)
      ON DELETE NO ACTION
      ON UPDATE NO ACTION,
  FOREIGN KEY(sales_channel_sk)
    REFERENCES sales_channel_dim(sales_channel_sk)
      ON DELETE NO ACTION
      ON UPDATE NO ACTION);




