CREATE TABLE promotion_dim (
  promotion_sk INTEGER   NOT NULL ,
  promotion_name VARCHAR(200)   NOT NULL ,
  discount_pct FLOAT   NOT NULL   ,
PRIMARY KEY(promotion_sk));




CREATE TABLE sales_channel_dim (
  sales_channel_sk INTEGER   NOT NULL ,
  sales_channel_name VARCHAR(100)   NOT NULL   ,
PRIMARY KEY(sales_channel_sk)  );


CREATE UNIQUE INDEX sales_channel_dim_index1048 ON sales_channel_dim (sales_channel_name);




CREATE TABLE supplier_dim (
  supplier_sk INTEGER   NOT NULL ,
  supplier_name VARCHAR(200)   NOT NULL ,
  supplier_address VARCHAR(500)    ,
  supplier_phone VARCHAR(100)      ,
PRIMARY KEY(supplier_sk)  );


CREATE UNIQUE INDEX supplier_dim_index1043 ON supplier_dim (supplier_name);




CREATE TABLE product_dim (
  product_sk INTEGER   NOT NULL ,
  product_name VARCHAR(200)   NOT NULL ,
  product_model VARCHAR(100)   NOT NULL ,
  product_category VARCHAR(100)   NOT NULL ,
  product_cost VARCHAR(10)   NOT NULL   ,
PRIMARY KEY(product_sk)  );


CREATE UNIQUE INDEX product_dim_index1010 ON product_dim (product_name, product_category, product_model, product_cost);




CREATE TABLE customer_dim (
  customer_sk INTEGER   NOT NULL ,
  customer_address VARCHAR(500)   NOT NULL ,
  customer_name VARCHAR(200)   NOT NULL ,
  customer_country VARCHAR(200)   NOT NULL ,
  customer_phone VARCHAR(100)      ,
PRIMARY KEY(customer_sk)  );


CREATE UNIQUE INDEX customer_dim_index1018 ON customer_dim (customer_name, customer_address, customer_country, customer_phone);




CREATE TABLE date_dim (
  date_sk INTEGER   NOT NULL ,
  date_val DATE   NOT NULL ,
  dateMMDDYYYY VARCHAR(12)   NOT NULL ,
  day_of_week INTEGER   NOT NULL ,
  month_num INTEGER   NOT NULL ,
  year_num INTEGER   NOT NULL   ,
PRIMARY KEY(date_sk)  );


CREATE UNIQUE INDEX date_dim_index1027 ON date_dim (dateMMDDYYYY);




CREATE TABLE employee_dim (
  employee_sk INTEGER   NOT NULL ,
  employee_name VARCHAR(100)   NOT NULL ,
  employee_id INTEGER   NOT NULL   ,
PRIMARY KEY(employee_sk)  );


CREATE INDEX employee_dim_index1036 ON employee_dim (employee_name);




CREATE TABLE sales_fact (
  sales_channel_sk INTEGER   NOT NULL ,
  promotion_sk INTEGER   NOT NULL ,
  supplier_sk INTEGER   NOT NULL ,
  employee_sk INTEGER   NOT NULL ,
  customer_sk INTEGER   NOT NULL ,
  product_sk INTEGER   NOT NULL ,
  booking_date INTEGER   NOT NULL ,
  sales_date INTEGER   NOT NULL ,
  delivery_date INTEGER   NOT NULL ,
  priceBeforeDiscount FLOAT   NOT NULL ,
  priceAfterDiscount FLOAT   NOT NULL ,
  saleCosts FLOAT   NOT NULL             ,
  FOREIGN KEY(product_sk)
    REFERENCES product_dim(product_sk),
  FOREIGN KEY(customer_sk)
    REFERENCES customer_dim(customer_sk),
  FOREIGN KEY(employee_sk)
    REFERENCES employee_dim(employee_sk),
  FOREIGN KEY(supplier_sk)
    REFERENCES supplier_dim(supplier_sk),
  FOREIGN KEY(promotion_sk)
    REFERENCES promotion_dim(promotion_sk),
  FOREIGN KEY(sales_channel_sk)
    REFERENCES sales_channel_dim(sales_channel_sk));


CREATE INDEX Table_08_FKIndex1 ON sales_fact (product_sk);
CREATE INDEX sales_fact_FKIndex2 ON sales_fact (customer_sk);
CREATE INDEX sales_fact_FKIndex3 ON sales_fact (employee_sk);
CREATE INDEX sales_fact_FKIndex4 ON sales_fact (supplier_sk);
CREATE INDEX sales_fact_FKIndex5 ON sales_fact (promotion_sk);
CREATE INDEX sales_fact_FKIndex7 ON sales_fact (sales_channel_sk);


CREATE INDEX IFK_Rel_02 ON sales_fact (product_sk);
CREATE INDEX IFK_Rel_03 ON sales_fact (customer_sk);
CREATE INDEX IFK_Rel_04 ON sales_fact (employee_sk);
CREATE INDEX IFK_Rel_05 ON sales_fact (supplier_sk);
CREATE INDEX IFK_Rel_06 ON sales_fact (promotion_sk);
CREATE INDEX IFK_Rel_07 ON sales_fact (sales_channel_sk);



