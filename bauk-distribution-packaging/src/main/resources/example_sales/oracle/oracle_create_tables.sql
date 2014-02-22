CREATE TABLE promotion_dim (
  promotion_id INTEGER   NOT NULL ,
  promotion_name VARCHAR(200)   NOT NULL ,
  discount_pct FLOAT   NOT NULL   ,
PRIMARY KEY(promotion_id));




CREATE TABLE sales_channel_dim (
  sales_channel_id INTEGER   NOT NULL ,
  sales_channel_name VARCHAR(100)   NOT NULL   ,
PRIMARY KEY(sales_channel_id)  );


CREATE UNIQUE INDEX sales_channel_dim_index1048 ON sales_channel_dim (sales_channel_name);




CREATE TABLE supplier_dim (
  supplier_id INTEGER   NOT NULL ,
  supplier_name VARCHAR(200)   NOT NULL ,
  supplier_address VARCHAR(500)    ,
  supplier_phone VARCHAR(100)      ,
PRIMARY KEY(supplier_id)  );


CREATE UNIQUE INDEX supplier_dim_index1043 ON supplier_dim (supplier_name);




CREATE TABLE product_dim (
  product_id INTEGER   NOT NULL ,
  product_name VARCHAR(200)   NOT NULL ,
  product_model VARCHAR(100)   NOT NULL ,
  product_category VARCHAR(100)   NOT NULL ,
  product_cost VARCHAR(10)   NOT NULL   ,
PRIMARY KEY(product_id)  );


CREATE UNIQUE INDEX product_dim_index1010 ON product_dim (product_name, product_category, product_model, product_cost);




CREATE TABLE customer_dim (
  customer_id INTEGER   NOT NULL ,
  customer_address VARCHAR(500)   NOT NULL ,
  customer_name VARCHAR(200)   NOT NULL ,
  customer_country VARCHAR(200)   NOT NULL ,
  customer_phone VARCHAR(100)      ,
PRIMARY KEY(customer_id)  );


CREATE UNIQUE INDEX customer_dim_index1018 ON customer_dim (customer_name, customer_address, customer_country, customer_phone);




CREATE TABLE date_dim (
  date_id INTEGER   NOT NULL ,
  date_val DATE   NOT NULL ,
  dateMMDDYYYY VARCHAR(12)   NOT NULL ,
  day_of_week INTEGER   NOT NULL ,
  month_num INTEGER   NOT NULL ,
  year_num INTEGER   NOT NULL   ,
PRIMARY KEY(date_id)  );


CREATE UNIQUE INDEX date_dim_index1027 ON date_dim (dateMMDDYYYY);




CREATE TABLE employee_dim (
  employee_id INTEGER   NOT NULL ,
  employee_name VARCHAR(100)   NOT NULL ,
  employee_number INTEGER   NOT NULL   ,
PRIMARY KEY(employee_id)  );


CREATE INDEX employee_dim_index1036 ON employee_dim (employee_name);




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
  saleCosts FLOAT   NOT NULL ,
  FOREIGN KEY(product_id)
    REFERENCES product_dim(product_id),
  FOREIGN KEY(customer_id)
    REFERENCES customer_dim(customer_id),
  FOREIGN KEY(employee_id)
    REFERENCES employee_dim(employee_id),
  FOREIGN KEY(supplier_id)
    REFERENCES supplier_dim(supplier_id),
  FOREIGN KEY(promotion_id)
    REFERENCES promotion_dim(promotion_id),
  FOREIGN KEY(sales_channel_id)
    REFERENCES sales_channel_dim(sales_channel_id));


CREATE INDEX IFK_Rel_02 ON sales_fact (product_id);
CREATE INDEX IFK_Rel_03 ON sales_fact (customer_id);
CREATE INDEX IFK_Rel_04 ON sales_fact (employee_id);
CREATE INDEX IFK_Rel_05 ON sales_fact (supplier_id);
CREATE INDEX IFK_Rel_06 ON sales_fact (promotion_id);
CREATE INDEX IFK_Rel_07 ON sales_fact (sales_channel_id);



