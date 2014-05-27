alter session set current_schema = sh;

drop index fact_customer;
drop index  fact_file_id;

alter table sales_fact drop constraint fk_customer;

alter table sales_fact
 add constraint fk_customer
 foreign key (customer_id) references customer_dim
 rely novalidate;
 
alter table sales_fact disable constraint fk_customer;

create bitmap index fact_customer
on sales_fact(customer_dim.customer_name)
from sales_fact, customer_dim
where sales_fact.customer_id = customer_dim.customer_id
local logging
;

create bitmap index fact_file_id 
on sales_fact (file_id)
local logging
;

drop index fact_agg_product;
drop index fact_agg_sales_channel;

alter table sales_fact_agg drop constraint fk_agg_product;
alter table sales_fact_agg drop constraint fk_agg_sales_channel;

alter table sales_fact_agg
 add constraint fk_agg_product
 foreign key (product_id) references product_dim
 rely novalidate;

alter table sales_fact_agg disable constraint fk_agg_product;

alter table sales_fact_agg
 add constraint fk_agg_sales_channel
 foreign key (sales_channel_id) references sales_channel_dim
 rely novalidate;
 
alter table sales_fact_agg disable constraint fk_agg_sales_channel;

create bitmap index fact_agg_product
on sales_fact_agg(product_dim.product_name)
from sales_fact_agg, product_dim
where sales_fact_agg.product_id = product_dim.product_id
local logging
;

create bitmap index fact_agg_sales_channel
on sales_fact_agg(sales_channel_dim.sales_channel_name)
from sales_fact_agg, sales_channel_dim
where sales_fact_agg.sales_channel_id = sales_channel_dim.sales_channel_id
local logging
;

