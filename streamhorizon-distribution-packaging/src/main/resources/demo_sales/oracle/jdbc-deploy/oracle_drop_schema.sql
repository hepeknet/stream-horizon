/* set your dataowner if desired...*/
--alter session set current_schema = <YourDataOwner>; 

DROP SEQUENCE promotion_dim_seq;

DROP SEQUENCE sales_channel_dim_seq;

DROP SEQUENCE supplier_dim_seq;

DROP SEQUENCE product_dim_seq;

DROP SEQUENCE customer_dim_seq;

DROP SEQUENCE employee_dim_seq;

DROP SEQUENCE sales_fact_seq;

DROP INDEX nk_promotion;

DROP INDEX nk_sales_channel;

DROP INDEX nk_supplier;

DROP INDEX nk_product;

DROP INDEX nk_customer;

DROP INDEX nk_date;

DROP INDEX nk_employee;

DROP INDEX fact_customer;

DROP INDEX  fact_file_id;

ALTER TABLE sales_fact DROP CONSTRAINT fk_customer;

DROP INDEX fact_agg_product;

DROP INDEX fact_agg_sales_channel;

ALTER TABLE sales_fact_agg DROP CONSTRAINT fk_agg_product;

ALTER TABLE sales_fact_agg DROP CONSTRAINT fk_agg_sales_channel;

DROP TABLE sales_fact;

DROP TABLE sales_fact_agg;

DROP TABLE employee_dim;

DROP TABLE date_dim;

DROP TABLE customer_dim;

DROP TABLE product_dim;

DROP TABLE supplier_dim;

DROP TABLE sales_channel_dim;

DROP TABLE promotion_dim;

DROP TABLE sh_metrics;

DROP VIEW sh_etl_bulk_metrics;

DROP VIEW sh_etl_metrics;

DROP VIEW sh_all_db_loader_proc_time;

DROP VIEW sh_dashboard_bulk;

DROP VIEW sh_dashboard_pipe;

DROP VIEW sh_dashboard;

DROP VIEW sh_dashboard_db_loader_mode;

DROP VIEW sh_dashboard_jdbc;

DROP VIEW sh_dashboard_file_2_file;

DROP VIEW sh_all_errors;

DROP VIEW sh_all_metrics;

DROP VIEW sh_all_sales_fact;

DROP VIEW sh_all_sales_fact_agg;

DROP VIEW sh_all_sales_fact_count;

DROP PROCEDURE log_sh_metrics;

DROP PROCEDURE log_sh_metrics_bulk;

DROP PROCEDURE p_sh_external_table_load;


DROP TABLE sh_load_0;
DROP TABLE sh_load_1;
DROP TABLE sh_load_2;
DROP TABLE sh_load_3;
DROP TABLE sh_load_4;
DROP TABLE sh_load_5;
DROP TABLE sh_load_6;
DROP TABLE sh_load_7;
DROP TABLE sh_load_8;
DROP TABLE sh_load_9;
DROP TABLE sh_load_10;
DROP TABLE sh_load_11;
DROP TABLE sh_load_12;
DROP TABLE sh_load_13;
DROP TABLE sh_load_14;
DROP TABLE sh_load_15;
DROP TABLE sh_load_16;
DROP TABLE sh_load_17;
DROP TABLE sh_load_18;
DROP TABLE sh_load_19;
DROP TABLE sh_load_20;
DROP TABLE sh_load_21;
DROP TABLE sh_load_22;
DROP TABLE sh_load_23;
DROP TABLE sh_load_24;
DROP TABLE sh_load_25;
DROP TABLE sh_load_26;
DROP TABLE sh_load_27;
DROP TABLE sh_load_28;
DROP TABLE sh_load_29;
DROP TABLE sh_load_30;
DROP TABLE sh_load_31;
DROP TABLE sh_load_32;
DROP TABLE sh_load_33;
DROP TABLE sh_load_34;
DROP TABLE sh_load_35;
DROP TABLE sh_load_36;
DROP TABLE sh_load_37;
DROP TABLE sh_load_38;
DROP TABLE sh_load_39;
DROP TABLE sh_load_40;
DROP TABLE sh_load_41;
DROP TABLE sh_load_42;
DROP TABLE sh_load_43;
DROP TABLE sh_load_44;
DROP TABLE sh_load_45;
DROP TABLE sh_load_46;
DROP TABLE sh_load_47;
DROP TABLE sh_load_48;
DROP TABLE sh_load_49;

DROP INDEX fact_agg_product;

DROP INDEX fact_agg_sales_channel;

DROP INDEX fact_customer;

DROP INDEX  fact_file_id;

