/*

NOTE: if you wish to set your data owner please uncomment first command of this script

*/


/* set your dataowner if desired...*/
--alter session set current_schema = <YourDataOwner>; 

DROP SEQUENCE promotion_dim_seq;

DROP SEQUENCE sales_channel_dim_seq;

DROP SEQUENCE supplier_dim_seq;

DROP SEQUENCE product_dim_seq;

DROP SEQUENCE customer_dim_seq;

DROP SEQUENCE employee_dim_seq;

DROP INDEX promotion_nk;

DROP INDEX sales_channe_nk;

DROP INDEX supplier_nk;

DROP INDEX product_nk;

DROP INDEX customer_nk;

DROP INDEX date_nk;

DROP INDEX employee_nk;

DROP TABLE sales_fact;

DROP TABLE employee_dim;

DROP TABLE date_dim;

DROP TABLE customer_dim;

DROP TABLE product_dim;

DROP TABLE supplier_dim;

DROP TABLE sales_channel_dim;

DROP TABLE promotion_dim;

DROP TABLE sh_metrics;

DROP VIEW sh_etl;

DROP VIEW sh_dashboard;

DROP PROCEDURE log_sh_metrics;

DROP PROCEDURE log_sh_metrics_bulk;

DROP PROCEDURE p_sh_external_table_load;


