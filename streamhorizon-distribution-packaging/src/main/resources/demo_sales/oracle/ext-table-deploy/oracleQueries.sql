--***************************************************************************************************--
--*********************************                                 *********************************--
--*********************************     FACT TABLE QUERIES          *********************************--
--*********************************                                 *********************************--
--***************************************************************************************************--

alter session set cursor_sharing=exact;

set timing on;
select /* star_transformation index(f fact_customer) */
	   d.customer_name as "ATTRIBUTE",  count(*) as "AGGREGATED RECORDS"
from sales_fact partition (P_20140107) f 
join customer_dim d  
 on (f.customer_id = d.customer_id) 
where f.booking_date_id = 20140107 
  and d.customer_name = 'Pamela Mendoza' 
group by d.customer_name; 


set timing on;
select /* star_transformation index(f fact_customer) */
       d.customer_name as "ATTRIBUTE 1", d2.supplier_name as "ATTRIBUTE 2",  count(*) as "AGGREGATED RECORDS"
from sales_fact partition (P_20140107) f 
join customer_dim d  on (f.customer_id = d.customer_id)
join supplier_dim d2  on (f.supplier_id = d2.supplier_id) 
where f.booking_date_id = 20140107 
  and d.customer_name = 'Pamela Mendoza' 
  and d2.supplier_name = 'Plajo'
group by d.customer_name, d2.supplier_name;  

--***************************************************************************************************--
--*********************************                                            **********************--
--*********************************     AGGREGATE FACT TABLE QUERIES           **********************--
--*********************************                                            **********************--
--***************************************************************************************************--



set timing on;
select /*+ star_transformation index(f fact_agg_sales_channel) */
       d.sales_channel_name as "ATTRIBUTE", count(*) as "     AGGREGATED RECORDS", sum(recordsaggregated) as " TOTAL AGGREGATED FACT RECORDS"
from sales_fact_agg partition (P_20140107) f 
join sales_channel_dim d  on (f.sales_channel_id = d.sales_channel_id) 
where f.booking_date_id = 20140107  
  and d.sales_channel_name = 'Onsite Consultant Sales' 
group by d.sales_channel_name; 



--***************************************************************************************************--
--*********************************                                            **********************--
--*********************************                    MISC                    **********************--
--*********************************                                            **********************--
--***************************************************************************************************--
 

alter system flush shared_pool;
alter system flush buffer_cache;




