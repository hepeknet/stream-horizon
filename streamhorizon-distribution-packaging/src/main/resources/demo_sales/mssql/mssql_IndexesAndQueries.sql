--we recommend building indexes against empty fact table if possible using create index statement (preferably partitioned indexes). however, you may decide to run set of alter index statements depending on your usecase. all code is provided below, feel free to use snippets that best suit your needs. 

--please ensure that you truncate tables before index creation (if not index creation may take a longer than a second)
truncate table sales_fact;		
truncate table sh_metrics;

--drop index statements
DROP INDEX [idx_product_fact] ON [dbo].[sales_fact] WITH ( ONLINE = OFF )
DROP INDEX idx_customer_fact ON [dbo].[sales_fact] WITH ( ONLINE = OFF )
DROP INDEX idx_sales_channel_fact ON [dbo].[sales_fact] WITH ( ONLINE = OFF )

--partitioned indexes
CREATE NONCLUSTERED INDEX idx_product_fact ON sales_fact (product_id) WITH (SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF) ON sh_partitionScheme(sub);
CREATE NONCLUSTERED INDEX idx_customer_fact ON sales_fact (customer_id) WITH (SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF) ON sh_partitionScheme(sub);
CREATE NONCLUSTERED INDEX idx_sales_channel_fact ON sales_fact (sales_channel_id) WITH (SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF) ON sh_partitionScheme(sub);

--indexes (not partitioned)
CREATE NONCLUSTERED INDEX idx_product_fact ON sales_fact (product_id);
CREATE NONCLUSTERED INDEX idx_customer_fact ON sales_fact (customer_id);
CREATE NONCLUSTERED INDEX idx_sales_channel_fact ON sales_fact (sales_channel_id);

--alter index statements (to be executed before data load)
ALTER INDEX idx_product_fact ON sales_fact DISABLE;
ALTER INDEX idx_customer_fact ON sales_fact DISABLE;
ALTER INDEX idx_sales_channel_fact ON sales_fact DISABLE;

--rebuild index statements (to be executed after data load)
ALTER INDEX idx_product_fact ON sales_fact REBUILD;
ALTER INDEX idx_customer_fact ON sales_fact REBUILD;
ALTER INDEX idx_sales_channel_fact ON sales_fact REBUILD;

--test sql queries (giving metrics of reduced query latency in running sql queries against transactional fact table which is suitably indexed for given dimensions)
select product_id, COUNT(*) from sh.dbo.sales_fact where product_id = (select MIN(product_id) from product_dim) group by product_id
select product_id, sum(saleCosts) from sh.dbo.sales_fact where product_id = (select MIN(product_id) from product_dim) group by product_id


