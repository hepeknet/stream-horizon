alter session set current_schema = sh; 

alter index fact_file_id compute statistics;
alter index fact_customer compute statistics;

BEGIN
  FOR rec IN
    (SELECT subpartition_name
     FROM all_tab_subpartitions
     WHERE table_owner = upper('sh')
     AND table_name = upper('sales_fact'))
   LOOP
     SYS.DBMS_STATS.GATHER_TABLE_STATS(
             OwnName          => 'sh',
             TabName          => upper('sales_fact'),
             PartName         => rec.subpartition_name,
             Granularity      => 'SUBPARTITION',
             Estimate_Percent => 10, 
             Degree           => NULL,
             Cascade          => TRUE);
   END LOOP;
END;
/

BEGIN
     SYS.DBMS_STATS.GATHER_TABLE_STATS(
             OwnName          => upper('sh'),
             TabName          => upper('sales_fact'),
             PartName         => null, 
             Granularity      => 'ALL',
             Estimate_Percent => 10, 
             Degree           => NULL,
             Cascade          => TRUE);
END;
/

alter index fact_agg_product compute statistics;

alter index fact_agg_sales_channel compute statistics;


BEGIN
  FOR rec IN
    (SELECT subpartition_name
     FROM all_tab_subpartitions
     WHERE table_owner = upper('sh')
     AND table_name = upper('sales_fact_agg'))
   LOOP
     SYS.DBMS_STATS.GATHER_TABLE_STATS(
             OwnName          => 'sh',
             TabName          => upper('sales_fact_agg'),
             PartName         => rec.subpartition_name,
             Granularity      => 'SUBPARTITION',
             Estimate_Percent => 10, 
             Degree           => NULL,
             Cascade          => TRUE);
   END LOOP;
END;
/

BEGIN
     SYS.DBMS_STATS.GATHER_TABLE_STATS(
             OwnName          => upper('sh'),
             TabName          => upper('sales_fact_agg'),
             PartName         => null,
             Granularity      => 'ALL',
             Estimate_Percent => 10, 
             Degree           => NULL,
             Cascade          => TRUE);
END;
/

EXEC DBMS_STATS.gather_schema_stats('sh',DBMS_STATS.AUTO_SAMPLE_SIZE);

alter index fact_agg_product compute statistics;

alter index fact_agg_sales_channel compute statistics;

alter index fact_customer compute statistics;

alter index fact_file_id compute statistics;

