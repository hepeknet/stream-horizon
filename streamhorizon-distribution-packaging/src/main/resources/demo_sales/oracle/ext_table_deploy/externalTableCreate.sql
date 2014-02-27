/*

NOTE: to create external table directory please uncomment and modify first two commands of the script as advised below before you execute it
NOTE: to assign correct priviledges please AFTER you execute this script run command:       
            GRANT ALL ON DIRECTORY EXT_LOADER_DATA TO <YOUR USER COMES HERE>                
            GRANT ALL ON DIRECTORY LOG TO <YOUR USER COMES HERE>

*/



/* modify this command to your own directory on your server and make sure that read/write priviledges are correctly assigned  */
--create or replace directory EXT_LOADER_DATA as 'your bulk file directory (as it is setup in  engine-config.xml <bulkOutputDirectory> tag)'
--create or replace directory LOG as 'directory for logging erros (please choose any on your OS)'


declare 
parallelism integer :=50;
createTalbe varchar2(4000) :=
' CREATE TABLE sh_load_$MOD_ID$
(
  employee_id NUMBER,
  customer_id NUMBER,
  product_id NUMBER, 
  sales_channel_id NUMBER,
  promotion_id NUMBER,
  supplier_id NUMBER,
  booking_date_id NUMBER,
  sales_date_id NUMBER,
  delivery_date_id NUMBER,
  priceBeforeDiscount FLOAT,
  priceAfterDiscount FLOAT,
  saleCosts FLOAT
)
ORGANIZATION EXTERNAL
   (
   TYPE ORACLE_LOADER
    DEFAULT DIRECTORY EXT_LOADER_DATA
    ACCESS PARAMETERS
      (RECORDS DELIMITED BY NEWLINE        
      READSIZE = 1048576    
      DISABLE_DIRECTORY_LINK_CHECK
       IO_OPTIONS (DIRECTIO)
       LOGFILE LOG:''''streamHorizon_$MOD_ID$.log''''
       BADFILE LOG:''''streamHorizon_$MOD_ID$.bad''''
       DISCARDFILE LOG:''''streamHorizon_$MOD_ID$.dsc''''
       FIELDS TERMINATED BY '''','''' 
      MISSING FIELD VALUES ARE NULL 
       (       
              employee_id INTEGER EXTERNAL(255),
              customer_id INTEGER EXTERNAL(255),
              product_id INTEGER EXTERNAL(255),
              sales_channel_id INTEGER EXTERNAL(255),
              promotion_id INTEGER EXTERNAL(255),
              supplier_id INTEGER EXTERNAL(255),
              booking_date_id INTEGER EXTERNAL(255),
              sales_date_id INTEGER EXTERNAL(255),
              delivery_date_id INTEGER EXTERNAL(255),
              priceBeforeDiscount DECIMAL EXTERNAL(255),  
              priceAfterDiscount DECIMAL EXTERNAL(255),  
              saleCosts DECIMAL EXTERNAL(255)
          )
      )
    LOCATION (''''$FILE_NAME$'''')
   )
noparallel
reject LIMIT 0
nomonitoring';
wrapBlockStart varchar2(4000) := 'declare
    tableDoesNotExist exception;
    pragma exception_init(tableDoesNotExist, -942);
  begin
    begin
      execute immediate ''drop table sh_load_$MOD_ID$'';
    exception
      when tableDoesNotExist then
        null;
    end;
    execute immediate';
wrapBlockEnd varchar2(20) :=' ; end;';
delimiter varchar2(10):=','; line varchar2(300);iteration integer; cmd varchar2(4000); 
begin     
     for i in 0..parallelism loop  --create ext table...
         cmd :=wrapBlockStart||''''||createTalbe||''''||wrapBlockEnd;         
         cmd :=REPLACE(cmd,'$FILE_NAME$',i);
         cmd :=REPLACE(cmd,'$MOD_ID$',i);         
         dbms_output.put_line('  '||cmd);     
         execute immediate cmd;
         cmd := null;                            
     end loop;
end;



CREATE OR REPLACE procedure p_sh_external_table_load
  (businessDate in INTEGER,dbthreadID in integer, fileName varchar2)
is
    cmd varchar2(4000):=null;  factPartitions integer :=50; startLoad timestamp; endLoad timestamp; duration number;
    function milliseconddiff (p_d1 in timestamp,   p_d2 in timestamp)
       return number as
       l_result   number;
    begin
       l_result   :=
             (extract(day    from p_d2 - p_d1)) * 86400000
           + (extract(hour   from p_d2 - p_d1)) * 3600000
           + (extract(minute from p_d2 - p_d1)) * 60000
           + (extract(second from p_d2 - p_d1)) * 1000;
       return l_result;
    end;    
begin
         startLoad :=systimestamp; 
         cmd := 'alter table sh_load_'||dbthreadID||' location ('''||fileName||''')';
         dbms_output.put_line('sql:     '||cmd);     
         execute immediate cmd;         
         cmd:=' insert  /*+ append_values  */ into sales_fact subpartition (P_'||to_char(businessDate)||'_SP_'|| mod(dbthreadID,factPartitions)  ||')   
                             (product_id, customer_id, employee_id, supplier_id, sales_channel_id, promotion_id, booking_date_id, sales_date_id, delivery_date_id, priceBeforeDiscount, priceAfterDiscount, saleCosts, sub) 
                    select  product_id, customer_id, employee_id, supplier_id, sales_channel_id, promotion_id, booking_date_id, sales_date_id, delivery_date_id, priceBeforeDiscount, priceAfterDiscount, saleCosts, '||dbthreadID||' 
                    from sh_load_'||dbthreadID;
        execute immediate cmd;   
        commit;
        endLoad := systimestamp;
        duration := milliseconddiff(startLoad,endLoad);
   EXCEPTION 
    WHEN OTHERS THEN  
        cmd := cmd||' exception error:    SQLERRM '||SQLERRM||'          SQLCODE:   '||SQLCODE;
        update  sh_metrics set bulkErrordescription= cmd , bulkCompletionflag='F' where bulkFileName = fileName;
        commit;        
end p_sh_external_table_load;

