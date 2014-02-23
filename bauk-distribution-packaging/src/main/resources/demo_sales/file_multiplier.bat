echo off
SET /a i=0

:loop
IF %i%==10 GOTO END
copy sh_demo_sales_data_sample_100k.csv  sh_demo_sales_data_%i%.csv
SET /a i=%i%+1
GOTO LOOP

:end