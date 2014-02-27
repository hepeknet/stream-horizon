echo off
SET /a i=0

:loop
IF %i%==1000 GOTO END
copy sales_20140107_data.csv  sales_20140107_data_%i%.csv
SET /a i=%i%+1
GOTO LOOP

:end