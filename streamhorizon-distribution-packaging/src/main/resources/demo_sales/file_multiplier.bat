echo off
SET /a i=0

:loop
IF %i%==500 GOTO END
copy sales_20140107_data.data  sales_20140107_data_%i%.csv
SET /a i=%i%+1
GOTO LOOP

:end