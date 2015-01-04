CALL SQLCMD -S server\myInstance -U sh -P StremHorizon369 -Q "BULK INSERT [sh].[dbo].[sales_fact] FROM 'G:\Data\bulk\%1' WITH (MAXERRORS=0,ROWS_PER_BATCH=11000,FIELDTERMINATOR=',',ERRORFILE ='G:\Data\error\%1.err')"


 



