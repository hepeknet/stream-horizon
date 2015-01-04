CALL SQLCMD -S localhost\TG4 -U sh -P StremHorizon369 -Q "BULK INSERT [sh].[dbo].[sales_fact] FROM 'C:\data\bulk\%1' WITH (MAXERRORS=0,ROWS_PER_BATCH=11000,FIELDTERMINATOR=',',ERRORFILE ='C:\data\error\%1.err')"


 



