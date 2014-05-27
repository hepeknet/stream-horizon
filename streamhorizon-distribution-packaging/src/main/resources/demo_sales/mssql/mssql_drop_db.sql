USE master;
GO

IF EXISTS(select * from sys.databases where name='sh') DROP DATABASE sh;
GO