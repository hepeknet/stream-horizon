/*

BEFORE EXECUTION: Run find and replace for your data file directory (if required). For example replace 'G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\' given below with 'C:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\'
BEFORE EXECUTION: Run find and replace for your data file directory (if required). For example replace 'G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG' given below with 'C:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG'

NOTE: Demo doesn't utilize Windows authentication, it rather creates new MSSQL user & login
NOTE: Create database script will allocate by default 16GB of storage space. (This may be changed by altering number of logs and data files in which case alteration to sh_partitionFunction and sh_partitionScheme is required)
NOTE: You may want to reduce default allocation of SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB which is used for every data and log file (this can be done simply by find & replace of string 'SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB' into for example 'SIZE=500MB,MAXSIZE=10GB,FILEGROWTH=500MB'

*/

USE master;
GO

--kill any outstanding connections in case that create sh database script is being re-run
DECLARE @DatabaseName nvarchar(50)
SET @DatabaseName = N'sh'
DECLARE @SQL varchar(max)
SELECT @SQL = COALESCE(@SQL,'') + 'Kill ' + Convert(varchar, SPId) + ';'
FROM MASTER..SysProcesses
WHERE DBId = DB_ID(@DatabaseName) AND SPId <> @@SPId
PRINT @SQL
EXEC(@SQL)
GO

IF EXISTS(select * from sys.databases where name='sh') DROP DATABASE sh;
GO
CREATE DATABASE sh
ON PRIMARY
  ( NAME='sh_primaryFileGroup',
    FILENAME='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_primaryFileGroupDataFile.mdf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_0
  ( NAME = 'sh_fileGroup_0_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_0_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_0_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_0_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_1
  ( NAME = 'sh_fileGroup_1_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_1_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_1_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_1_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_2
  ( NAME = 'sh_fileGroup_2_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_2_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_2_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_2_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_3
  ( NAME = 'sh_fileGroup_3_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_3_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_3_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_3_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_4
  ( NAME = 'sh_fileGroup_4_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_4_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_4_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_4_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_5
  ( NAME = 'sh_fileGroup_5_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_5_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_5_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_5_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_6
  ( NAME = 'sh_fileGroup_6_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_6_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_6_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_6_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_7
  ( NAME = 'sh_fileGroup_7_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_7_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_7_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_7_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_8
  ( NAME = 'sh_fileGroup_8_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_8_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_8_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_8_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_9
  ( NAME = 'sh_fileGroup_9_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_9_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_9_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_9_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_10
  ( NAME = 'sh_fileGroup_10_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_10_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_10_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_10_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_11
  ( NAME = 'sh_fileGroup_11_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_11_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_11_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_11_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_12
  ( NAME = 'sh_fileGroup_12_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_12_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_12_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_12_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_13
  ( NAME = 'sh_fileGroup_13_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_13_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_13_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_13_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_14
  ( NAME = 'sh_fileGroup_14_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_14_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_14_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_14_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_15
  ( NAME = 'sh_fileGroup_15_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_15_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_15_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_15_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_16
  ( NAME = 'sh_fileGroup_16_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_16_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_16_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_16_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_17
  ( NAME = 'sh_fileGroup_17_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_17_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_17_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_17_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_18
  ( NAME = 'sh_fileGroup_18_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_18_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_18_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_18_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_19
  ( NAME = 'sh_fileGroup_19_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_19_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_19_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_19_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_20
  ( NAME = 'sh_fileGroup_20_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_20_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_20_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_20_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_21
  ( NAME = 'sh_fileGroup_21_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_21_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_21_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_21_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_22
  ( NAME = 'sh_fileGroup_22_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_22_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_22_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_22_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_23
  ( NAME = 'sh_fileGroup_23_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_23_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_23_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_23_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_24
  ( NAME = 'sh_fileGroup_24_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_24_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_24_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_24_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_25
  ( NAME = 'sh_fileGroup_25_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_25_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_25_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_25_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_26
  ( NAME = 'sh_fileGroup_26_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_26_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_26_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_26_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_27
  ( NAME = 'sh_fileGroup_27_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_27_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_27_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_27_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_28
  ( NAME = 'sh_fileGroup_28_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_28_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_28_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_28_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_29
  ( NAME = 'sh_fileGroup_29_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_29_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_29_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_29_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_30
  ( NAME = 'sh_fileGroup_30_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_30_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_30_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_30_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_31
  ( NAME = 'sh_fileGroup_31_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_31_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_31_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_31_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_32
  ( NAME = 'sh_fileGroup_32_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_32_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_32_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_32_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_33
  ( NAME = 'sh_fileGroup_33_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_33_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_33_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_33_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_34
  ( NAME = 'sh_fileGroup_34_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_34_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_34_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_34_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_35
  ( NAME = 'sh_fileGroup_35_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_35_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_35_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_35_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_36
  ( NAME = 'sh_fileGroup_36_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_36_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_36_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_36_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_37
  ( NAME = 'sh_fileGroup_37_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_37_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_37_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_37_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_38
  ( NAME = 'sh_fileGroup_38_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_38_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_38_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_38_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_39
  ( NAME = 'sh_fileGroup_39_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_39_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_39_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_39_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
FILEGROUP sh_fileGroup_40
  ( NAME = 'sh_fileGroup_40_dataFile_1',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_40_dataFile_1.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME = 'sh_fileGroup_40_dataFile_2',
    FILENAME ='G:\Data\MSSQL10_50.RWHU01\MSSQL\DATA\sh_fileGroup_40_dataFile_2.ndf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB)   
LOG ON
  ( NAME='sh_log_1',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_1.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_2',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_2.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_3',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_3.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_4',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_4.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_5',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_5.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_6',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_6.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_7',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_7.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_8',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_8.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_9',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_9.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_10',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_10.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_11',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_11.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_12',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_12.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_13',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_13.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_14',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_14.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_15',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_15.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_16',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_16.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_17',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_17.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_18',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_18.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_19',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_19.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_20',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_20.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_21',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_21.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_22',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_22.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_23',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_23.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_24',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_24.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_25',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_25.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_26',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_26.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_27',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_27.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_28',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_28.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_29',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_29.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_30',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_30.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_31',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_31.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_32',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_32.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_33',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_33.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_34',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_34.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_35',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_35.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_36',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_36.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_37',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_37.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_38',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_38.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_39',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_39.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB),
  ( NAME='sh_log_40',
    FILENAME ='G:\LOG\MSSQL10_50.RWHU01\MSSQL\LOG\sh_log_40.ldf',
    SIZE=50MB,MAXSIZE=10GB,FILEGROWTH=50MB);
GO

ALTER DATABASE sh SET RECOVERY BULK_LOGGED;

USE [sh]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[promotion_dim](
  promotion_id INT IDENTITY(1,1) NOT NULL,
  promotion_name VARCHAR(200) NOT NULL  ,
  discount_pct INT NOT NULL    ,
CONSTRAINT [promotion_pk] PRIMARY KEY (promotion_id)
);

ALTER TABLE [dbo].[promotion_dim] ADD CONSTRAINT nk_promotion UNIQUE (promotion_name,discount_pct);

CREATE TABLE [dbo].[sales_channel_dim] (
  sales_channel_id INT IDENTITY(1,1) NOT NULL,
  sales_channel_name VARCHAR(200) NOT NULL,
CONSTRAINT [sales_channel_pk] PRIMARY KEY (sales_channel_id)
);

ALTER TABLE [dbo].[sales_channel_dim] ADD CONSTRAINT nk_sales_channel UNIQUE (sales_channel_name);

CREATE TABLE [dbo].[supplier_dim] (
  supplier_id INT IDENTITY(1,1) NOT NULL,
  supplier_name VARCHAR(200) NOT NULL,
  supplier_address VARCHAR(500) NULL,
  supplier_phone VARCHAR(100) NULL,
CONSTRAINT [supplier_pk] PRIMARY KEY (supplier_id)
);

ALTER TABLE [dbo].[supplier_dim] ADD CONSTRAINT nk_supplier UNIQUE (supplier_name,supplier_address,supplier_phone);

CREATE TABLE [dbo].[product_dim] (
  product_id INT IDENTITY(1,1) NOT NULL,
  product_name VARCHAR(200) NOT NULL,
  product_model VARCHAR(100) NOT NULL,
  product_category VARCHAR(100) NOT NULL,
  product_cost VARCHAR(100) NOT NULL,
CONSTRAINT [product_pk] PRIMARY KEY (product_id)
);

ALTER TABLE [dbo].[product_dim] ADD CONSTRAINT nk_product UNIQUE (product_name,product_model,product_category,product_cost);

CREATE TABLE [dbo].[customer_dim] (
  customer_id INT IDENTITY(1,1) NOT NULL,
  customer_address VARCHAR(500) NOT NULL,
  customer_name VARCHAR(200) NOT NULL,
  customer_country VARCHAR(100) NOT NULL,
  customer_phone VARCHAR(100) NOT NULL,
CONSTRAINT [customer_pk] PRIMARY KEY (customer_id)
);

ALTER TABLE [dbo].[customer_dim] ADD CONSTRAINT nk_customer UNIQUE (customer_address,customer_name,customer_country,customer_phone);

CREATE TABLE [dbo].[date_dim] (
  date_id INT NOT NULL,
  date_val DATETIME  NOT NULL,
  date_MMDDYYYY VARCHAR(12) NOT NULL,
  year_num INT NOT NULL
CONSTRAINT [date_pk] PRIMARY KEY (date_id)
);

ALTER TABLE [dbo].[date_dim] ADD CONSTRAINT nk_date UNIQUE (date_val);

CREATE TABLE [dbo].[employee_dim] (
  employee_id INT IDENTITY(1,1) NOT NULL,
  employee_name VARCHAR(100) NOT NULL,
  employee_number INT NOT NULL,
CONSTRAINT [employee_pk] PRIMARY KEY (employee_id)
);

ALTER TABLE [dbo].[employee_dim] ADD CONSTRAINT nk_employee UNIQUE (employee_name,employee_number);

CREATE PARTITION FUNCTION sh_partitionFunction (int)
AS RANGE LEFT FOR VALUES (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,
						  20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39
);
GO

CREATE PARTITION SCHEME sh_partitionScheme
AS PARTITION sh_partitionFunction 
TO (sh_fileGroup_0, sh_fileGroup_1, sh_fileGroup_2, sh_fileGroup_3, sh_fileGroup_4, sh_fileGroup_5,
	sh_fileGroup_6, sh_fileGroup_7, sh_fileGroup_8, sh_fileGroup_9, sh_fileGroup_10, sh_fileGroup_11,
	sh_fileGroup_12, sh_fileGroup_13, sh_fileGroup_14, sh_fileGroup_15, sh_fileGroup_16, sh_fileGroup_17,
	sh_fileGroup_18, sh_fileGroup_19, sh_fileGroup_20, sh_fileGroup_21, sh_fileGroup_22, sh_fileGroup_23,
	sh_fileGroup_24, sh_fileGroup_25, sh_fileGroup_26, sh_fileGroup_27, sh_fileGroup_28, sh_fileGroup_29,
	sh_fileGroup_30, sh_fileGroup_31, sh_fileGroup_32, sh_fileGroup_33, sh_fileGroup_34, sh_fileGroup_35,
	sh_fileGroup_36, sh_fileGroup_37, sh_fileGroup_38, sh_fileGroup_39, sh_fileGroup_40
);
GO

CREATE TABLE [dbo].[sales_fact] (
  employee_id INT NOT NULL,
  customer_id INT NOT NULL,
  product_id INT NOT NULL,
  sales_channel_id INT NOT NULL,
  promotion_id INT NOT NULL,
  supplier_id INT NOT NULL,
  booking_date_id INT NOT NULL,
  sales_date_id INT NOT NULL,
  delivery_date_id INT NOT NULL,
  priceBeforeDiscount DECIMAL(15,4) NOT NULL,
  priceAfterDiscount DECIMAL(15,4) NOT NULL,
  saleCosts DECIMAL(15,4) NOT NULL,
  sub INT NOT NULL,
  fileID INT NOT NULL
CONSTRAINT product_fk FOREIGN KEY (product_id) REFERENCES product_dim(product_id),
CONSTRAINT customer_fk FOREIGN KEY (customer_id) REFERENCES customer_dim(customer_id),
CONSTRAINT employee_fk FOREIGN KEY (employee_id) REFERENCES employee_dim(employee_id),
CONSTRAINT supplier_fk FOREIGN KEY (supplier_id) REFERENCES supplier_dim(supplier_id),
CONSTRAINT promotion_fk FOREIGN KEY (promotion_id) REFERENCES promotion_dim(promotion_id),
CONSTRAINT sales_channel_fk FOREIGN KEY (sales_channel_id) REFERENCES sales_channel_dim(sales_channel_id)
)
ON sh_partitionScheme (sub)
;
GO

ALTER TABLE [dbo].[sales_fact] SET (LOCK_ESCALATION = AUTO);--it is default but just to make sure it is on force...
GO


USE master;
GO

If Exists (select loginname from master.dbo.syslogins where name = 'sh' and dbname = 'sh') 
	BEGIN
		DROP USER sh;
		DROP LOGIN sh;
	END


CREATE LOGIN sh WITH PASSWORD = 'StremHorizon369', DEFAULT_DATABASE = sh ,CHECK_POLICY = OFF, CHECK_EXPIRATION = OFF;
GO
CREATE USER sh FOR LOGIN sh;
GO
EXEC sys.sp_addsrvrolemember @loginame = N'sh', @rolename = N'sysadmin'
GO

USE sh;
GO

create table [dbo].[sh_metrics] (
servername VARCHAR(100) null,
instancenumber int null,
instancestarted VARCHAR(100) null,
eventName VARCHAR(100) null,
fileReceived datetime null,
etlThreadID int null,
fileName VARCHAR(100) null,	 
fileProcessingStart datetime  null,
fileProcessingFinish datetime  null,
fileProcessingMillis int null,    
fileJdbcInsertStart datetime  null,
fileJdbcInsertFinish datetime  null,    
jdbcProcessingMillis int null,
bulkFileSubmitted VARCHAR(100) null,
dbThreadID int null,
bulkFilePath VARCHAR(200) null,
bulkFileName VARCHAR(100) null,
fileRecordCount int null,
bulkFileReceived datetime  null,
bulkFileProcessingStart datetime  null,
bulkFileProcessingFinish datetime  null,
bulkProcessingMillis int null,
etlCompletionFlag VARCHAR(100) null,
bulkCompletionFlag VARCHAR(100) null,
etlErrorDescription VARCHAR(1000) null,
bulkErrorDescription VARCHAR(1000) null,
recordInserted datetime  null
); 

 
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'FN' AND name = 'to_date') 
	DROP FUNCTION to_date;
GO


CREATE FUNCTION to_date(@dateInMillis numeric(19,0))
RETURNS datetime 
WITH EXECUTE AS CALLER
AS
BEGIN
	DECLARE @total bigint 
	DECLARE @seconds int
	DECLARE @milliseconds int
	DECLARE @result datetime = '1970-1-1'

	SET @total = @dateInMillis
	SET @seconds = @total / 1000
	SET @milliseconds = @total % 1000
	SET @result = DATEADD(SECOND, @seconds,@result)
	SET @result = DATEADD(MILLISECOND, @milliseconds,@result)
	RETURN(@result);
END;
GO


IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'log_sh_metrics') 
	DROP PROCEDURE log_sh_metrics;
GO

CREATE PROCEDURE log_sh_metrics
(
@servername nvarchar(1000),@instancenumber int,@instancestarted numeric(19,0) ,@eventName nvarchar(1000),@fileReceived numeric(19,0) ,@etlThreadID int ,@fileName nvarchar(1000),@fileProcessingStart numeric(19,0),
@fileProcessingFinish numeric(19,0) ,@fileJdbcInsertStart numeric(19,0) ,@fileJdbcInsertFinish numeric(19,0) ,@bulkFileSubmitted nvarchar(50),@dbThreadID int,@bulkFilePath nvarchar(1000),@bulkFileName nvarchar(1000),
@fileRecordCount int,@bulkFileReceived numeric(19,0),@bulkFileProcessingStart numeric(19,0),@bulkFileProcessingFinish numeric(19,0),@etlCompletionFlag nvarchar(1), @etlErrorDescription nvarchar(4000)
)
AS
BEGIN
DECLARE @fileProcessingStart_ts datetime
DECLARE @instancestarted_ts datetime
DECLARE @fileReceived_ts datetime
DECLARE @fileProcessingFinish_ts datetime
DECLARE @fileJdbcInsertStart_ts datetime
DECLARE @fileJdbcInsertFinish_ts datetime
DECLARE @bulkFileReceived_ts datetime
DECLARE @fileProcessingMillis int
DECLARE @jdbcProcessingMillis int


SET @fileProcessingStart_ts = sh.dbo.to_date(@fileProcessingStart)
SET @instancestarted_ts = sh.dbo.to_date(@instancestarted)
SET @fileReceived_ts = sh.dbo.to_date(@fileReceived)
SET @fileProcessingFinish_ts = sh.dbo.to_date(@fileProcessingFinish)
SET @fileJdbcInsertStart_ts = sh.dbo.to_date(@fileJdbcInsertStart)
SET @fileJdbcInsertFinish_ts = sh.dbo.to_date(@fileJdbcInsertFinish)
SET @bulkFileReceived_ts = sh.dbo.to_date(@bulkFileReceived)
SET @fileProcessingMillis = @fileProcessingFinish - @fileProcessingStart
SET @jdbcProcessingMillis = @fileJdbcInsertFinish - @fileJdbcInsertStart


insert into sh_metrics 
(servername,instancenumber,instancestarted,eventName,fileReceived,etlThreadID,fileName,fileProcessingStart,fileProcessingFinish,fileProcessingMillis,fileJdbcInsertStart,
fileJdbcInsertFinish,jdbcProcessingMillis,bulkFileSubmitted,dbThreadID,bulkFilePath,bulkFileName,fileRecordCount,bulkFileReceived,etlCompletionFlag,etlErrorDescription,recordInserted)
values(@servername,@instancenumber,@instancestarted_ts,'<'+@eventName+'>',@fileReceived_ts,@etlThreadID,
@fileName,@fileProcessingStart_ts,@fileProcessingFinish_ts,@fileProcessingMillis,@fileJdbcInsertStart_ts,@fileJdbcInsertFinish_ts,@jdbcProcessingMillis,@bulkFileSubmitted,@dbThreadID,@bulkFilePath,
@bulkFileName,@fileRecordCount,@bulkFileReceived_ts,@etlCompletionFlag,@etlErrorDescription,GETDATE());

END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'log_sh_metrics_bulk') 
	DROP PROCEDURE log_sh_metrics_bulk;
GO

CREATE PROCEDURE log_sh_metrics_bulk
(
@servername nvarchar(1000),@instancenumber int,@instancestarted numeric(19,0) ,@eventName nvarchar(1000),
@bulkFile nvarchar(1000),@bulkFileProcessingStart numeric(19,0),@bulkFileProcessingFinish numeric(19,0), 
@bulkCompletionFlag nvarchar(1), @bulkErrorDesc nvarchar(4000), @bulkThreadId int, @bulkFilePath nvarchar(1000)
)
AS
BEGIN
DECLARE @instancestarted_ts datetime
DECLARE @bulkFileReceived_ts datetime
DECLARE @bulkFileProcessingStart_ts datetime
DECLARE @bulkFileProcessingFinish_ts datetime
DECLARE @bulkProcessingMillis int    

SET @bulkFileProcessingStart_ts = sh.dbo.to_date(@bulkFileProcessingStart)
SET @bulkFileProcessingFinish_ts = sh.dbo.to_date(@bulkFileProcessingFinish)

SET @bulkProcessingMillis = @bulkFileProcessingFinish - @bulkFileProcessingStart
SET @instancestarted_ts = sh.dbo.to_date(@instancestarted)

insert into sh_metrics 
(servername,instancenumber,instancestarted,eventName,bulkErrorDescription,bulkCompletionFlag,bulkFileProcessingStart,
bulkFileProcessingFinish,bulkProcessingMillis,bulkFileName,recordInserted,dbThreadID,bulkFilePath) 
values(@servername,@instancenumber,@instancestarted_ts,'<' + @eventName + '>',
@bulkErrorDesc,@bulkCompletionFlag,@bulkFileProcessingStart_ts,@bulkFileProcessingFinish_ts,
@bulkProcessingMillis,@bulkFile,GETDATE(),@bulkThreadId,@bulkFilePath);
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'V' AND name = 'sh_dashboard') 
	DROP VIEW sh_dashboard;
GO

CREATE VIEW sh_dashboard as
select TOP 1000
servername as 'server name',instancenumber as 'instance number', instancestarted as 'instance started',
         (b."total file records processed" /
            (
			 CAST(DATEPART(MINUTE, b."processing window") as int)*60 + CAST(DATEPART(SECOND, b."processing window") as int)
            )
         )
as 'throughput records/second',
 CAST(DATEPART(MINUTE, b."processing window") as int)*60 + CAST(DATEPART(SECOND, b."processing window") as int) as "processing window (sec)",
 b."total file records processed"
from
(
select servername,instancestarted,cast(sum(filerecordcount) as int) as 'total file records processed',
coalesce(max(filejdbcinsertfinish),max(fileprocessingfinish)) - min(fileprocessingstart)  as 'processing window',instancenumber
from 
(
select 
servername,instancestarted,filerecordcount,filejdbcinsertfinish,fileprocessingstart,
bulkFileName,fileprocessingfinish, instancenumber
from sh_metrics
where filename is not null and eventname='<afterFeedProcessingCompletion>'
)a 
group by servername,instancenumber,instancestarted
) b 
order by instancestarted desc, servername asc,instancenumber asc
GO


IF EXISTS (SELECT * FROM sys.objects WHERE type = 'V' AND name = 'sh_etl_bulk_metrics') 
	DROP VIEW sh_etl_bulk_metrics;
GO

CREATE VIEW sh_etl_bulk_metrics as
select TOP 100000 etl.servername,etl.instancestarted, fileProcessingStart,fileProcessingFinish,"etl recordInserted",bulkFileProcessingStart,bulkFileProcessingFinish,"bulk recordInserted"
from 
(
select 
servername,instancestarted,filerecordcount,filejdbcinsertfinish,fileprocessingstart,bulkFileName,fileProcessingFinish,recordInserted as "etl recordInserted"
from sh_metrics
where  instancestarted = (select max(instancestarted) from sh_metrics) and filename is not null and eventname='<afterFeedProcessingCompletion>'
)etl,
(
select 
servername,instancestarted,bulkfileprocessingfinish,bulkFileName,bulkFileProcessingStart,recordInserted as "bulk recordInserted"
from sh_metrics
where instancestarted = (select max(instancestarted) from sh_metrics) and filename is null and eventname='<onBulkLoadCompletion>'
)db
where etl.servername=db.servername and etl.instancestarted = db.instancestarted and etl.bulkFileName = db.bulkFileName 
order by instancestarted desc, servername asc
GO

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'V' AND name = 'sh_all_db_loader_proc_time') 
	DROP VIEW sh_all_db_loader_proc_time;
GO

CREATE VIEW  sh_all_db_loader_proc_time as
select TOP 1000 servername as "server name",instancenumber as "instance number", instancestarted as "instance started", 
max(bulkFileProcessingFinish) - min(bulkFileProcessingStart)  as "processing window", 
CAST(DATEPART(MINUTE, max(bulkFileProcessingFinish) - min(bulkFileProcessingStart)) as int)*60 + CAST(DATEPART(SECOND, max(bulkFileProcessingFinish) - min(bulkFileProcessingStart)) as int) as "processing window (sec)",
count(*) as "files processed"
from sh_metrics 
where  eventname='<onBulkLoadCompletion>'
group by servername,instancenumber,instancestarted
order by instancestarted desc, servername asc,instancenumber asc
GO


IF EXISTS (SELECT * FROM sys.objects WHERE type = 'V' AND name = 'sh_dashboard_db_loader_mode') 
	DROP VIEW sh_dashboard_db_loader_mode;
GO

CREATE VIEW  sh_dashboard_db_loader_mode as
select TOP 1000
c."server name",c."instance number", c."instance started",
         (c."total file records processed" /
            (
			 CAST(DATEPART(MINUTE, c."processing window") as int)*60 + CAST(DATEPART(SECOND, c."processing window") as int)
            )
         )
as 'throughput records/second',
CAST(DATEPART(MINUTE, c."processing window") as int)*60 + CAST(DATEPART(SECOND, c."processing window") as int) as "processing window (sec)",
 c."total file records processed",
 c."files processed"
from(
select * from sh_all_db_loader_proc_time a,
(select count(*) as "total file records processed" from sales_fact) b
)c
GO


IF EXISTS (SELECT * FROM sys.objects WHERE type = 'V' AND name = 'sh_dashboard_bulk') 
	DROP VIEW sh_dashboard_bulk;
GO

CREATE VIEW sh_dashboard_bulk as
select TOP 10000
a.servername as "server name",a.instancenumber as "instance number",a.instancestarted as "instance started",
         (a."total file records processed" /
            (
			 CAST(DATEPART(MINUTE, a."processing window") as int)*60 + CAST(DATEPART(SECOND, a."processing window") as int)
            )
         ) as "throughput records/second",
  CAST(DATEPART(MINUTE, a."processing window") as int)*60 + CAST(DATEPART(SECOND, a."processing window") as int) as "processing window (sec)",
 a."total file records processed",
 a."total files processed" 
from
(
	select etl.servername,etl.instancenumber,etl.instancestarted,count(*) as "total files processed",
	sum(filerecordcount) as "total file records processed",(max(bulkfileprocessingfinish) - min(fileprocessingstart))  as "processing window"
	from 
	(
		select 
		servername,instancenumber,instancestarted,filerecordcount,fileprocessingstart,bulkFileName
		from sh_metrics
		where filename is not null and eventname='<afterFeedProcessingCompletion>'
	)etl,
	(
		select 
		bulkfileprocessingfinish,bulkFileName
		from sh_metrics
		where filename is null and eventname='<onBulkLoadCompletion>'
	)db
	where etl.bulkFileName = db.bulkFileName
	group by etl.servername,etl.instancenumber,etl.instancestarted
)a 
order by a.instancestarted desc, a.servername asc,a.instancenumber asc
GO


IF EXISTS (SELECT * FROM sys.objects WHERE type = 'V' AND name = 'sh_etl_metrics') 
	DROP VIEW sh_etl_metrics;
GO

CREATE VIEW sh_etl_metrics as
select TOP 1000000
etl.servername, etl.instancestarted,fileprocessingstart,fileprocessingfinish,"etl recordInserted",bulkfileprocessingstart,bulkfileprocessingfinish, "bulk recordInserted"
from (select servername,instancestarted,filerecordcount,filejdbcinsertfinish,fileprocessingstart,bulkfilename,fileprocessingfinish, recordinserted as "etl recordInserted"
               from sh_metrics
              where instancestarted =
                       (select max (instancestarted) from sh_metrics)
                    and filename is not null
                    and eventname = '<afterFeedProcessingCompletion>') etl,
            (select servername,instancestarted,bulkfileprocessingfinish,bulkfilename,bulkfileprocessingstart,recordinserted as "bulk recordInserted"
               from sh_metrics
              where instancestarted =
                       (select max (instancestarted) from sh_metrics)
                    and filename is null
                    and eventname = '<onBulkLoadCompletion>') db
      where     etl.servername = db.servername
            and etl.instancestarted = db.instancestarted
            and etl.bulkfilename = db.bulkfilename
   order by instancestarted desc, servername asc
GO


IF EXISTS (SELECT * FROM sys.objects WHERE type = 'V' AND name = 'sh_dashboard_jdbc') 
	DROP VIEW sh_dashboard_jdbc;
GO

CREATE VIEW sh_dashboard_jdbc as
select * from sh_dashboard
GO

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'V' AND name = 'sh_dashboard_file_2_file') 
	DROP VIEW sh_dashboard_file_2_file;
GO

CREATE VIEW sh_dashboard_file_2_file as
select * from sh_dashboard
GO

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'V' AND name = 'sh_dashboard_pipe') 
	DROP VIEW sh_dashboard_pipe;
GO

CREATE VIEW sh_dashboard_pipe as
select * from sh_dashboard
GO

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'V' AND name = 'sh_all_errors') 
	DROP VIEW sh_all_errors
GO

CREATE VIEW sh_all_errors as
select *  from sh_metrics where etlCompletionFlag = 'F' or bulkCompletionFlag = 'F';
GO

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'V' AND name = 'sh_all_metrics') 
	DROP VIEW sh_all_metrics
GO

CREATE VIEW sh_all_metrics as
select *  from sh_metrics
GO


IF EXISTS (SELECT * FROM sys.objects WHERE type = 'V' AND name = 'sh_all_sales_fact') 
	DROP VIEW sh_all_sales_fact
GO

CREATE VIEW sh_all_sales_fact as
select *  from sales_fact
GO


IF EXISTS (SELECT * FROM sys.objects WHERE type = 'V' AND name = 'sh_all_sales_fact_count') 
	DROP VIEW sh_all_sales_fact_count;
GO

CREATE VIEW sh_all_sales_fact_count as
select count(*) as record_count  from sales_fact;
GO


--generate one year worth of calendar data (for 2014)
BEGIN
DECLARE @date datetime = '12/31/2013'
DECLARE @SQL varchar(max)
DECLARE @dateInt int = -1
truncate table dbo.date_dim
	WHILE (SELECT count(*) from dbo.date_dim)  < 365
	BEGIN	
		SELECT @SQL = ''	
		SELECT @dateInt = ''
		SELECT @date = dateadd(dd, 1, @date)
		SELECT @dateInt = year(@date)*10000 + month(@date)* 100 + day(@date)
		SELECT @SQL = COALESCE(@SQL,'') + 'insert into date_dim values (' + 
				LEFT(CONVERT(VARCHAR, @dateInt, 120),8) + ',''' + 
				LEFT(CONVERT(VARCHAR, @date, 120),10) + ''',''' +
				LEFT(CONVERT(VARCHAR, @dateInt, 120),8) + ''',' +
				LEFT(CONVERT(VARCHAR, year(@date), 120),4) + ');'
		--PRINT ' SQL: ' +  @SQL
		EXEC(@SQL)
	END
END




