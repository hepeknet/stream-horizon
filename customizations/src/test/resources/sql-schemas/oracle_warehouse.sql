CREATE TABLE WAREHOUSE.RWH_RISK_FACT
(
  RWH_DEAL_DIM_WID        NUMBER,
  RWH_PRDCT_DIM_WID       NUMBER,
  RWH_FACTOR_DIM_WID      NUMBER,
  RWH_UNDERLYING_DIM_WID  NUMBER,
  RWH_CURVETYPE_DIM_WID   NUMBER,
  RWH_DATE_DIM_WID        NUMBER,
  RWH_CURRENCY_DIM_WID    NUMBER,
  RWH_ORG_DIM_WID         NUMBER,
  RWH_PARTY_DIM_WID       NUMBER,
  RWH_PORTFOLIO_DIM_WID   NUMBER,
  RWH_ETL_BATCH_WID       NUMBER,
  RWH_DEALRISK_WID        NUMBER,
  RWH_ERROR_DIM_WID       NUMBER,-- always set to -1
  RISK_VALUE              NUMBER,--LocalValue
  BASE_VALUE              NUMBER,--BaseValue
  LOCAL_VALUE             NUMBER,--RiskValue
  CURRENT_FLAG            CHAR(1 BYTE),-- always hardcode to 'Y'
  ISADJUSTMENT            CHAR(1 BYTE),-- always hardcode to 'N'
  RWH_NOTIFICATION_WID    NUMBER
);

CREATE TABLE WAREHOUSE.RWH_UNDERLYING_DIM
(
  UNDERLYING_DIM_WID    NUMBER,
  UNDERLYING_NAME       VARCHAR2(1000 BYTE),
  UNDERLYING_DESC       VARCHAR2(4000 BYTE),
  RISK_UNDERLYING_NAME  VARCHAR2(1000 BYTE),
  RISK_UNDERLYING_DESC  VARCHAR2(4000 BYTE)
);

CREATE SEQUENCE rwh_underlying_seq;
CREATE UNIQUE INDEX underlying_uindex ON RWH_UNDERLYING_DIM (UNDERLYING_NAME, RISK_UNDERLYING_NAME);

CREATE TABLE WAREHOUSE.RWH_RISK_STATUS_DIM
(
  RISK_STATUS_DIM_WID  NUMBER,
  STATUS_CODE          VARCHAR2(1000 BYTE),
  STATUS_DESCRIPTION   VARCHAR2(4000 BYTE),
  STATUS_FLAG          CHAR(1 BYTE)
);

CREATE SEQUENCE rwh_risk_status_seq;
CREATE UNIQUE INDEX rwh_risk_status_uindex ON RWH_RISK_STATUS_DIM (STATUS_CODE, STATUS_DESCRIPTION, STATUS_FLAG);

CREATE TABLE WAREHOUSE.RWH_PRODUCT_DIM
(
  PRODUCT_DIM_WID          NUMBER,
  PRODUCT_CODE             VARCHAR2(255 BYTE),
  PRODUCT_HIERARCHY_LVL1   VARCHAR2(255 BYTE),
  PRODUCT_HIERARCHY_LVL2   VARCHAR2(255 BYTE),
  PRODUCT_HIERARCHY_LVL3   VARCHAR2(255 BYTE),
  PRODUCT_HIERARCHY_LVL4   VARCHAR2(255 BYTE),
  PRODUCT_HIERARCHY_LVL5   VARCHAR2(255 BYTE),
  PRODUCT_HIERARCHY_LVL6   VARCHAR2(255 BYTE),
  PRODUCT_HIERARCHY_LVL7   VARCHAR2(255 BYTE),
  PRODUCT_HIERARCHY_LVL8   VARCHAR2(255 BYTE),
  PRODUCT_HIERARCHY_LVL9   VARCHAR2(255 BYTE),
  PRODUCT_HIERARCHY_LVL10  VARCHAR2(255 BYTE),
  PRODUCT_HIERARCHY_LVL11  VARCHAR2(255 BYTE),
  PRODUCT_HIERARCHY_LVL12  VARCHAR2(255 BYTE)
);

CREATE SEQUENCE rwh_product_seq;
CREATE UNIQUE INDEX RWH_PRODUCT_DIM_uindex ON RWH_PRODUCT_DIM (PRODUCT_CODE);

CREATE TABLE WAREHOUSE.RWH_PORTFOLIO_DIM
(
  PORTFOLIO_DIM_WID  NUMBER,
  PORTFOLIO_NAME     VARCHAR2(1000 BYTE)
);

CREATE SEQUENCE rwh_portfolio_seq;
CREATE UNIQUE INDEX RWH_PORTFOLIO_DIM_uindex ON RWH_PORTFOLIO_DIM (PORTFOLIO_NAME);

CREATE TABLE WAREHOUSE.RWH_PARTY_DIM
(
  PARTY_DIM_WID  NUMBER,
  PARTY_NAME     VARCHAR2(1000 BYTE)
);

CREATE SEQUENCE rwh_party_seq;
CREATE UNIQUE INDEX RWH_PARTY_DIM_uindex ON RWH_PARTY_DIM (PARTY_NAME);

CREATE TABLE WAREHOUSE.RWH_ORG_DIM
(
  ORG_DIM_WID               INTEGER,
  ORG_LEVEL                 INTEGER,
  ORG_NAME                  VARCHAR2(255 BYTE),
  ORG_HIERARCHY_LVL1        VARCHAR2(255 BYTE),
  ORG_HIERARCHY_LVL2        VARCHAR2(255 BYTE),
  ORG_HIERARCHY_LVL3        VARCHAR2(255 BYTE),
  ORG_HIERARCHY_LVL4        VARCHAR2(255 BYTE),
  ORG_HIERARCHY_LVL5        VARCHAR2(255 BYTE),
  ORG_HIERARCHY_LVL6        VARCHAR2(255 BYTE),
  ORG_HIERARCHY_LVL7        VARCHAR2(255 BYTE),
  ORG_HIERARCHY_LVL8        VARCHAR2(255 BYTE),
  ORG_HIERARCHY_LVL9        VARCHAR2(255 BYTE),
  ORG_HIERARCHY_LVL10       VARCHAR2(255 BYTE),
  ORG_HIERARCHY_LVL11       VARCHAR2(255 BYTE),
  ORG_HIERARCHY_LVL12       VARCHAR2(255 BYTE),
  CURR_ORG_HIERARCHY_LVL1   VARCHAR2(255 BYTE),
  CURR_ORG_HIERARCHY_LVL2   VARCHAR2(255 BYTE),
  CURR_ORG_HIERARCHY_LVL3   VARCHAR2(255 BYTE),
  CURR_ORG_HIERARCHY_LVL4   VARCHAR2(255 BYTE),
  CURR_ORG_HIERARCHY_LVL5   VARCHAR2(255 BYTE),
  CURR_ORG_HIERARCHY_LVL6   VARCHAR2(255 BYTE),
  CURR_ORG_HIERARCHY_LVL7   VARCHAR2(255 BYTE),
  CURR_ORG_HIERARCHY_LVL8   VARCHAR2(255 BYTE),
  CURR_ORG_HIERARCHY_LVL9   VARCHAR2(255 BYTE),
  CURR_ORG_HIERARCHY_LVL10  VARCHAR2(255 BYTE),
  CURR_ORG_HIERARCHY_LVL11  VARCHAR2(255 BYTE),
  CURR_ORG_HIERARCHY_LVL12  VARCHAR2(255 BYTE),
  VALID_FROM_DATE           DATE,
  VALID_TO_DATE             DATE,
  CURRENT_FLAG              CHAR(1 BYTE),
  ETL_BATCH_FILE_ID         INTEGER,
  PARIS_ID                  NUMBER
);

CREATE SEQUENCE rwh_org_seq;
CREATE UNIQUE INDEX RWH_ORG_DIM_uindex ON RWH_ORG_DIM (PARTY_NAME);

CREATE TABLE WAREHOUSE.RWH_FACTOR_DIM
(
  FACTOR_DIM_WID      NUMBER,
  BUCKET_CODE         VARCHAR2(4000 BYTE),
  FACTOR_NAME         VARCHAR2(4000 BYTE),
  BUCKET1_NAME        VARCHAR2(4000 BYTE),
  BUCKET1_DESC        VARCHAR2(4000 BYTE),
  BUCKET1_SORTORDER   NUMBER,
  BUCKET2_NAME        VARCHAR2(4000 BYTE),
  BUCKET2_DESC        VARCHAR2(4000 BYTE),
  BUCKET2_SORTORDER   NUMBER,
  BUCKET3_NAME        VARCHAR2(4000 BYTE),
  BUCKET3_DESC        VARCHAR2(4000 BYTE),
  BUCKET3_SORTORDER   NUMBER,
  BUCKET4_NAME        VARCHAR2(4000 BYTE),
  BUCKET4_DESC        VARCHAR2(4000 BYTE),
  BUCKET4_SORTORDER   NUMBER,
  BUCKET5_NAME        VARCHAR2(4000 BYTE),
  BUCKET5_DESC        VARCHAR2(4000 BYTE),
  BUCKET5_SORTORDER   NUMBER,
  BUCKET6_NAME        VARCHAR2(4000 BYTE),
  BUCKET6_DESC        VARCHAR2(4000 BYTE),
  BUCKET6_SORTORDER   NUMBER,
  BUCKET7_NAME        VARCHAR2(4000 BYTE),
  BUCKET7_DESC        VARCHAR2(4000 BYTE),
  BUCKET7_SORTORDER   NUMBER,
  BUCKET8_NAME        VARCHAR2(4000 BYTE),
  BUCKET8_DESC        VARCHAR2(4000 BYTE),
  BUCKET8_SORTORDER   NUMBER,
  BUCKET9_NAME        VARCHAR2(4000 BYTE),
  BUCKET9_DESC        VARCHAR2(4000 BYTE),
  BUCKET9_SORTORDER   NUMBER,
  BUCKET10_NAME       VARCHAR2(4000 BYTE),
  BUCKET10_DESC       VARCHAR2(4000 BYTE),
  BUCKET10_SORTORDER  NUMBER
);

CREATE TABLE WAREHOUSE.RWH_DEAL_DIM
(
  DEAL_DIM_WID               NUMBER,
  SOURCENAME                 VARCHAR2(1000 BYTE),
  DEAL_TYPE                  VARCHAR2(1000 BYTE),
  DEAL_ID                    VARCHAR2(1000 BYTE),
  DEAL_VERSION               NUMBER,
  DEAL_WHATIF_UID            VARCHAR2(1000 BYTE),
  COMPONENT_CODE             VARCHAR2(1000 BYTE),
  DEAL_DATE                  DATE,
  DEAL_AMENDED               DATE,
  DEAL_STATE                 VARCHAR2(1000 BYTE),
  DEAL_STRUCTURE_CD          VARCHAR2(1000 BYTE),
  DEAL_ADDITIONAL_INFO       CLOB,
  MATURITY_DATE              DATE,
  DEAL_ADDITIONAL_INFO_CUBE  VARCHAR2(4000 BYTE),
  DEALINCUBEGENERATION       NUMBER
);

CREATE SEQUENCE rwh_deal_seq;
CREATE UNIQUE INDEX RWH_DEAL_DIM_uindex ON RWH_DEAL_DIM (SOURCENAME, DEAL_TYPE, DEAL_ID, DEAL_VERSION, DEAL_WHATIF_UID, COMPONENT_CODE);

CREATE TABLE WAREHOUSE.RWH_DATE_DIM
(
  DATE_DIM_WID       NUMBER,
  BUSINESS_DATE      TIMESTAMP(6),
  DAYOFWEEK          VARCHAR2(9 BYTE),
  SHORTDAYOFWEEK     CHAR(3 BYTE),
  YR                 NUMBER(4),
  MONTHNAME          VARCHAR2(9 BYTE),
  MONTHNAMESHORT     CHAR(3 BYTE),
  MONTHNUM           NUMBER(2),
  DAYOFMONTH         NUMBER(2),
  DAYOFYEAR          NUMBER(3),
  WEEKNUMBER         NUMBER(2),
  PREV_DATE_DIM_WID  NUMBER
);

CREATE UNIQUE INDEX RWH_DATE_DIM_uindex ON RWH_DATE_DIM (BUSINESS_DATE);

CREATE TABLE WAREHOUSE.RWH_CURVETYPE_DIM
(
  CURVETYPE_DIM_WID  NUMBER,
  CURVE_TYPE_CODE    VARCHAR2(1000 BYTE),
  CURVE_TYPE_DESC    VARCHAR2(1000 BYTE),
  PRICING_MODEL      VARCHAR2(1000 BYTE)
);

CREATE SEQUENCE rwh_curve_type_seq;
CREATE UNIQUE INDEX RWH_CURVETYPE_DIM_uindex ON RWH_CURVETYPE_DIM (CURVE_TYPE_CODE, PRICING_MODEL);

CREATE TABLE WAREHOUSE.RWH_CURRENCY_DIM
(
  CURRENCY_DIM_WID     NUMBER,
  BASE_CURRENCY        VARCHAR2(1000 BYTE),
  BASE_CURRENCY_DESC   VARCHAR2(4000 BYTE),
  RISK_CURRENCY        VARCHAR2(1000 BYTE),
  RISK_CURRENCY_DESC   VARCHAR2(4000 BYTE),
  LOCAL_CURRENCY       VARCHAR2(1000 BYTE),
  LOCAL_CURRENCY_DESC  VARCHAR2(4000 BYTE)
);

CREATE SEQUENCE rwh_currency_seq;
CREATE UNIQUE INDEX RWH_CURRENCY_DIM_uindex ON RWH_CURRENCY_DIM (BASE_CURRENCY, RISK_CURRENCY, LOCAL_CURRENCY);

CREATE SEQUENCE RWHSTG_ETL_BATCH_seq;
CREATE TABLE WAREHOUSE.RWHSTG_ETL_BATCH
(
  BATCH_ID                      NUMBER,--not used
  FILE_ID                       NUMBER,--file id
  SOURCE_SYSTEM                 VARCHAR2(255 BYTE),--not used
  FILE_NAME                     VARCHAR2(255 BYTE),
  FILE_TYPE				  VARCHAR2(255 BYTE), --substring of file name  substr(fineName,1,7)  example value:  CSRISKFEED  ILI CSNOTIF
  FILE_CREATED_TIMESTAMP DATE,--kada je file kreiran u direcotry
  BUSINESS_DATE DATE, --    businessDate  (data files only)
  NUMBER_OF_ROWS NUMBER,--numberOfRows (data files only)
  USERNAME_REQUESTED   VARCHAR2(255 BYTE),--fileUserName (data files only)
  LOCATION   VARCHAR2(255 BYTE), --location (data files only)
  QL_VERSION VARCHAR2(255 BYTE),--qlVersion   (data files only)
  START_TIMESTAMP               DATE,--etl start timestamp  (record creation time of this record in batch table by dispatcher)
  CASPAR_GUID                   VARCHAR2(255 BYTE),--guid
  RERUN_GUID                    VARCHAR2(255 BYTE),--guid
  FILE_VALIDATION_FLAG          CHAR(1 BYTE),--S F  same as STAGING_DATA_LOAD_FLAG
  STAGING_DATA_LOAD_FLAG        CHAR(1 BYTE),--same as above
  DIMENSION_SWEEP_STATUS        NUMBER,--not used
  FACT_LOAD_FLAG                CHAR(1 BYTE), --same as STAGING_DATA_LOAD_FLAG     set to P (or R) if purged and rerun
  END_TIMESTAMP                 DATE,--when we have finished writing to fact table
  ERROR_DESCRIPTION             VARCHAR2(4000 BYTE),--
  ALERT_RAISED_TIMESTAMP        DATE,--not used or populated
  BUNDLE_ID                     NUMBER,--not used
  UNIQUE_INTRADAY_REQUEST_NAME  VARCHAR2(255 BYTE),
  PARIS_REQUEST_ID              VARCHAR2(255 BYTE),--not used
  RERUN_VERSION                 VARCHAR2(255 BYTE),--not used
  ANAL_REQUEST_GUID             VARCHAR2(255 BYTE),--not used
  EOD_FLAG                      VARCHAR2(255 BYTE),--EOD SOD coming from data file
  REQUEST_TIMESTAMP             VARCHAR2(255 BYTE),--not used
  RUN_TAG                       VARCHAR2(255 BYTE),--london official
  PENDING_SWEEP_STATUS          NUMBER,--not used
  PORTFOLIO                     VARCHAR2(1000 BYTE),--portfolio from data file
  VARIANCE_BATCH_ID             NUMBER,--not used
  VARIANCE_FACT_LOAD_FLAG       VARCHAR2(1 BYTE)--not used
);

CREATE SEQUENCE rwh_request_notification_seq;
CREATE UNIQUE INDEX request_notification_uindex ON REQUESTNOTIFICATION (UNIQUENAME, ANALYTICSREQUESTGUID);

CREATE TABLE WAREHOUSE.REQUESTNOTIFICATION--this is control table and dimension at the same time
(
  NOTIFICATIONID                  INTEGER       NOT NULL, --key
  SOURCE                          VARCHAR2(30 BYTE) NOT NULL,--
  UNIQUENAME                      VARCHAR2(100 BYTE),--guid
  ANALYTICSREQUESTGUID            VARCHAR2(100 BYTE),--guid
  NUMBEROFTASKS                   NUMBER,--not used   
  NUMBEROFFILES                   NUMBER,--data files  must match sum(LOADINCRFILES)  --cube upload condition
  NUMBEROFPERSISTEDTASKS          NUMBER,-- not used ------from notif file   
  NUMBEROFPERSISTEDTRADES         NUMBER,--trades in the file
  NUMBEROFPERSISTEDVALIDVALUES    NUMBER,--records in a file msut match sum(LOADINCRROWS) --cube upload condition
  NUMBEROFPERSISTEDINVALIDVALUES  NUMBER,--not used from notif
  NUMBEROFNOTPERSISTEDTASKS       NUMBER,--not used from notif
  NUMBEROFNOTPERSISTEDTRADES      NUMBER,--not used from notif
  RUNTAG                          VARCHAR2(100 BYTE),--from data file    
  BUSINESSDATE                    DATE,
  BATCHTYPE                       VARCHAR2(1000 BYTE),
  REQUESTNAME                     VARCHAR2(200 BYTE),--uniq intraday req name
  USERNAMEREQUESTED               VARCHAR2(100 BYTE),
  PORTFOLIO                       VARCHAR2(255 BYTE),
  RERUNGUID                       VARCHAR2(100 BYTE),
  RERUNVERSION                    VARCHAR2(20 BYTE),
  PERSISTSTATUS                   CHAR(1 BYTE),--fact table persist flage Y complete A abandoned P purged R replaced by rerun E complete with dta eror F failed   n incomplete e uncomplete with data erors f uncoplete failed files
  --this is run complete flag replpacement, allows cube upload (cuberequest table)
  PERSISTSTARTTIMESTAMP           TIMESTAMP(6),--creation time of the notification record (this record)
  PERSISTENDTIMESTAMP             TIMESTAMP(6),-- last time of update of PERSISTSTATUS to Y E F only
  REPORTFLAG                      CHAR(1 BYTE),--not used
  AGGFLAG                         CHAR(1 BYTE),--not used
  VARFLAG                         CHAR(1 BYTE),--not used
  LOADINCRFILES                   NUMBER,--how many files loaded so far     not used for adjustments and variance
  LOADINCRROWS                    NUMBER,--same as above for rows....    not used for adjustments and variance
  PORTFOLIODIMWID                 INTEGER,-- portfoio dim wid
  NOTUSED1                        INTEGER
);

create sequence s_batch_id  start with 1 maxvalue 999999999999999999999999999 minvalue 1 nocycle cache 20 noorder;
create sequence s_notification_id start with 1 maxvalue 999999999999999999999999999 minvalue 1 nocycle cache 20 noorder;

CREATE OR REPLACE procedure warehouse.notifFeed(casparGuid varchar2, runGuid varchar2,taskCnt integer,fileCnt integer,
persistedTaskCnt integer,persistedTradesCnt integer,persistedValidValuesCnt integer,persistedInvalidValuesCnt integer,notPersistedTaskCnt integer,notPersistedTradesCnt integer,
fileName varchar2,etlExceptionString varchar2,requestTimestamp varchar2,etlStartTimestamp varchar2,filePersistedToDiskTimestamp timestamp,source varchar2,fileType varchar2)
is
notifiId integer;
fileId integer;
begin
      begin
          insert into requestNotification (
          notificationId, source, uniqueName, analyticsRequestGuid,
          numberOfTasks, numberOfFiles, numberOfPersistedTasks, numberOfPersistedTrades, numberOfPersistedValidValues, 
          numberOfPersistedInvalidValues, numberOfNotPersistedTasks, numberOfNotPersistedTrades,
          persistStatus, persistStartTimestamp, persistEndtimestamp,
          loadIncrFiles, loadIncrRows) values (s_notification_id.nextval,notifFeed.source||':'||notifFeed.fileType, notifFeed.casparGuid, notifFeed.casparGuid,
           notifFeed.taskCnt, notifFeed.fileCnt, notifFeed.persistedTaskCnt, notifFeed.persistedTradesCnt, notifFeed.persistedValidValuesCnt, 
           notifFeed.persistedInvalidValuesCnt, notifFeed.notPersistedTaskCnt, notifFeed.notPersistedTradesCnt,
           case when notifFeed.fileCnt=0 then 'Y' else 'n' end, systimestamp, case when notifFeed.fileCnt=0 then systimestamp else null end,
           0, 0) returning notificationId into notifiId;
      exception
        when dup_val_on_index then 
            begin
                  select notificationId into notifiId from requestNotification where uniqueName= notifFeed.casparGuid;
            exception 
                when no_data_found then    
                          --p_log('Could not insert notification (duplication) and could not find it. INTERNAL ERROR. Processing: '||fileName, 'ERROR'); -- to get full error stack of the exact error.
                          raise_application_error(-20000, 'Could not insert notification (duplication) and could not find it. INTERNAL ERROR. Processing: [filename]:'||notifFeed.fileName||'  [guid]:'||notifFeed.casparGuid);                    
            end;            
             update requestNotification 
             set persistStatus = decode(loadIncrRows,numberofpersistedvalidvalues, 'Y', persistStatus),
              persistEndTimestamp = case when decode(loadIncrRows,numberofpersistedvalidvalues, 'Y', persistStatus) in ('Y', 'E') then systimestamp else persistEndTimestamp end,
              numberOfTasks = notifFeed.taskCnt,
              numberOfFiles = notifFeed.fileCnt,
              numberOfPersistedTasks = notifFeed.persistedTaskCnt,
              numberOfPersistedTrades = notifFeed.persistedTradesCnt,
              numberOfPersistedValidValues = notifFeed.persistedValidValuesCnt,
              numberOfPersistedInvalidValues = notifFeed.persistedInvalidValuesCnt,
              numberOfNotPersistedTasks = notifFeed.notPersistedTaskCnt,
              numberOfNotPersistedTrades = notifFeed.notPersistedTradesCnt
              where notificationId = notifiId;             
      end;
   insert into rwhstg_etl_batch(file_id, file_name, file_type, file_created_timestamp, start_timestamp,caspar_guid,
                                                       fact_load_flag, end_timestamp, error_description,request_timestamp                                                                  
                                                       )
   values (s_batch_id.nextval, notifFeed.fileName, notifFeed.fileType, to_date(to_char(notifFeed.filePersistedToDiskTimestamp,'dd-Mon-yyyy HH24:MI:SS'),'dd-Mon-yyyy HH24:MI:SS'), notifFeed.etlStartTimestamp,notifFeed.casparGuid,
                                                       null ,systimestamp,notifFeed.etlExceptionString,notifFeed.requestTimestamp
                                            )        
 commit;                   
end;
/

CREATE OR REPLACE procedure warehouse.dataFeed(uniqueName varchar2, analyticsRequestGuid varchar2, runTag varchar2, businessDate date,
rerunGuid varchar2, requestName varchar2, batchType varchar2,portfolio varchar2, portfolioDimSK integer,numberOfRows integer,filePersistedToDiskTimestamp timestamp,
source varchar2, intradayName varchar2, casparGuid varchar2, fileType varchar2,
fileName varchar2,etlStartTimestamp varchar2, fileUserName varchar2,location varchar2, qlVersion varchar2,fileValid varchar2, /* if number of lines equals numberOfRows from footer than S else F */
etlExceptionString varchar2/* java processing exception string */,EODflag varchar2, requestTimestamp varchar2,rerunVersion varchar2) 
is
notifiId integer;
fileId integer;
begin
      begin
           insert into requestNotification (
            notificationId,source,uniqueName,analyticsRequestGuid,usernamerequested,
            runTag, businessDate, rerunGuid, requestName, batchType,
            portfolio, portfolioDimWid,rerunVersion,
            persistStatus, persistStartTimestamp, persistEndtimestamp,
            loadIncrFiles, loadIncrRows)
         values (
             s_notification_id.nextval, dataFeed.source||':'||dataFeed.fileType, dataFeed.uniqueName, dataFeed.analyticsRequestGuid,dataFeed.fileUserName,
             dataFeed.runTag, dataFeed.businessDate, dataFeed.rerunGuid, dataFeed.intradayName, dataFeed.batchType,
             dataFeed.portfolio, dataFeed.portfolioDimSK,dataFeed.rerunVersion,
             'n', dataFeed.filePersistedToDiskTimestamp, systimestamp,
             1, dataFeed.numberOfRows)
         returning notificationId into notifiId;
      exception
        when dup_val_on_index then 
            begin
                  select notificationId into notifiId from requestNotification where uniqueName= dataFeed.uniqueName;
            exception 
                when no_data_found then    
                          --p_log('Could not insert notification (duplication) and could not find it. INTERNAL ERROR. Processing: '||fileName, 'ERROR'); -- to get full error stack of the exact error.
                          raise_application_error(-20000, 'Could not insert notification (duplication) and could not find it. INTERNAL ERROR. Processing: [filename]:'||dataFeed.fileName||'  [guid]:'||dataFeed.uniqueName);                    
            end;
              update requestNotification set           
              loadIncrFiles = loadIncrFiles+1,
              loadIncrRows = loadIncrRows+dataFeed.numberOfRows,
              persistStatus = decode((loadIncrRows+dataFeed.numberOfRows), numberofpersistedvalidvalues, 'Y', persistStatus),
              persistEndTimestamp = systimestamp,
              usernamerequested = dataFeed.fileUserName,
              runTag = dataFeed.runTag,
              businessDate = dataFeed.businessDate, 
              rerunGuid = dataFeed.rerunGuid, 
              requestName = dataFeed.intradayName, 
              batchType = dataFeed.batchType,
              portfolio = dataFeed.portfolio,
              portfolioDimWid = dataFeed.portfolioDimSK,
              rerunVersion = dataFeed.rerunVersion
              where notificationId = notifiId;        
      end;
   insert into rwhstg_etl_batch(file_id, file_name, file_type, file_created_timestamp, start_timestamp,
                                                       business_date,number_of_rows,username_requested,location,ql_version,caspar_guid,rerun_guid,
                                                       file_validation_flag, fact_load_flag, end_timestamp, error_description, eod_flag  ,request_timestamp,run_tag)
                                            values (s_batch_id.nextval, dataFeed.fileName, dataFeed.fileType, to_date(to_char(dataFeed.filePersistedToDiskTimestamp,'dd-Mon-yyyy HH24:MI:SS'),'dd-Mon-yyyy HH24:MI:SS'), dataFeed.etlStartTimestamp,
                                                        dataFeed.businessDate,dataFeed.numberOfRows,dataFeed.fileUserName,dataFeed.location,dataFeed.qlVersion,dataFeed.uniqueName,dataFeed.rerunGuid,
                                                        dataFeed.fileValid,null /* not known at this point, need be set by external table routine */,systimestamp,dataFeed.etlExceptionString,dataFeed.EODflag,dataFeed.requestTimestamp,dataFeed.runTag);
 commit;                        
end;
/