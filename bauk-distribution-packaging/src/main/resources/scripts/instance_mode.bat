rem copy this file in case you want to run multiple instances of engine processing files from same source folder
rem ensure you assign unique integer identifier to every instance

rem must be non-negative integer (0-number_of_instances-1)
set "baukInstanceId=0"

rem should be set only when multiple instances are processing files from the same feed source folder (<sourceDirectory> tag in engine-config.xml file)
set "totalInstancesCount="

set "heapSizeGb=8"
set "etlProcessingThreadCount=10"
set "databaseProcessingThreadCount=10"
set "readBufferSizeMb=20"
set "writeBufferSizeMb=20"
set "jdbcLoadBatchSize=1000"

start.bat %baukInstanceId% %heapSizeGb% "-DfeedProcessingThreads=%feedProcessingThreads% -DbulkLoadProcessingThreads=%bulkLoadProcessingThreads% -Dread.buffer.size.mb=%readBufferSizeMb% -Dwrite.buffer.size.mb=%writeBufferSizeMb% -Djdbc.bulk.loading.batch.size=%jdbcLoadBatchSize% -Dmulti.instance.total.partition.count=%totalInstancesCount%" 