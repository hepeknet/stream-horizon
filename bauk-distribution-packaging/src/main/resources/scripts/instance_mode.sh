# copy this file in case you want to run multiple instances of engine processing files from same source folder (<sourceDirectory> tag in engine-config.xml file)
# ensure you assign unique integer identifier to every instance

# must be non-negative integer (0-number_of_instances-1)
engineInstanceId="0"

#should be set only when multiple instances are processing files from the same feed source folder (<sourceDirectory> tag in engine-config.xml file)
totalInstancesCount=""

heapSizeGb="8"
etlProcessingThreadCount="10"
databaseProcessingThreadCount="10"
readBufferSizeMb="20"
writeBufferSizeMb="20"
jdbcLoadBatchSize="1000"

./start.sh $engineInstanceId $heapSizeGb "-DetlProcessingThreadCount=$etlProcessingThreadCount -DdatabaseProcessingThreadCount=$databaseProcessingThreadCount -Dread.buffer.size.mb=$readBufferSizeMb -Dwrite.buffer.size.mb=$writeBufferSizeMb -Djdbc.bulk.loading.batch.size=$jdbcLoadBatchSize -Dmulti.instance.total.partition.count=$totalInstancesCount" 