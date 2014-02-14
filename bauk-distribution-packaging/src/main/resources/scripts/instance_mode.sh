# copy this file in case you want to run multiple instances of engine processing files from same source folder
# ensure you assign unique integer identifier to every instance

# must be non-negative integer (0-number_of_instances-1)
baukInstanceId="0"

#should be set only when multiple instances are processing files from the same source folder
totalInstancesCount=""

heapSizeGb="8"
feedProcessingThreads="10"
bulkLoadProcessingThreads="10"
readBufferSizeMb="20"
writeBufferSizeMb="20"
jdbcLoadBatchSize="1000"

./start.sh $baukInstanceId $heapSizeGb "-DfeedProcessingThreads=$feedProcessingThreads -DbulkLoadProcessingThreads=$bulkLoadProcessingThreads -Dread.buffer.size.mb=$readBufferSizeMb -Dwrite.buffer.size.mb=$writeBufferSizeMb -Djdbc.bulk.loading.batch.size=$jdbcLoadBatchSize -Dmulti.instance.total.partition.count=$totalInstancesCount" 