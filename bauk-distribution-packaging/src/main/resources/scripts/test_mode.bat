set "baukInstanceId=0"
set "heapSizeGb=8"

start.bat %baukInstanceId% %heapSizeGb% "-DfeedProcessingThreads=10 -DbulkLoadProcessingThreads=10 -Dread.buffer.size.mb=20 -Dwrite.buffer.size.mb=20 -Djdbc.bulk.loading.batch.size=1000" 