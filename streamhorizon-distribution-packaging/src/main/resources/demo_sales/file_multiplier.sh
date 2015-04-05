#!/bin/bash
cd "$(dirname "$0")"

for i in {0..499}
do
   cp sales_20140107_data.data  sales_20140107_data_$i.csv
done
