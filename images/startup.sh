service mysql restart

mysql -pshroot -e 'create database pet_shop;'

mysql -pshroot pet_shop < /opt/streamhorizon/stream-horizon-3.3.5/demo_sales/mysql/mysql_create_schema.sql

/opt/streamhorizon/stream-horizon-3.3.5/demo_sales/file_multiplier.sh

cp /opt/streamhorizon/stream-horizon-3.3.5/demo_sales/*.csv /tmp/demo_sales/input/

echo "Created initial data set for StreamHorizon"
