THIS FOLDER CONTAINS DEMO FEATURE THAT CAN BE EXECUTED BY STREAMHORIZON ENGINE. THIS IS FUNCTIONAL DEMO AND CAN BE USED FOR PERFORMANCE TEST (IF SYSTEM IS TUNED PROPERLY)

1) Decide whether you want to use Oracle or MySQL database
2) Tune database properly
3) Execute appropriate create scripts (provided)
4) Copy appropriate engine-config.xml (provided)
5) Use provided input files
6) If you want to rerun demo copy input feed files back from <archiveDirectory> directory into <sourceDirectory> directory

1) Provided database scripts are for Oracle and MySQL databases, located in their respective folders.
2) StreamHorizon processing power exceeds database throughput. Please ensure that your target fact table is partitioned and sub partitioned. Ensure that StreamHorizon database threads don't compete for locks (this would occur if inserts are made in parallel into single table which isn't adequately designed). For more help on how to optimize database load please contact StreamHorizon support.
3) Execute appropriate database schema create scripts (provided) /mysql/mysql_create_schema.sql or /oracle/oracle_create_schema.sql
4) Copy appropriate engine-config.xml (provided in MySQL or Oracle folder ) into 'config' folder of your StreamHorizon instance. Make sure you provide your database server username, password and other connection settings in <connectionProperties> tag of the engine-config.xml file. Make sure you set <sourceDirectory>, <archiveDirectory>, <errorDirectory> and <bulkOutputDirectory> tags of engine-config.xml to point to desired directories.
5) Tune JDBC URL, thread count in engine-config.xml (see StreamHorizon documentation for details).
6) Use provided input file /demo_sales/sh_demo_sales_data_sample_100k.csv and execute file_multiplier.sh or file_multiplier.bat to create 1000 feed files. Move files from /demo_sales/ directory into designated <sourceDirectory>
7) Execute $ENGINE_HOME/bin/start.sh or $ENGINE_HOME/bin/start.bat to start StreamHorizon
8) First run through files will be slow (this is expected) because StreamHorizon has to populate all dimensions (initially all dimensions and fact table are empty). Every subsequent run will be fast because most of inserts are going into fact table
9) To access throughput of StreamHorizon platform please kill process by ctrl+z command (executed against console) while StreamHorizon is still processing data. StreamHorizon will print out at console average throughput. If you don't kill process while running throughput will seem low as startup time and 'kill' time of StreamHorizon engine are used to calculate throughput. 
If throughput is below 500K/second please contact StreamHorizon support (threeglav.com) as setup or hardware you are running on is most likely not adequate.
