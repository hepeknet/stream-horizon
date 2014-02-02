- JDK 1.7 is required
- in most cases only thing to be changed should be $BAUK_HOME/config/feedConfig.xml (where mappings are defined), no need to change anything else

- startup scripts can be found in $BAUK_HOME/bin/ folder. JVM tuning and turning some experimental options can be done in startup scripts
- configuration files for logging, caching and default feed config file can be found in $BAUK_HOME/config/ folder
- $BAUK_HOME/config/feedConfig.xml is the default feed config file
- startup scripts can be used to point to different feed config file
- any additional classpath dependencies should be added to $BAUK_HOME/extras/
- all log files will be created in $BAUK_HOME/log/ folder
- JMX metrics are exposed by default under metrics subtree (use JVisualVM or JConsole)
- do not delete or create anything in $BAUK_HOME/data/ folder - this folder is used for housekeeping

HTTP commands

1) Pausing processing, flushing dimension cache and continuing processing

http://localhost:21000/flushDimensionCache/?dimension=DIMENSION_NAME_AS_DEFINED_IN_CONFIG

http://localhost:21000/flushDimensionCache/?dimension=PORTFOLIO_DIMENSION


2) Accessing monitoring page

http://localhost:21000/monitor/