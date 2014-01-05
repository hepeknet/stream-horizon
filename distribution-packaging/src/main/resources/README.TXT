- JDK 1.7 is required
- in most cases only thing to be changed should be $BAUK_HOME/config/feedConfig.xml (where mappings are defined), no need to change anything else

- startup scripts can be found in $BAUK_HOME/bin/ folder. JVM tuning and turning some experimental options can be done in startup scripts
- configuration files for logging, caching and default feed config file can be found in $BAUK_HOME/config/ folder
- $BAUK_HOME/config/feedConfig.xml is the default feed config file
- startup scripts can be used to point to different feed config file
- any additional classpath dependencies should be added to $BAUK_HOME/extras/
- all log files will be created in $BAUK_HOME/log/ folder
- JMX metrics are exposed by default under metrics subtree (use JVisualVM or JConsole)
- do not delete anything in $BAUK_HOME/data/ folder

- in case when engine is not able to process input feed file error will be logged and .fail file will be created in <errorDirectory> containing the description of error