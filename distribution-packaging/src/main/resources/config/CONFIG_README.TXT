Bauk config files:

1) logback.xml - for tuning logging levels. Set to DEBUG and restart engine to get more information in logs. It is also possible to change logging using JMX - no need to restart JVM in this case.
2) feedConfig.xml - default feed configuration file
3) bauk-hazelcast-config.xml - if hazelcast is used for caching then additional tuning can be done here (check hazelcast documentation for more details). By default infinispan is used for caching
4) bauk_config.xsd is XSD schema for configuration file and all Bauk config files (feedConfig.xml) must comply to this schema