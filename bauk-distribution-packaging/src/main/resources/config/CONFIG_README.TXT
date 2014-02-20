SH config files:

1) logback.xml - for tuning logging levels. Set to DEBUG and restart engine to get more information in logs. It is also possible to change logging using JMX - no need to restart JVM in this case.
2) feedConfig.xml - default feed configuration file should be put in this folder and engine will automatically start using it on startup
3) sh-hazelcast-config.xml - if hazelcast is used for caching then any additional tuning can be done here (check hazelcast documentation for more details). By default infinispan is used for caching
4) sh-infinispan-config.xml - if Infinispan is used for caching then any additional Infinispan tuning can be done here (check Infinispan documentation for more details).