By default only WARN and ERROR logs are shown. This can be changed in $ENGINE_HOME/config/logback.xml

Every engine instance will create three logs. The most important one is sh-engine-ENGINE_INSTANCE_IDENTIFIER.log

1) sh-engine-ENGINE_INSTANCE_IDENTIFIER.log (sh-engine-0.log for example) - contains all the logs
2) etlEngine-ENGINE_INSTANCE_IDENTIFIER.log - contains logs related to processing of input files
2) dbLoadEngine-ENGINE_INSTANCE_IDENTIFIER.log - contains logs related to loading of bulk files into database