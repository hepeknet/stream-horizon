
DIRNAME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
RESOLVED_HOME=$DIRNAME/../

# following two lines are the only two lines that need to be changed
# where config file is, if not specified then default one will be used in $RESOLVED_HOME/config/feedConfig.xml
BAUK_CONFIG_FILE_LOCATION="$RESOLVED_HOME/config/feedConfig.xml"
# if needed increase the size of heap to be used
HEAP_OPTS="-Xmx8G -Xms2G"


GC_OPTS=""
JAVA_OPTS="-server -XX:+UseCompressedOops -XX:+AggressiveOpts -XX:+UseStringCache -XX:+OptimizeStringConcat -XX:+UseBiasedLocking -XX:+UseFastAccessorMethods -XX:+UseFastEmptyMethods -XX:+TieredCompilation -XX:+DisableExplicitGC"
JAVA_OPTS="$JAVA_OPTS -Dsun.rmi.dgc.server.gcInterval=3600000 -Dsun.rmi.dgc.client.gcInterval=3600000 -Djava.net.preferIPv4Stack=true"
#JAVA_OPTS="$JAVA_OPTS -XX:+UnlockCommercialFeatures -XX:+FlightRecorder"
#JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"
#JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.port=8765 -Djava.rmi.server.hostname=127.0.0.1"

# set this to true for test mode
JAVA_OPTS="$JAVA_OPTS -Dbauk.test.mode=false"

# set bauk.config property to point to valid configuration file on file system
JAVA_OPTS="$JAVA_OPTS -Dbauk.home=$RESOLVED_HOME -Dbauk.config=${BAUK_CONFIG_FILE_LOCATION}"

CONFIG_OPTIONS="-Dlogback.configurationFile=$RESOLVED_HOME/config/logback.xml -Dhazelcast.config=$RESOLVED_HOME/config/bauk-hazelcast-config.xml"

JAVA_CP="$RESOLVED_HOME/lib/*:$RESOLVED_HOME/extras/*:"

echo
echo "classpath $JAVA_CP"
echo "heap $HEAP_OPTS"
echo "JVM options $JAVA_OPTS"
echo

FINAL_JAVA_COMMAND="-classpath $JAVA_CP $HEAP_OPTS $JAVA_OPTS $CONFIG_OPTIONS com.threeglav.bauk.main.BaukApplication > bauk.out"
echo "java $FINAL_JAVA_COMMAND"

java $FINAL_JAVA_COMMAND