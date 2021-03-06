
DIRNAME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
RESOLVED_HOME=$DIRNAME/../

# first parameter - instance identifier
# second parameter - heap size GB
# third parameter - additional JVM parameters (added as standard JVM parameters -Dk=v) 

BAUK_INSTANCE_ID="0"

if [ -n "$1" ]; then BAUK_INSTANCE_ID="$1"; fi


heapSizeGb="4"

if [ -n "$2" ]; then heapSizeGb="$2"; fi

additionalJVMProperties=""
if [ -n "$3" ]; then additionalJVMProperties="$3"; fi

# where config file is, if not specified then default one will be used in $RESOLVED_HOME/config/feedConfig.xml
BAUK_CONFIG_FILE_LOCATION="$RESOLVED_HOME/config/engine-config.xml"
# if needed increase the size of heap to be used
HEAP_OPTS="-Xmx${heapSizeGb}G -Xms${heapSizeGb}G"


#GC_OPTS="-XX:+UseG1GC -XX:MaxGCPauseMillis=200 "
GC_OPTS="-XX:+UseConcMarkSweepGC -XX:+CMSScavengeBeforeRemark -XX:+CMSParallelRemarkEnabled -XX:ParallelGCThreads=2 "
#GC_OPTS="$GC_OPTS -XX:TLABSize=5M -XX:-ResizeTLAB"
#GC_OPTS="$GC_OPTS -verbose:gc "
JAVA_OPTS="-server -d64 -XX:+UseCompressedOops -XX:+AggressiveOpts -XX:+UseStringCache -XX:+OptimizeStringConcat -XX:+UseBiasedLocking -XX:+UseFastAccessorMethods -XX:+UseFastEmptyMethods -XX:+TieredCompilation -XX:+DisableExplicitGC"
JAVA_OPTS="$JAVA_OPTS -Dsun.rmi.dgc.server.gcInterval=3600000 -Dsun.rmi.dgc.client.gcInterval=3600000 -Djava.net.preferIPv4Stack=true"
JAVA_OPTS="$JAVA_OPTS -DBAUK_INSTANCE_ID=$BAUK_INSTANCE_ID"

JAVA_OPTS="$GC_OPTS $JAVA_OPTS"
JAVA_OPTS="$JAVA_OPTS $additionalJVMProperties "

# try setting these to half of total heap size
JAVA_OPTS="$JAVA_OPTS -XX:NewSize=2G -XX:MaxNewSize=2G"

#JAVA_OPTS="$JAVA_OPTS -XX:+UnlockCommercialFeatures -XX:+FlightRecorder"
#JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"
#JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.port=8765 -Djava.rmi.server.hostname=127.0.0.1"

# set bauk.config property to point to valid configuration file on file system
JAVA_OPTS="$JAVA_OPTS -Dbauk.home=$RESOLVED_HOME -Dbauk.config=${BAUK_CONFIG_FILE_LOCATION}"

CONFIG_OPTIONS="-Dlogback.configurationFile=$RESOLVED_HOME/config/logback.xml -Dhazelcast.config=$RESOLVED_HOME/config/sh-hazelcast-config.xml"

JAVA_CP="$RESOLVED_HOME/lib/*:$RESOLVED_HOME/ext-lib/*:"

echo
echo "classpath $JAVA_CP"
echo "heap $HEAP_OPTS"
echo "JVM options $JAVA_OPTS"
echo

FINAL_JAVA_COMMAND="-classpath $JAVA_CP $HEAP_OPTS $JAVA_OPTS $CONFIG_OPTIONS com.threeglav.sh.bauk.main.StreamHorizonEngine"
echo "java $FINAL_JAVA_COMMAND"

java $FINAL_JAVA_COMMAND