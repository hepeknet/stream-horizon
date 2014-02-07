@echo off

rem first parameter - instance identifier
rem second parameter - heap size GB

set DIRNAME=.\

pushd %DIRNAME%..
set "RESOLVED_HOME=%CD%"
popd

set "BAUK_INSTANCE_ID=%1"
IF "%BAUK_INSTANCE_ID%"=="" set "BAUK_INSTANCE_ID=0"

set "heapSizeGb=%2"
if "%heapSizeGb%"=="" set "heapSizeGb=2"

rem following two lines are the only two lines that need to be changed
rem where config file is, if not specified then default one will be used
set "BAUK_CONFIG_FILE_LOCATION=%RESOLVED_HOME%\config\feedConfig.xml"

rem if needed increase the size of heap to be used
set "HEAP_OPTS=-Xmx%heapSizeGb%G -Xms%heapSizeGb%G"

set "GC_OPTS="
set "JAVA_OPTS=-server -d64 -XX:+UseConcMarkSweepGC -XX:+CMSScavengeBeforeRemark -XX:+UseCompressedOops -XX:+AggressiveOpts -XX:+UseStringCache -XX:+OptimizeStringConcat -XX:+UseBiasedLocking -XX:+UseFastAccessorMethods -XX:+UseFastEmptyMethods -XX:+TieredCompilation -XX:+DisableExplicitGC"
set "JAVA_OPTS=%JAVA_OPTS% -Dsun.rmi.dgc.server.gcInterval=3600000 -Dsun.rmi.dgc.client.gcInterval=3600000 -Djava.net.preferIPv4Stack=true"
set "JAVA_OPTS=%JAVA_OPTS% -DBAUK_INSTANCE_ID=%BAUK_INSTANCE_ID%"

rem set "JAVA_OPTS=%JAVA_OPTS% -XX:NewSize=1G -XX:MaxNewSize=2G"

rem set "JAVA_OPTS=%JAVA_OPTS% -XX:+UnlockCommercialFeatures -XX:+FlightRecorder"
rem set "JAVA_OPTS=%JAVA_OPTS% -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"
rem set "JAVA_OPTS=%JAVA_OPTS% -Dcom.sun.management.jmxremote.port=8765 -Djava.rmi.server.hostname=127.0.0.1"

rem set bauk.config property to point to valid configuration file on file system
set "JAVA_OPTS=%JAVA_OPTS% -Dbauk.home=%RESOLVED_HOME% -Dbauk.config=%BAUK_CONFIG_FILE_LOCATION%"

set "CONFIG_OPTIONS=-Dlogback.configurationFile=%RESOLVED_HOME%\config\logback.xml -Dhazelcast.config=%RESOLVED_HOME%\config\bauk-hazelcast-config.xml"

set JAVA_CP="%RESOLVED_HOME%\lib\*;%RESOLVED_HOME%\extras\*;"

echo.
echo classpath %JAVA_CP%
echo heap %HEAP_OPTS%
echo JVM options %JAVA_OPTS%
echo.

set "FINAL_JAVA_COMMAND=-classpath %JAVA_CP% %HEAP_OPTS% %JAVA_OPTS% %CONFIG_OPTIONS% com.threeglav.bauk.main.BaukApplication"
echo java %FINAL_JAVA_COMMAND%

java %FINAL_JAVA_COMMAND%