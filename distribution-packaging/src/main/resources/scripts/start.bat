@echo off

set DIRNAME=.\

pushd %DIRNAME%..
set "RESOLVED_HOME=%CD%"
popd

rem following two lines are the only two lines that need to be changed
rem where config file is, if not specified then default one will be used
set "BAUK_CONFIG_FILE_LOCATION=%RESOLVED_HOME%\config\feedConfig.xml"

rem if needed increase the size of heap to be used
set "HEAP_OPTS=-Xmx4G -Xms2G"

set "GC_OPTS="
set "JAVA_OPTS=-server -XX:+UseCompressedOops -XX:+AggressiveOpts -XX:+UseStringCache -XX:+OptimizeStringConcat -XX:+UseBiasedLocking -XX:+UseFastAccessorMethods -XX:+UseFastEmptyMethods -XX:+TieredCompilation -XX:+DisableExplicitGC"
set "JAVA_OPTS=%JAVA_OPTS% -Dsun.rmi.dgc.server.gcInterval=3600000 -Dsun.rmi.dgc.client.gcInterval=3600000 -Djava.net.preferIPv4Stack=true"
rem set "JAVA_OPTS=%JAVA_OPTS% -XX:+UnlockCommercialFeatures -XX:+FlightRecorder"

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