# StreamHorizon Docker file

# sudo docker build -t streamhorizon/streamhorizon:v2 .
# sudo docker run -t -i streamhorizon/streamhorizon:v2 /bin/bash


FROM ubuntu:14.10
MAINTAINER streamhorizon@gmail.com

RUN export DEBIAN_FRONTEND=noninteractive
RUN echo "mysql-server-5.5 mysql-server/root_password password shroot" | debconf-set-selections
RUN echo "mysql-server-5.5 mysql-server/root_password_again password shroot" | debconf-set-selections

RUN apt-get update && apt-get install -y wget unzip mysql-server-5.5 mysql-client-core-5.5 mysql-server openjdk-7-jdk
RUN wget -q http://stream-horizon.com/trial_download.php -O /tmp/streamhorizon.zip
RUN mkdir -p /opt/streamhorizon/
RUN mkdir -p /tmp/demo_sales/input
RUN mkdir -p /tmp/demo_sales/bulk_out
RUN mkdir -p /tmp/demo_sales/archive
RUN mkdir -p /tmp/demo_sales/error
RUN unzip /tmp/streamhorizon.zip -d /opt/streamhorizon/

# remove this line after new SH release
RUN echo 'cd "$(dirname "$0")"\n\n' | cat - /opt/streamhorizon/stream-horizon-3.3.5/demo_sales/file_multiplier.sh > temp && mv temp /opt/streamhorizon/stream-horizon-3.3.5/demo_sales/file_multiplier.sh
RUN sed -i 's/499/99/g' /opt/streamhorizon/stream-horizon-3.3.5/demo_sales/file_multiplier.sh
RUN chmod +x /opt/streamhorizon/stream-horizon-3.3.5/demo_sales/file_multiplier.sh

# prepare mysql
RUN cp /opt/streamhorizon/stream-horizon-3.3.5/demo_sales/mysql/engine-config.xml /opt/streamhorizon/stream-horizon-3.3.5/config/
RUN perl -i -pe 's/.*/ <property name="directoryPath">\/tmp\/demo_sales\/input<\/property> / if $.==12' /opt/streamhorizon/stream-horizon-3.3.5/config/engine-config.xml
RUN perl -i -pe 's/.*/ <property name="bulkOutputDirectory">\/tmp\/demo_sales\/bulk_out<\/property> / if $.==57' /opt/streamhorizon/stream-horizon-3.3.5/config/engine-config.xml
RUN perl -i -pe 's/.*/ <archiveDirectory>\/tmp\/demo_sales\/archive<\/archiveDirectory> / if $.==16' /opt/streamhorizon/stream-horizon-3.3.5/config/engine-config.xml
RUN perl -i -pe 's/.*/ <errorDirectory>\/tmp\/demo_sales\/error<\/errorDirectory> / if $.==17' /opt/streamhorizon/stream-horizon-3.3.5/config/engine-config.xml
RUN perl -i -pe 's/.*/ <etlProcessingThreadCount>4<\/etlProcessingThreadCount> / if $.==19' /opt/streamhorizon/stream-horizon-3.3.5/config/engine-config.xml


ADD startup.sh /var/startup.sh

RUN chmod +x /var/startup.sh

RUN echo "\n/var/startup.sh" >> /etc/bash.bashrc
