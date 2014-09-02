#!/usr/bin/bash
echo ${1} ${2} >> out.txt
export ORACLE_HOME=/db/oracle/product/11.2.0
export ORACLE_SID=orcl
export PATH=${PATH}:/db/oracle/product/11.2.0/bin
sqlldr userid=sh/sh@orcl silent=\(header,feedback\) errors=0 columnarrayrows=20000 streamsize=1024000 multithreading=false control=$ENGINE_HOME/bin/sqlldr/t${1}.ctl data=${2}
exit $?

