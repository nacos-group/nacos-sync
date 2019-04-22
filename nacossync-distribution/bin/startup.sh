#!/bin/bash


ACTION=$1


[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=$HOME/jdk/java
[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=/usr/java
[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=/opt/taobao/java
[ ! -e "$JAVA_HOME/bin/java" ] && error_exit "Please set the JAVA_HOME variable in your environment, We need java(x64)! jdk8 or later is better!"


export JAVA_HOME
export JAVA="$JAVA_HOME/bin/java"
export BASE_DIR=`cd $(dirname $0)/..; pwd`




JAVA_OPT="${JAVA_OPT} -server -Xms2g -Xmx2g -Xmn1g -XX:MetaspaceSize=256m -XX:MaxMetaspaceSize=512m"
JAVA_OPT="${JAVA_OPT} -XX:-OmitStackTraceInFastThrow -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${BASE_DIR}/nacossync_java_heapdump.hprof"
JAVA_OPT="${JAVA_OPT} -XX:-UseLargePages"
JAVA_OPT="${JAVA_OPT} -Dspring.config.location=${BASE_DIR}/conf/application.properties"
JAVA_OPT="${JAVA_OPT} -DnacosSync.home=${BASE_DIR}"

JAVA_MAJOR_VERSION=$($JAVA -version 2>&1 | sed -E -n 's/.* version "([0-9]*).*$/\1/p')
if [[ "$JAVA_MAJOR_VERSION" -ge "9" ]] ; then
  JAVA_OPT="${JAVA_OPT} -Xlog:gc*:file=${BASE_DIR}/logs/nacossync_gc.log:time,tags:filecount=10,filesize=102400"
else
  JAVA_OPT="${JAVA_OPT} -Xloggc:${BASE_DIR}/logs/nacossync_gc.log -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100M"
fi

JAVA_OPT="${JAVA_OPT} -jar ${BASE_DIR}/nacosSync-server.jar"
JAVA_OPT="${JAVA_OPT} --logging.config=${BASE_DIR}/conf/logback-spring.xml"

echo "$JAVA ${JAVA_OPT}"
echo "${BASE_DIR}"



if [ ! -f "${BASE_DIR}/logs/nacossync_start.out" ]; then
        touch "${BASE_DIR}/logs/nacossync_start.out"
fi


usage(){
    echo "command error"
}

start(){

echo "$JAVA ${JAVA_OPT}" > ${BASE_DIR}/logs/nacossync_start.out 2>&1 &
nohup $JAVA ${JAVA_OPT} >> ${BASE_DIR}/logs/nacossync_start.out 2>&1 &
echo "nacossync is starting，you can check the ${BASE_DIR}/logs/nacossync_start.out"

}


case "$ACTION" in
    start)
        start
    ;;
    *)
        usage
    ;;
esac
