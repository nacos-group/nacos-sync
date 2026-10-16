# nacos-sync built from a local checkout. run.sh copies the unpacked distribution
# (nacossync-distribution/target/nacos-sync-*/nacos-sync) into ./dist before building.
# The upgrade scenario overrides JRE_VERSION and DIST to build an older release from its own distribution.
ARG JRE_VERSION=17
FROM eclipse-temurin:${JRE_VERSION}-jre
ARG DIST=dist
WORKDIR /opt/nacos-sync
COPY ${DIST}/ /opt/nacos-sync/
EXPOSE 8083
# Run in the foreground instead of bin/startup.sh (nohup) so `docker logs` and restarts work.
ENTRYPOINT ["sh", "-c", "exec java ${JAVA_OPTS:--Xms256m -Xmx512m} \
  -Dspring.config.location=/opt/nacos-sync/conf/application.properties \
  -DnacosSync.home=/opt/nacos-sync \
  -jar /opt/nacos-sync/nacos-sync-server.jar \
  --logging.config=/opt/nacos-sync/conf/logback-spring.xml"]
