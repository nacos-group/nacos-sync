FROM eclipse-temurin:11
ARG GID=2000
ARG UID=2000
ADD nacossync-distribution/target/*.tar.gz /opt/
WORKDIR /opt/nacos-sync
RUN addgroup --gid $GID app && useradd -r -M -g app app -u $UID
RUN chown -R app:app /opt/nacos-sync
USER app
ENV JAVA_OPTS=''
EXPOSE 8083
CMD ["./bin/startup.sh","run"]