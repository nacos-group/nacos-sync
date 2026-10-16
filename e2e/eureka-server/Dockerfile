# Eureka Server from cloud-eureka/eureka-server, built for one Spring Cloud release train.
# run.sh builds the jar with -Dspring-boot.version / -Dspring-cloud.version and copies it to ./app.jar.
FROM eclipse-temurin:17-jre
COPY app.jar /app.jar
EXPOSE 18033
ENTRYPOINT ["java", "-Xms128m", "-Xmx256m", "-jar", "/app.jar"]
