FROM docker.io/folioci/eclipse-temurin:25-alpine
WORKDIR /app
COPY mod-source-record-storage-server/target/*-fat.jar app.jar
EXPOSE 8081
CMD ["java", "-jar", "app.jar"]
