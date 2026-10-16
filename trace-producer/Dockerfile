FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
# Hive JDBC driver for the warehouse API; loaded in an isolated classloader at runtime
RUN mvn -q dependency:copy -Dartifact=org.apache.hive:hive-jdbc:3.1.3:jar:standalone \
        -DoutputDirectory=/app/lib -Dmdep.stripVersion=true
COPY src ./src
RUN mvn clean package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/lib /app/lib
COPY --from=build /app/target/trace-producer-0.0.1-SNAPSHOT.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
