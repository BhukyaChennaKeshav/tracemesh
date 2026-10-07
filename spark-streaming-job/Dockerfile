# Stage 1: Build the Fat JAR
FROM maven:3.9-eclipse-temurin-11 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
# Package the application into an uber-jar (using maven-shade-plugin from pom.xml)
RUN mvn clean package -DskipTests

# Stage 2: Submit to Spark Master
FROM bitnami/spark:3.5
WORKDIR /app
USER root
COPY --from=build /app/target/spark-streaming-job-1.0-SNAPSHOT.jar /app/job.jar

# Wait for master and HDFS to initialize, then submit the streaming job
CMD ["sh", "-c", "sleep 25 && spark-submit \
    --class com.tracemesh.spark.SparkStreamingApp \
    --master spark://spark-master:7077 \
    /app/job.jar"]