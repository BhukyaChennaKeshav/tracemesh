FROM maven:3.9-eclipse-temurin-11 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

FROM apache/spark:3.5.0
WORKDIR /app
USER root
COPY --from=build /app/target/spark-streaming-job-1.0-SNAPSHOT.jar /app/job.jar

# spark-submit owns the master configuration; the application must not replace
# it with local[*]. Wait for the standalone master before submitting.
CMD ["bash", "-lc", "set -e; until bash -c '</dev/tcp/spark-master/7077' 2>/dev/null; do echo 'Waiting for Spark master...'; sleep 3; done; exec /opt/spark/bin/spark-submit --class com.tracemesh.spark.SparkStreamingApp --master ${SPARK_MASTER_URL:-spark://spark-master:7077} --conf spark.driver.host=${SPARK_DRIVER_HOST:-spark-streaming-app} --conf spark.driver.bindAddress=0.0.0.0 --conf spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-3} /app/job.jar"]
