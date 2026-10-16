package com.tracemesh.producer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Probes each stage of the pipeline so the dashboard can show which node is up. */
@Service
public class PipelineHealthService {

    private static final Duration TIMEOUT = Duration.ofSeconds(2);

    private final KafkaAdmin kafkaAdmin;
    private final LiveMetricsService metrics;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();
    private final ObjectMapper mapper = new ObjectMapper();

    @Value("${tracemesh.kafka.topic}")
    private String topic;
    @Value("${tracemesh.pipeline.spark-master-url}")
    private String sparkMasterUrl;
    @Value("${tracemesh.pipeline.namenode-url}")
    private String namenodeUrl;
    @Value("${tracemesh.pipeline.hive-host}")
    private String hiveHost;
    @Value("${tracemesh.pipeline.hive-port}")
    private int hivePort;

    public PipelineHealthService(KafkaAdmin kafkaAdmin, LiveMetricsService metrics) {
        this.kafkaAdmin = kafkaAdmin;
        this.metrics = metrics;
    }

    public Map<String, Object> check() {
        // Probe in parallel so one unreachable node doesn't stall the whole check
        List<Supplier<Map<String, Object>>> probes = List.of(this::producer, this::kafka, this::spark, this::hdfs, this::hive);
        List<Map<String, Object>> stages = probes.stream()
                .map(CompletableFuture::supplyAsync)
                .toList().stream()
                .map(CompletableFuture::join)
                .toList();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("checkedAt", Instant.now().toString());
        out.put("stages", stages);
        return out;
    }

    private Map<String, Object> producer() {
        Map<String, Object> snap = metrics.snapshot();
        Object last = snap.get("lastSpanAt");
        boolean flowing = last != null && Instant.parse(last.toString()).isAfter(Instant.now().minusSeconds(10));
        return stage("producer", "Trace Producer", "Spring Boot", flowing ? "up" : "degraded",
                flowing ? snap.get("throughputPerSec") + " spans/s consumed" : "No spans seen in the last 10s",
                Map.of("consumedTotal", snap.get("consumedTotal")));
    }

    private Map<String, Object> kafka() {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            DescribeClusterResult cluster = admin.describeCluster();
            int brokers = cluster.nodes().get(3, TimeUnit.SECONDS).size();
            int partitions = admin.describeTopics(List.of(topic)).allTopicNames().get(3, TimeUnit.SECONDS)
                    .get(topic).partitions().size();
            return stage("kafka", "Kafka", "KRaft broker", "up",
                    brokers + " broker, topic " + topic + " (" + partitions + " partitions)",
                    Map.of("brokers", brokers, "partitions", partitions));
        } catch (Exception e) {
            return stage("kafka", "Kafka", "KRaft broker", "down", rootMessage(e), Map.of());
        }
    }

    private Map<String, Object> spark() {
        try {
            JsonNode json = getJson(sparkMasterUrl + "/json/");
            int workers = json.path("aliveworkers").asInt(json.path("workers").size());
            List<String> apps = new ArrayList<>();
            json.path("activeapps").forEach(a -> apps.add(a.path("name").asText()));
            boolean jobRunning = apps.stream().anyMatch(a -> a.contains("TraceMesh"));
            String status = workers == 0 ? "degraded" : jobRunning ? "up" : "degraded";
            String detail = workers + " worker(s), " + (jobRunning ? "streaming job running" : "streaming job not running");
            return stage("spark", "Spark Streaming", "Structured Streaming", status, detail,
                    Map.of("workers", workers, "activeApps", apps));
        } catch (Exception e) {
            return stage("spark", "Spark Streaming", "Structured Streaming", "down", rootMessage(e), Map.of());
        }
    }

    private Map<String, Object> hdfs() {
        try {
            JsonNode bean = getJson(namenodeUrl + "/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState")
                    .path("beans").path(0);
            int live = bean.path("NumLiveDataNodes").asInt();
            long used = bean.path("CapacityUsed").asLong();
            long total = bean.path("CapacityTotal").asLong();
            return stage("hdfs", "HDFS", "NameNode + DataNode", live > 0 ? "up" : "degraded",
                    live + " live datanode(s), " + (used / (1024 * 1024)) + " MB used",
                    Map.of("liveDataNodes", live, "capacityUsed", used, "capacityTotal", total,
                            "filesTotal", bean.path("FilesTotal").asLong()));
        } catch (Exception e) {
            return stage("hdfs", "HDFS", "NameNode + DataNode", "down", rootMessage(e), Map.of());
        }
    }

    private Map<String, Object> hive() {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(hiveHost, hivePort), (int) TIMEOUT.toMillis());
            return stage("hive", "Hive", "HiveServer2", "up", "Accepting connections on :" + hivePort, Map.of());
        } catch (Exception e) {
            return stage("hive", "Hive", "HiveServer2", "down", rootMessage(e), Map.of());
        }
    }

    private JsonNode getJson(String url) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url)).timeout(TIMEOUT).GET().build();
        HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() >= 400) throw new IllegalStateException("HTTP " + res.statusCode() + " from " + url);
        return mapper.readTree(res.body());
    }

    private static Map<String, Object> stage(String id, String name, String tech, String status,
                                             String detail, Map<String, Object> meta) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", id);
        m.put("name", name);
        m.put("tech", tech);
        m.put("status", status);
        m.put("detail", detail);
        m.put("meta", meta);
        return m;
    }

    private static String rootMessage(Throwable e) {
        Throwable t = e;
        while (t.getCause() != null) t = t.getCause();
        if (t instanceof java.nio.channels.UnresolvedAddressException || t instanceof java.net.UnknownHostException) {
            return "Host not found (is the container running?)";
        }
        if (t instanceof java.net.ConnectException) return "Connection refused (still starting?)";
        if (t instanceof java.net.http.HttpTimeoutException || t instanceof java.net.SocketTimeoutException
                || t instanceof java.util.concurrent.TimeoutException) return "Timed out";
        String msg = t.getMessage();
        return msg == null ? t.getClass().getSimpleName() : msg;
    }
}
