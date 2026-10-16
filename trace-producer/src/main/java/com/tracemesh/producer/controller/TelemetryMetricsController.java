package com.tracemesh.producer.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/api")
public class TelemetryMetricsController {

    private static final Logger log = LoggerFactory.getLogger(TelemetryMetricsController.class);
    private static final int MAX_STORED_METRICS = 500;
    private static final int MAX_RESPONSE_METRICS = 50;

    // Live dashboard cache only. Historical metrics remain persisted as Parquet in HDFS and queryable via Hive.
    private final LinkedHashMap<String, Map<String, Object>> latestMetrics = new LinkedHashMap<>();

    @PostMapping("/metrics/ingest")
    public ResponseEntity<?> ingestMetrics(@RequestBody List<Map<String, Object>> incomingMetrics) {
        if (incomingMetrics == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "Expected a JSON array of processed metrics."));
        }

        List<Map<String, Object>> normalized = new ArrayList<>();
        Set<String> keys = new LinkedHashSet<>();
        for (Map<String, Object> incoming : incomingMetrics) {
            if (incoming == null
                    || incoming.get("serviceName") == null
                    || incoming.get("windowStart") == null
                    || incoming.get("p99LatencyMs") == null
                    || incoming.get("avgLatencyMs") == null
                    || incoming.get("totalRequests") == null
                    || incoming.get("errorCount") == null) {
                return ResponseEntity.badRequest()
                        .body(Map.of("error", "Each metric must include serviceName, windowStart, p99LatencyMs, avgLatencyMs, totalRequests, and errorCount."));
            }

            Map<String, Object> metric = new LinkedHashMap<>();
            metric.put("serviceName", String.valueOf(incoming.get("serviceName")));
            metric.put("windowStart", String.valueOf(incoming.get("windowStart")));
            metric.put("p99LatencyMs", incoming.get("p99LatencyMs"));
            metric.put("avgLatencyMs", incoming.get("avgLatencyMs"));
            metric.put("totalRequests", incoming.get("totalRequests"));
            metric.put("errorCount", incoming.get("errorCount"));
            normalized.add(metric);
            keys.add(metricKey(metric));
        }

        synchronized (latestMetrics) {
            for (Map<String, Object> metric : normalized) {
                String key = metricKey(metric);
                // Replayed Spark batches update the same window instead of duplicating it.
                latestMetrics.remove(key);
                latestMetrics.put(key, metric);
            }
            while (latestMetrics.size() > MAX_STORED_METRICS) {
                Iterator<String> oldest = latestMetrics.keySet().iterator();
                oldest.next();
                oldest.remove();
            }
        }

        log.debug("Accepted {} processed metric rows from Spark", normalized.size());
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of("accepted", normalized.size(), "uniqueWindows", keys.size()));
    }

    @GetMapping("/metrics")
    public List<Map<String, Object>> getLatestMetrics() {
        synchronized (latestMetrics) {
            return latestMetrics.values().stream()
                    .sorted(Comparator.comparing(
                            (Map<String, Object> row) -> String.valueOf(row.get("windowStart"))
                    ).reversed())
                    .limit(MAX_RESPONSE_METRICS)
                    .map(LinkedHashMap::new)
                    .map(row -> (Map<String, Object>) row)
                    .toList();
        }
    }

    private static String metricKey(Map<String, Object> metric) {
        return metric.get("serviceName") + "|" + metric.get("windowStart");
    }
}
