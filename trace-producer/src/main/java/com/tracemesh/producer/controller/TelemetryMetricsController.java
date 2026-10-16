package com.tracemesh.producer.controller;

import org.springframework.web.bind.annotation.*;
import java.sql.*;
import java.util.*;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class TelemetryMetricsController {

    private static final String HIVE_URL = "jdbc:hive2://hive-server:10000/tracemesh_db";

    @GetMapping("/metrics")
    public List<Map<String, Object>> getLatestMetrics() {
        List<Map<String, Object>> metrics = new ArrayList<>();
        
        String sql = "SELECT serviceName, window_start, p99_latency_ms, avg_latency_ms, total_requests, error_count " +
                     "FROM service_latency_metrics ORDER BY window_start DESC LIMIT 10";

        try (Connection conn = DriverManager.getConnection(HIVE_URL, "hive", "");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("serviceName", rs.getString("serviceName"));
                row.put("windowStart", rs.getString("window_start"));
                row.put("p99LatencyMs", rs.getDouble("p99_latency_ms"));
                row.put("avgLatencyMs", rs.getDouble("avg_latency_ms"));
                row.put("totalRequests", rs.getLong("total_requests"));
                row.put("errorCount", rs.getLong("error_count"));
                metrics.add(row);
            }
        } catch (Exception e) {
            // Fallback empty list if Hive is busy running MapReduce
            System.err.println("Error querying Hive: " + e.getMessage());
        }
        
        return metrics;
    }
}