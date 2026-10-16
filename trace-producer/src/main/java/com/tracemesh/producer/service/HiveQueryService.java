package com.tracemesh.producer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Runs the warehouse queries (hive-warehouse/queries/*.sql) against HiveServer2.
 *
 * The Hive JDBC driver drags in old Hadoop/Jetty/Jackson classes that clash with Spring Boot 3,
 * so the standalone driver jar is loaded in its own isolated classloader and only used through java.sql.
 */
@Service
public class HiveQueryService {

    private static final Logger log = LoggerFactory.getLogger(HiveQueryService.class);
    private static final long CACHE_MS = 15_000;
    private static final String TABLE = "tracemesh_db.service_latency_metrics";

    public record NamedQuery(String id, String title, String description, String sql) {}

    private record Cached(long at, Map<String, Object> result) {}

    // Spark writes Parquet partitioned by date_partition, so the table must be partitioned too,
    // and new partitions must be registered (MSCK REPAIR) before they become queryable.
    private static final List<String> SCHEMA_DDL = List.of(
            "CREATE DATABASE IF NOT EXISTS tracemesh_db",
            "CREATE EXTERNAL TABLE IF NOT EXISTS " + TABLE + " ("
                    + " window_start STRING, window_end STRING, serviceName STRING,"
                    + " p99_latency_ms DOUBLE, avg_latency_ms DOUBLE, total_requests BIGINT, error_count BIGINT)"
                    + " PARTITIONED BY (date_partition STRING)"
                    + " STORED AS PARQUET"
                    + " LOCATION 'hdfs://namenode:9000/user/hive/warehouse/tracemesh_metrics'"
    );

    private final Map<String, Cached> cache = new ConcurrentHashMap<>();
    private volatile Driver driver;
    private volatile String lastError;
    private volatile Instant lastRepairAt;

    @Value("${tracemesh.hive.jdbc-url}")
    private String jdbcUrl;
    @Value("${tracemesh.hive.user}")
    private String user;
    @Value("${tracemesh.hive.driver-jar}")
    private String driverJar;
    @Value("${tracemesh.sla.p99-threshold-ms}")
    private double slaThresholdMs;

    public List<NamedQuery> queries(String service) {
        String svc = service == null ? TraceProducerService.DEFAULT_TARGET : service;
        return List.of(
                new NamedQuery("sla-violations", "SLA violations",
                        "Most recent windows where P99 exceeded 500 ms (01_detect_sla_violations.sql)",
                        "SELECT serviceName, window_start, p99_latency_ms, total_requests, error_count"
                                + " FROM " + TABLE + " WHERE p99_latency_ms > 500"
                                + " ORDER BY window_start DESC LIMIT 50"),
                new NamedQuery("service-trend", "Service trend",
                        "Recent windows for one service (02_payment_service_trend.sql)",
                        "SELECT window_start, p99_latency_ms, avg_latency_ms, total_requests, error_count"
                                + " FROM " + TABLE + " WHERE serviceName = '" + svc + "'"
                                + " ORDER BY window_start DESC LIMIT 60"),
                new NamedQuery("historical-summary", "Historical summary",
                        "Per-service rollup across all stored windows (03_historical_summary.sql)",
                        "SELECT serviceName, COUNT(1) AS total_windows_processed,"
                                + " ROUND(AVG(p99_latency_ms), 2) AS overall_avg_p99,"
                                + " MAX(p99_latency_ms) AS peak_p99_latency,"
                                + " SUM(error_count) AS total_server_errors"
                                + " FROM " + TABLE + " GROUP BY serviceName ORDER BY peak_p99_latency DESC")
        );
    }

    /** service must already be validated against the known service list by the caller. */
    public Map<String, Object> run(String queryId, String service, boolean refresh) throws SQLException {
        NamedQuery q = queries(service).stream().filter(x -> x.id().equals(queryId)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown query: " + queryId));
        String key = queryId + ":" + service;
        Cached c = cache.get(key);
        if (!refresh && c != null && System.currentTimeMillis() - c.at() < CACHE_MS) return c.result();

        long started = System.currentTimeMillis();
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            prepare(st);
            List<String> columns = new ArrayList<>();
            List<Map<String, Object>> rows = new ArrayList<>();
            try (ResultSet rs = st.executeQuery(q.sql())) {
                ResultSetMetaData md = rs.getMetaData();
                for (int i = 1; i <= md.getColumnCount(); i++) columns.add(stripTablePrefix(md.getColumnLabel(i)));
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int i = 1; i <= columns.size(); i++) row.put(columns.get(i - 1), rs.getObject(i));
                    rows.add(row);
                }
            }
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("query", q);
            result.put("columns", columns);
            result.put("rows", rows);
            result.put("elapsedMs", System.currentTimeMillis() - started);
            result.put("executedAt", Instant.now().toString());
            cache.put(key, new Cached(System.currentTimeMillis(), result));
            lastError = null;
            return result;
        } catch (SQLException e) {
            lastError = e.getMessage();
            throw e;
        }
    }

    public Map<String, Object> status() {
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("jdbcUrl", jdbcUrl);
        s.put("driverAvailable", new File(driverJar).isFile());
        s.put("lastError", lastError);
        s.put("lastPartitionRepairAt", lastRepairAt == null ? null : lastRepairAt.toString());
        s.put("slaThresholdMs", slaThresholdMs);
        return s;
    }

    private void prepare(Statement st) throws SQLException {
        for (String ddl : SCHEMA_DDL) st.execute(ddl);
        // Register partitions Spark has written since the last repair (at most every 30s).
        if (lastRepairAt == null || Instant.now().isAfter(lastRepairAt.plusSeconds(30))) {
            // Spark's _spark_metadata dir sits next to the partitions; don't let MSCK reject it.
            st.execute("SET hive.msck.path.validation=ignore");
            st.execute("MSCK REPAIR TABLE " + TABLE);
            lastRepairAt = Instant.now();
        }
    }

    private Connection connect() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", "");
        Connection conn = loadDriver().connect(jdbcUrl, props);
        if (conn == null) throw new SQLException("Hive driver rejected URL " + jdbcUrl);
        return conn;
    }

    private Driver loadDriver() throws SQLException {
        if (driver != null) return driver;
        synchronized (this) {
            if (driver != null) return driver;
            File jar = new File(driverJar);
            if (!jar.isFile()) throw new SQLException("Hive JDBC driver jar not found at " + driverJar);
            try {
                URLClassLoader cl = new URLClassLoader(new URL[]{jar.toURI().toURL()}, ClassLoader.getPlatformClassLoader());
                driver = (Driver) Class.forName("org.apache.hive.jdbc.HiveDriver", true, cl)
                        .getDeclaredConstructor().newInstance();
                log.info("Loaded Hive JDBC driver from {}", driverJar);
                return driver;
            } catch (Exception e) {
                throw new SQLException("Could not load Hive JDBC driver: " + e.getMessage(), e);
            }
        }
    }

    private static String stripTablePrefix(String label) {
        int dot = label.lastIndexOf('.');
        return dot >= 0 ? label.substring(dot + 1) : label;
    }
}
