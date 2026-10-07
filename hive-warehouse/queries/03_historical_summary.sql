USE tracemesh_db;

SELECT 
    serviceName,
    COUNT(1) AS total_windows_processed,
    ROUND(AVG(p99_latency_ms), 2) AS overall_avg_p99,
    MAX(p99_latency_ms) AS peak_p99_latency,
    SUM(error_count) AS total_server_errors
FROM service_latency_metrics
GROUP BY serviceName
ORDER BY peak_p99_latency DESC;