USE tracemesh_db;

SELECT 
    serviceName, 
    window_start, 
    p99_latency_ms, 
    total_requests,
    error_count
FROM service_latency_metrics
WHERE p99_latency_ms > 500
ORDER BY window_start DESC
LIMIT 10;