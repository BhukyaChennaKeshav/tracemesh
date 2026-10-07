USE tracemesh_db;

SELECT 
    window_start, 
    p99_latency_ms, 
    avg_latency_ms,
    total_requests,
    error_count
FROM service_latency_metrics
WHERE serviceName = 'payment-service'
ORDER BY window_start DESC
LIMIT 10;
