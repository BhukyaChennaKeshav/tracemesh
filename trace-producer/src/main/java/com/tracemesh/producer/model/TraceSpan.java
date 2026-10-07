package com.tracemesh.producer.model;

public class TraceSpan {
    private String serviceName;
    private String traceId;
    private String spanId;
    private String timestamp;
    private long executionDurationMs;
    private int statusCode;

    public TraceSpan() {}

    public TraceSpan(String serviceName, String traceId, String spanId, String timestamp, long executionDurationMs, int statusCode) {
        this.serviceName = serviceName;
        this.traceId = traceId;
        this.spanId = spanId;
        this.timestamp = timestamp;
        this.executionDurationMs = executionDurationMs;
        this.statusCode = statusCode;
    }

    public String getServiceName() { return serviceName; }
    public void setServiceName(String serviceName) { this.serviceName = serviceName; }

    public String getTraceId() { return traceId; }
    public void setTraceId(String traceId) { this.traceId = traceId; }

    public String getSpanId() { return spanId; }
    public void setSpanId(String spanId) { this.spanId = spanId; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public long getExecutionDurationMs() { return executionDurationMs; }
    public void setExecutionDurationMs(long executionDurationMs) { this.executionDurationMs = executionDurationMs; }

    public int getStatusCode() { return statusCode; }
    public void setStatusCode(int statusCode) { this.statusCode = statusCode; }
}