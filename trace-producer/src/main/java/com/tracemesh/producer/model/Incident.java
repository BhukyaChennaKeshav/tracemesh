package com.tracemesh.producer.model;

/** An SLA breach: opens when a service's window P99 crosses the threshold, resolves when it drops back. */
public class Incident {
    private final String id;
    private final String serviceName;
    private final String startedAt;
    private String resolvedAt;
    private double peakP99Ms;
    private long breachedWindows;

    public Incident(String id, String serviceName, String startedAt, double p99) {
        this.id = id;
        this.serviceName = serviceName;
        this.startedAt = startedAt;
        this.peakP99Ms = p99;
        this.breachedWindows = 1;
    }

    public void recordBreach(double p99) {
        peakP99Ms = Math.max(peakP99Ms, p99);
        breachedWindows++;
    }

    public void resolve(String at) { this.resolvedAt = at; }

    public String getId() { return id; }
    public String getServiceName() { return serviceName; }
    public String getStartedAt() { return startedAt; }
    public String getResolvedAt() { return resolvedAt; }
    public double getPeakP99Ms() { return peakP99Ms; }
    public long getBreachedWindows() { return breachedWindows; }
    public boolean isActive() { return resolvedAt == null; }
}
