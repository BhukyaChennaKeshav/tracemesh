package com.tracemesh.producer.service;

import com.tracemesh.producer.model.Incident;
import com.tracemesh.producer.model.TraceSpan;
import com.tracemesh.producer.model.WindowMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tails the telemetry topic and computes the same sliding-window aggregates as the Spark job
 * (P99, avg, count, 5xx errors per service), in memory, so the dashboard has sub-second-fresh data.
 * Spark/Hive remain the durable, historical path.
 */
@Service
public class LiveMetricsService {

    private static final Logger log = LoggerFactory.getLogger(LiveMetricsService.class);
    private static final int RECENT_SPANS = 200;
    private static final int MAX_INCIDENTS = 100;

    private record SpanPoint(long epochMs, long durationMs, int statusCode) {}

    private final Map<String, ConcurrentLinkedDeque<SpanPoint>> buffers = new ConcurrentHashMap<>();
    private final ConcurrentLinkedDeque<TraceSpan> recentSpans = new ConcurrentLinkedDeque<>();
    private final Deque<Map<String, Object>> history = new ArrayDeque<>();
    private final Deque<Incident> incidents = new ArrayDeque<>();
    private final Map<String, Incident> openIncidents = new HashMap<>();
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();
    private final AtomicLong consumedTotal = new AtomicLong();

    private volatile List<WindowMetric> latest = List.of();
    private volatile double throughputPerSec;
    private volatile Instant lastSpanAt;
    private long lastTickCount;
    private long lastTickAt = System.currentTimeMillis();

    private final TraceProducerService producerService;

    @Value("${tracemesh.sla.p99-threshold-ms}")
    private double slaThresholdMs;
    @Value("${tracemesh.window.size-seconds}")
    private long windowSeconds;
    @Value("${tracemesh.window.slide-seconds}")
    private long slideSeconds;
    @Value("${tracemesh.window.history-points}")
    private int historyPoints;

    public LiveMetricsService(TraceProducerService producerService) {
        this.producerService = producerService;
    }

    @KafkaListener(topics = "${tracemesh.kafka.topic}")
    public void onSpan(TraceSpan span) {
        long ts;
        try {
            ts = Instant.parse(span.getTimestamp()).toEpochMilli();
        } catch (Exception e) {
            ts = System.currentTimeMillis();
        }
        buffers.computeIfAbsent(span.getServiceName(), k -> new ConcurrentLinkedDeque<>())
               .addLast(new SpanPoint(ts, span.getExecutionDurationMs(), span.getStatusCode()));
        recentSpans.addFirst(span);
        while (recentSpans.size() > RECENT_SPANS) recentSpans.pollLast();
        consumedTotal.incrementAndGet();
        lastSpanAt = Instant.now();
    }

    @Scheduled(fixedRateString = "#{${tracemesh.window.slide-seconds} * 1000}")
    public void tick() {
        long now = System.currentTimeMillis();
        long windowStart = now - windowSeconds * 1000;

        List<WindowMetric> metrics = new ArrayList<>();
        for (var entry : new TreeMap<>(buffers).entrySet()) {
            ConcurrentLinkedDeque<SpanPoint> buf = entry.getValue();
            while (!buf.isEmpty() && buf.peekFirst().epochMs() < windowStart) buf.pollFirst();
            List<SpanPoint> window = new ArrayList<>(buf);
            metrics.add(aggregate(entry.getKey(), window, windowStart, now));
        }
        latest = List.copyOf(metrics);

        long count = consumedTotal.get();
        throughputPerSec = (count - lastTickCount) * 1000.0 / Math.max(1, now - lastTickAt);
        lastTickCount = count;
        lastTickAt = now;

        Map<String, Object> point = new LinkedHashMap<>();
        point.put("time", Instant.ofEpochMilli(now).toString());
        for (WindowMetric m : metrics) point.put(m.serviceName(), m.p99LatencyMs());

        synchronized (this) {
            history.addLast(point);
            while (history.size() > historyPoints) history.pollFirst();
            trackIncidents(metrics, Instant.ofEpochMilli(now).toString());
        }
        broadcast();
    }

    private WindowMetric aggregate(String service, List<SpanPoint> window, long start, long end) {
        long n = window.size();
        double p99 = 0, avg = 0;
        long errors = 0;
        if (n > 0) {
            long[] d = window.stream().mapToLong(SpanPoint::durationMs).sorted().toArray();
            p99 = d[(int) Math.min(n - 1, Math.ceil(0.99 * n) - 1)];
            avg = Arrays.stream(d).average().orElse(0);
            errors = window.stream().filter(s -> s.statusCode() >= 500).count();
        }
        return new WindowMetric(service, Instant.ofEpochMilli(start).toString(), Instant.ofEpochMilli(end).toString(),
                p99, Math.round(avg * 10) / 10.0, n, errors, n > 0 && p99 > slaThresholdMs);
    }

    private void trackIncidents(List<WindowMetric> metrics, String at) {
        for (WindowMetric m : metrics) {
            Incident open = openIncidents.get(m.serviceName());
            if (m.slaBreached()) {
                if (open == null) {
                    Incident inc = new Incident(UUID.randomUUID().toString(), m.serviceName(), at, m.p99LatencyMs());
                    openIncidents.put(m.serviceName(), inc);
                    incidents.addFirst(inc);
                    while (incidents.size() > MAX_INCIDENTS) incidents.pollLast();
                    log.warn("SLA breach opened: {} p99={}ms", m.serviceName(), m.p99LatencyMs());
                } else {
                    open.recordBreach(m.p99LatencyMs());
                }
            } else if (open != null) {
                open.resolve(at);
                openIncidents.remove(m.serviceName());
                log.info("SLA breach resolved: {}", m.serviceName());
            }
        }
    }

    public Map<String, Object> snapshot() {
        List<WindowMetric> m = latest;
        double maxP99 = m.stream().mapToDouble(WindowMetric::p99LatencyMs).max().orElse(0);
        long total = m.stream().mapToLong(WindowMetric::totalRequests).sum();
        double weightedAvg = total == 0 ? 0 :
                m.stream().mapToDouble(x -> x.avgLatencyMs() * x.totalRequests()).sum() / total;
        long errors = m.stream().mapToLong(WindowMetric::errorCount).sum();

        Map<String, Object> s = new LinkedHashMap<>();
        s.put("generatedAt", Instant.now().toString());
        s.put("slaThresholdMs", slaThresholdMs);
        s.put("windowSeconds", windowSeconds);
        s.put("slideSeconds", slideSeconds);
        s.put("maxP99Ms", maxP99);
        s.put("avgLatencyMs", Math.round(weightedAvg * 10) / 10.0);
        s.put("windowSpans", total);
        s.put("windowErrors", errors);
        s.put("activeBreaches", m.stream().filter(WindowMetric::slaBreached).count());
        s.put("throughputPerSec", Math.round(throughputPerSec * 10) / 10.0);
        s.put("consumedTotal", consumedTotal.get());
        s.put("lastSpanAt", lastSpanAt == null ? null : lastSpanAt.toString());
        s.put("anomaly", producerService.anomalyState());
        s.put("services", m);
        return s;
    }

    public synchronized List<Map<String, Object>> history(int points) {
        List<Map<String, Object>> all = new ArrayList<>(history);
        return all.subList(Math.max(0, all.size() - points), all.size());
    }

    public synchronized List<Incident> incidents() {
        return new ArrayList<>(incidents);
    }

    public List<TraceSpan> recentSpans(int limit, String service) {
        return recentSpans.stream()
                .filter(s -> service == null || service.equals(s.getServiceName()))
                .limit(limit)
                .toList();
    }

    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(0L);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
        emitters.add(emitter);
        send(emitter);
        return emitter;
    }

    /** Push the fresh snapshot to dashboards right after state changes (e.g. an anomaly toggle). */
    public void broadcast() {
        for (SseEmitter e : emitters) send(e);
    }

    private void send(SseEmitter emitter) {
        try {
            Map<String, Object> payload = new LinkedHashMap<>(snapshot());
            payload.put("historyPoint", history(1).stream().findFirst().orElse(null));
            payload.put("incidents", incidents().stream().limit(50).toList());
            emitter.send(SseEmitter.event().name("snapshot").data(payload));
        } catch (IOException | IllegalStateException ex) {
            emitters.remove(emitter);
        }
    }
}
