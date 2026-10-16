package com.tracemesh.producer.service;

import com.tracemesh.producer.model.TraceSpan;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

@Service
public class TraceProducerService {

    public static final String DEFAULT_TARGET = "payment-service";

    private final KafkaTemplate<String, TraceSpan> kafkaTemplate;
    private final Random random = new Random();

    private final List<String> services = List.of(
        "auth-service", "order-service", "payment-service", "inventory-service", "notification-service"
    );

    @Value("${tracemesh.kafka.topic}")
    private String topicName;

    // Written by the REST thread, read by the scheduler thread
    private volatile boolean anomalyActive = false;
    private volatile String anomalyTarget = DEFAULT_TARGET;
    private volatile Instant anomalySince;

    public TraceProducerService(KafkaTemplate<String, TraceSpan> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void setAnomalyActive(boolean active) {
        setAnomaly(active, anomalyTarget);
    }

    public void setAnomaly(boolean active, String target) {
        if (!services.contains(target)) {
            throw new IllegalArgumentException("Unknown service: " + target);
        }
        this.anomalyTarget = target;
        this.anomalySince = active ? Instant.now() : null;
        this.anomalyActive = active;
    }

    public boolean isAnomalyActive() {
        return anomalyActive;
    }

    public List<String> getServices() {
        return services;
    }

    public Map<String, Object> anomalyState() {
        return Map.of(
            "anomalyActive", anomalyActive,
            "targetService", anomalyTarget,
            "since", anomalySince == null ? "" : anomalySince.toString()
        );
    }

    @Scheduled(fixedRate = 10) // Emits ~100 logs/sec
    public void generateAndSendTrace() {
        String service = services.get(random.nextInt(services.size()));
        long duration;

        // Inject high latency on the target service when anomaly toggle is ON
        if (anomalyActive && anomalyTarget.equals(service)) {
            duration = 1500 + random.nextInt(2000); // 1500ms - 3500ms spike
        } else {
            duration = 20 + random.nextInt(150);    // 20ms - 170ms normal
        }

        int statusCode = (duration > 2000) ? 504 : 200;
        String timestamp = Instant.now().toString();

        TraceSpan span = new TraceSpan(
            service,
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            timestamp,
            duration,
            statusCode
        );

        kafkaTemplate.send(topicName, service, span);
    }
}
