package com.tracemesh.producer.service;

import com.tracemesh.producer.model.TraceSpan;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class TraceProducerService {

    private final KafkaTemplate<String, TraceSpan> kafkaTemplate;

    private final List<String> services = List.of(
            "auth-service", "order-service", "payment-service",
            "inventory-service", "notification-service"
    );

    @Value("${tracemesh.kafka.topic}")
    private String topicName;

    // The controller writes this flag while the scheduler reads it on another thread.
    private volatile boolean anomalyActive = false;

    public TraceProducerService(KafkaTemplate<String, TraceSpan> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void setAnomalyActive(boolean active) {
        this.anomalyActive = active;
    }

    public boolean isAnomalyActive() {
        return anomalyActive;
    }

    @Scheduled(fixedRateString = "${tracemesh.producer.interval-ms:10}")
    public void generateAndSendTrace() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        String service = services.get(random.nextInt(services.size()));

        long duration;
        if (anomalyActive && "payment-service".equals(service)) {
            duration = 1500 + random.nextInt(2000);
        } else {
            duration = 20 + random.nextInt(150);
        }

        int statusCode = duration > 2000 ? 504 : 200;
        TraceSpan span = new TraceSpan(
                service,
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                Instant.now().toString(),
                duration,
                statusCode
        );

        kafkaTemplate.send(topicName, service, span);
    }
}
