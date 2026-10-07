package com.tracemesh.producer.service;

import com.tracemesh.producer.model.TraceSpan;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.UUID;

@Service
public class TraceProducerService {

    private final KafkaTemplate<String, TraceSpan> kafkaTemplate;
    private final Random random = new Random();
    
    private final List<String> services = List.of(
        "auth-service", "order-service", "payment-service", "inventory-service", "notification-service"
    );

    @Value("${tracemesh.kafka.topic}")
    private String topicName;

    private boolean anomalyActive = false;

    public TraceProducerService(KafkaTemplate<String, TraceSpan> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void setAnomalyActive(boolean active) {
        this.anomalyActive = active;
    }

    public boolean isAnomalyActive() {
        return anomalyActive;
    }

    @Scheduled(fixedRate = 10) // Emits ~100 logs/sec
    public void generateAndSendTrace() {
        String service = services.get(random.nextInt(services.size()));
        long duration;

        // Inject high latency on payment-service when anomaly toggle is ON
        if (anomalyActive && "payment-service".equals(service)) {
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