package com.tracemesh.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Value("${tracemesh.kafka.topic}")
    private String topicName;

    @Bean
    public NewTopic telemetryTopic() {
        return TopicBuilder.name(topicName)
                .partitions(3)  // Crucial: Allows Spark to read streams in parallel
                .replicas(1)    // 1 replica because we only have 1 local Kafka broker
                .build();
    }
}