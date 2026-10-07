package com.tracemesh.producer.controller;

import com.tracemesh.producer.service.TraceProducerService;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/anomaly")
public class AnomalyController {

    private final TraceProducerService producerService;

    public AnomalyController(TraceProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("/toggle")
    public Map<String, Object> toggleAnomaly(@RequestParam boolean enable) {
        producerService.setAnomalyActive(enable);
        return Map.of("anomalyActive", enable, "targetService", "payment-service");
    }

    @GetMapping("/status")
    public Map<String, Object> getStatus() {
        return Map.of("anomalyActive", producerService.isAnomalyActive());
    }
}