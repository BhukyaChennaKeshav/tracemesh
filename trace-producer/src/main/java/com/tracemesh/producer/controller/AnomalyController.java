package com.tracemesh.producer.controller;

import com.tracemesh.producer.service.LiveMetricsService;
import com.tracemesh.producer.service.TraceProducerService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;

@RestController
@RequestMapping("/api/anomaly")
public class AnomalyController {

    private final TraceProducerService producerService;
    private final LiveMetricsService metricsService;

    public AnomalyController(TraceProducerService producerService, LiveMetricsService metricsService) {
        this.producerService = producerService;
        this.metricsService = metricsService;
    }

    @PostMapping("/toggle")
    public Map<String, Object> toggleAnomaly(@RequestParam boolean enable,
                                             @RequestParam(defaultValue = TraceProducerService.DEFAULT_TARGET) String service) {
        try {
            producerService.setAnomaly(enable, service);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        }
        metricsService.broadcast();
        return producerService.anomalyState();
    }

    @GetMapping("/status")
    public Map<String, Object> getStatus() {
        return producerService.anomalyState();
    }
}
