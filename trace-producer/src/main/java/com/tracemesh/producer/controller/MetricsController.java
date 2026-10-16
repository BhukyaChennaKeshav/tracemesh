package com.tracemesh.producer.controller;

import com.tracemesh.producer.model.Incident;
import com.tracemesh.producer.model.TraceSpan;
import com.tracemesh.producer.service.LiveMetricsService;
import com.tracemesh.producer.service.TraceProducerService;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class MetricsController {

    private final LiveMetricsService metrics;
    private final TraceProducerService producer;

    public MetricsController(LiveMetricsService metrics, TraceProducerService producer) {
        this.metrics = metrics;
        this.producer = producer;
    }

    @GetMapping("/services")
    public List<String> services() {
        return producer.getServices();
    }

    @GetMapping("/metrics/live")
    public Map<String, Object> live() {
        return metrics.snapshot();
    }

    /** P99 per service for each slide tick; points=120 covers the last 10 minutes at a 5s slide. */
    @GetMapping("/metrics/history")
    public List<Map<String, Object>> history(@RequestParam(defaultValue = "120") int points) {
        return metrics.history(Math.max(1, Math.min(points, 1000)));
    }

    @GetMapping("/incidents")
    public List<Incident> incidents() {
        return metrics.incidents();
    }

    @GetMapping("/spans/recent")
    public List<TraceSpan> recentSpans(@RequestParam(defaultValue = "50") int limit,
                                       @RequestParam(required = false) String service) {
        return metrics.recentSpans(Math.max(1, Math.min(limit, 200)), service);
    }

    /** Server-sent events: a "snapshot" event every slide tick and on every anomaly toggle. */
    @GetMapping(value = "/stream", produces = "text/event-stream")
    public SseEmitter stream() {
        return metrics.subscribe();
    }
}
