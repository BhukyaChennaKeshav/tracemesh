package com.tracemesh.producer.controller;

import com.tracemesh.producer.service.HiveQueryService;
import com.tracemesh.producer.service.PipelineHealthService;
import com.tracemesh.producer.service.TraceProducerService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class WarehouseController {

    private final HiveQueryService hive;
    private final PipelineHealthService health;
    private final TraceProducerService producer;

    public WarehouseController(HiveQueryService hive, PipelineHealthService health, TraceProducerService producer) {
        this.hive = hive;
        this.health = health;
        this.producer = producer;
    }

    @GetMapping("/pipeline/health")
    public Map<String, Object> pipelineHealth() {
        return health.check();
    }

    @GetMapping("/warehouse/status")
    public Map<String, Object> warehouseStatus() {
        return hive.status();
    }

    @GetMapping("/warehouse/queries")
    public List<HiveQueryService.NamedQuery> queries() {
        return hive.queries(null);
    }

    @GetMapping("/warehouse/queries/{id}")
    public ResponseEntity<Map<String, Object>> run(@PathVariable String id,
                                                   @RequestParam(defaultValue = TraceProducerService.DEFAULT_TARGET) String service,
                                                   @RequestParam(defaultValue = "false") boolean refresh) {
        // The service name is spliced into SQL, so only accept known services.
        if (!producer.getServices().contains(service)) {
            return ResponseEntity.badRequest().body(Map.of("error", "Unknown service: " + service));
        }
        try {
            return ResponseEntity.ok(hive.run(id, service, refresh));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "Hive query failed: " + e.getMessage()));
        }
    }
}
