package com.urbanpulse.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@RestController
@RequestMapping("/api")
public class TrafficController {

    @Autowired
    private RestTemplate restTemplate;

    @Value("${python.ai.url}")
    private String pythonAiUrl;

    @PostMapping("/roadblock")
    public ResponseEntity<?> createRoadblock(@RequestBody Map<String, Object> payload) {
        String targetUrl = pythonAiUrl + "/api/roadblock";
        return restTemplate.postForEntity(targetUrl, payload, Map.class);
    }

    @PostMapping("/route")
    public ResponseEntity<?> calculateRoute(@RequestBody Map<String, Object> payload) {
        String targetUrl = pythonAiUrl + "/api/route";
        return restTemplate.postForEntity(targetUrl, payload, Map.class);
    }

    @GetMapping("/roadblocks")
    public ResponseEntity<?> getRoadblocks() {
        String targetUrl = pythonAiUrl + "/api/roadblocks";
        return restTemplate.getForEntity(targetUrl, Map.class);
    }

    @PostMapping("/reset")
    public ResponseEntity<?> resetGrid() {
        String targetUrl = pythonAiUrl + "/api/reset";
        return restTemplate.postForEntity(targetUrl, null, Map.class);
    }

    @GetMapping("/congestion")
    public ResponseEntity<?> getCongestion() {
        String targetUrl = pythonAiUrl + "/api/congestion";
        return restTemplate.getForEntity(targetUrl, Map.class);
    }

    @GetMapping("/health")
    public ResponseEntity<?> health() {
        String targetUrl = pythonAiUrl + "/api/health";
        return restTemplate.getForEntity(targetUrl, Map.class);
    }

    @GetMapping("/status")
    public ResponseEntity<?> status() {
        String targetUrl = pythonAiUrl + "/api/status";
        return restTemplate.getForEntity(targetUrl, Map.class);
    }
}