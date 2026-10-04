package com.urbanpulse.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    @KafkaListener(topics = "live-traffic-updates", groupId = "urbanpulse-group")
    public void consumeTrafficUpdate(String message) {
        // Ambient congestion + roadblock events. Broadcast to /topic/traffic.
        // (Congestion payloads are frequent, so keep this at debug level.)
        logger.debug("Kafka traffic event: {}", message);
        messagingTemplate.convertAndSend("/topic/traffic", message);
    }

    // Separate topic/websocket destination from ambient traffic updates
    // (Section 5) so the frontend's route-completion handler doesn't have to
    // filter out congestion noise from the same channel.
    @KafkaListener(topics = "route-decisions", groupId = "urbanpulse-group")
    public void consumeRouteDecision(String message) {
        logger.info("Kafka route-decision event received, relaying to /topic/route-updates");
        messagingTemplate.convertAndSend("/topic/route-updates", message);
    }
}