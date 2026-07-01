package com.broker.notification.service;

import java.math.BigDecimal;
import java.util.Map;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import com.broker.notification.model.Threshold;
import com.broker.notification.repository.ThresholdRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class RateEventListener {

    private final ThresholdRepository thresholdRepository;
    private final SimpMessagingTemplate messagingTemplate;

    @KafkaListener(topics = "${notification.topic}", groupId = "notification-group")
    public void onRateEvent(Map<String, Object> event) {
        String base = (String) event.get("base");
        String target = (String) event.get("target");
        BigDecimal rate = new BigDecimal(event.get("rate").toString());

        for (Threshold t : thresholdRepository.findByBaseCurrencyAndTargetCurrency(base, target)) {
            boolean triggered =
                (t.getDirection() == Threshold.Direction.ABOVE && rate.compareTo(t.getThresholdValue()) > 0) ||
                (t.getDirection() == Threshold.Direction.BELOW && rate.compareTo(t.getThresholdValue()) < 0);

            if (triggered) {
                String message = String.format(
                    "ALERT for %s: %s/%s is now %s (threshold %s %s)",
                    t.getUserId(), base, target, rate, t.getDirection(), t.getThresholdValue());
                log.info(message);
                messagingTemplate.convertAndSend("/topic/alerts/" + t.getUserId(), message);
            }
        }
    }
}