package com.broker.notification.controller;

import com.broker.notification.model.Threshold;
import com.broker.notification.repository.ThresholdRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/thresholds")
@RequiredArgsConstructor
public class ThresholdController {

    private final ThresholdRepository repository;

    @PostMapping
    public Threshold create(@RequestBody Threshold threshold) {
        List<Threshold> existing = repository.findByBaseCurrencyAndTargetCurrency(
            threshold.getBaseCurrency(), threshold.getTargetCurrency());

        return existing.stream()
            .filter(t -> t.getUserId().equals(threshold.getUserId())
                && t.getDirection() == threshold.getDirection())
            .findFirst()
            .map(t -> {
                t.setThresholdValue(threshold.getThresholdValue());
                return repository.save(t);
            })
            .orElseGet(() -> repository.save(threshold));
    }

    @GetMapping
    public Iterable<Threshold> all() {
        return repository.findAll();
    }
}
