package com.broker.notification.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.broker.notification.model.Threshold;
import com.broker.notification.repository.ThresholdRepository;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/thresholds")
@RequiredArgsConstructor
public class ThresholdController {

    private final ThresholdRepository repository;

    @PostMapping
    public Threshold create(@RequestBody Threshold threshold) {
        return repository.save(threshold);
    }

    @GetMapping
    public Iterable<Threshold> all() {
        return repository.findAll();
    }
}