package com.broker.currency.controller;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.broker.currency.model.ExchangeRate;
import com.broker.currency.repository.ExchangeRateRepository;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/rates")
@RequiredArgsConstructor
public class RateController {

    private final ExchangeRateRepository repository;

    @GetMapping("/{base}/{target}/history")
    public List<ExchangeRate> history(@PathVariable String base, @PathVariable String target) {
        return repository.findTop20ByBaseCurrencyAndTargetCurrencyOrderByFetchedAtDesc(base, target);
    }
}