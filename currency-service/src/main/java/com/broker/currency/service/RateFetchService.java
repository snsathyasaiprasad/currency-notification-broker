package com.broker.currency.service;

import com.broker.currency.client.FrankfurterResponse;
import com.broker.currency.dto.RateEvent;
import com.broker.currency.model.ExchangeRate;
import com.broker.currency.repository.ExchangeRateRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class RateFetchService {

    private final ExchangeRateRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RestClient restClient = RestClient.create("https://api.frankfurter.dev");

    @Value("${currency.base}")
    private String base;

    @Value("${currency.targets}")
    private String targetsCsv;

    @Value("${currency.topic}")
    private String topic;

    @Scheduled(fixedDelayString = "${currency.poll-interval-ms}")
    public void fetchAndPublish() {
        List<String> targets = List.of(targetsCsv.split(","));
        String symbols = String.join(",", targets);

        FrankfurterResponse response = restClient.get()
            .uri("/v1/latest?base={base}&symbols={symbols}", base, symbols)
            .retrieve()
            .body(FrankfurterResponse.class);

        if (response == null || response.rates() == null) {
            log.warn("No rate data returned from provider");
            return;
        }

        response.rates().forEach((target, rateValue) -> {
            BigDecimal rate = BigDecimal.valueOf(rateValue);
            Instant now = Instant.now();

            repository.save(ExchangeRate.builder()
                .baseCurrency(base)
                .targetCurrency(target)
                .rate(rate)
                .fetchedAt(now)
                .build());

            RateEvent event = new RateEvent(base, target, rate, now);
            kafkaTemplate.send(topic, base + "-" + target, event);
            log.info("Published rate {} -> {} = {}", base, target, rate);
        });
    }
}
