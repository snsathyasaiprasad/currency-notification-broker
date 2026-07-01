package com.broker.currency.dto;

import java.math.BigDecimal;
import java.time.Instant;

public record RateEvent(String base, String target, BigDecimal rate, Instant timestamp) {}
