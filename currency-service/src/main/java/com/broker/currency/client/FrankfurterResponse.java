package com.broker.currency.client;

import java.util.Map;

public record FrankfurterResponse(String base, String date, Map<String, Double> rates) {}
