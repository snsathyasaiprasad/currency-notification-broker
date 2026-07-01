package com.broker.currency.repository;

import com.broker.currency.model.ExchangeRate;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface ExchangeRateRepository extends JpaRepository<ExchangeRate, Long> {
    List<ExchangeRate> findTop20ByBaseCurrencyAndTargetCurrencyOrderByFetchedAtDesc(
        String base, String target);
}
