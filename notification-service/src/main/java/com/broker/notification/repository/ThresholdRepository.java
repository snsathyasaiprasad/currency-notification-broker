package com.broker.notification.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.broker.notification.model.Threshold;

public interface ThresholdRepository extends JpaRepository<Threshold, Long> {
    List<Threshold> findByBaseCurrencyAndTargetCurrency(String base, String target);
}