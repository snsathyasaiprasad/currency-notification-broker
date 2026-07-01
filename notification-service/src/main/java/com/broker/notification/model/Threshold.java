package com.broker.notification.model;

import jakarta.persistence.*;
import lombok.*;
import java.math.BigDecimal;

@Entity
@Table(name = "thresholds")
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class Threshold {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String userId;
    private String baseCurrency;
    private String targetCurrency;
    private BigDecimal thresholdValue;

    @Enumerated(EnumType.STRING)
    private Direction direction; // ABOVE or BELOW

    public enum Direction { ABOVE, BELOW }
}
