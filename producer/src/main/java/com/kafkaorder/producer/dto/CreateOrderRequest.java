package com.kafkaorder.producer.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public record CreateOrderRequest(

        @NotBlank // Impede null, "" e "   "
        String customerId,

        @NotBlank // Impede null, "" e "   "
        String product,

        @NotNull // Impede null
        @Positive // Impede 0 e números negativos
        Integer quantity,

        @NotNull // Impede null
        @Positive
        BigDecimal amount
) {
}