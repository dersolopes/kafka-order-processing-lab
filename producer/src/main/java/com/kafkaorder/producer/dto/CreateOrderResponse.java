package com.kafkaorder.producer.dto;

import jakarta.validation.constraints.NotNull;

public record CreateOrderResponse(

        String orderId,
        OrderStatus orderStatus) {}