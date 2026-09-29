package com.kafkaorder.producer.dto;

import com.kafkaorder.producer.model.OrderStatus;

public record CreateOrderResponse(

        String orderId,
        OrderStatus orderStatus) {}