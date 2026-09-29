package com.kafkaorder.consumer_notification.event;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderCreatedEvent(
        String eventId,
        String orderId,
        String customerId,
        String product,
        Integer quantity,
        BigDecimal amount,
        Instant occurredAt
) {}
