package com.kafkaorder.consumer_audit.consumer;

import com.kafkaorder.consumer_audit.event.OrderCreatedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderAuditConsumer {

    @KafkaListener(
            topics = "orders.created",
            groupId = "order-audit-group"
    )
    public void consume(OrderCreatedEvent event) {

        System.out.println("=== AUDITORIA ===");
        System.out.println("Event ID: " + event.eventId());
        System.out.println("Pedido: " + event.orderId());
        System.out.println("Cliente: " + event.customerId());
        System.out.println("Produto: " + event.product());
        System.out.println("Data: " + event.occurredAt());
    }
}