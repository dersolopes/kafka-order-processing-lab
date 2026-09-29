package com.kafkaorder.consumer_notification.consumer;

import com.kafkaorder.consumer_notification.event.OrderCreatedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderNotificationConsumer {

    @KafkaListener(
            topics = "orders.created",
            groupId = "order-notification-group"
    )
    public void consume(OrderCreatedEvent event) {

        System.out.println("=== NOTIFICAÇÃO ===");
        System.out.println("Pedido: " + event.orderId());
        System.out.println("Cliente: " + event.customerId());
        System.out.println("Produto: " + event.product());
    }
}