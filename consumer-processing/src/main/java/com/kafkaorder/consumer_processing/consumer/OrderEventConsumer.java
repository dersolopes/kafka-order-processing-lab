package com.kafkaorder.consumer_processing.consumer;
import com.kafkaorder.consumer_processing.event.OrderCreatedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderEventConsumer {

    @KafkaListener(
            topics = "orders.created",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(OrderCreatedEvent event) {

        System.out.println("=== PEDIDO RECEBIDO ===");
        System.out.println("Event ID: " + event.eventId());
        System.out.println("Order ID: " + event.orderId());
        System.out.println("Cliente: " + event.customerId());
        System.out.println("Produto: " + event.product());
        System.out.println("Quantidade: " + event.quantity());
        System.out.println("Valor: " + event.amount());
    }
}