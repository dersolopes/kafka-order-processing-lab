package com.kafkaorder.producer.service;

import com.kafkaorder.producer.dto.CreateOrderRequest;
import com.kafkaorder.producer.dto.CreateOrderResponse;
import com.kafkaorder.producer.model.OrderStatus;
import com.kafkaorder.producer.event.OrderCreatedEvent;
import com.kafkaorder.producer.event.OrderEventProducer;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

/**
 * Service responsável por criar pedidos e publicar eventos no Kafka.
 *
 * Em uma arquitetura orientada a eventos com Kafka, o Producer (produtor)
 * envia mensagens para tópicos (topics) que serão consumidas por outros serviços.
 * Isso permite desacoplamento entre sistemas e processamento assíncrono.
 */
@Service
public class OrderService {

    // Producer Kafka responsável por enviar eventos para o broker
    private final OrderEventProducer orderEventProducer;

    public OrderService(OrderEventProducer orderEventProducer) {
        this.orderEventProducer = orderEventProducer;
    }

    /**
     * Cria um novo pedido e publica um evento no Kafka.
     * Vantagens desse padrão:
     * - Desacoplamento: o produtor não precisa conhecer os consumidores
     * - Escalabilidade: múltiplos consumidores podem processar em paralelo
     * - Resiliência: Kafka persiste as mensagens, garantindo entrega
     * - Assincronismo: o produtor não espera o processamento completar
     */
    public CreateOrderResponse createOrder(CreateOrderRequest request) {

        // 1. O pedido é criado localmente (geração do ID)
        String orderId = UUID.randomUUID().toString();

        // 2. Um evento de domínio é criado contendo os dados do pedido
        OrderCreatedEvent event = new OrderCreatedEvent(
                UUID.randomUUID().toString(), // ID único do evento
                orderId,
                request.customerId(),
                request.product(),
                request.quantity(),
                request.amount(),
                Instant.now() // Timestamp do evento
        );

        // 3. O Producer envia o evento para um tópico Kafka
        orderEventProducer.publish(event);

        // 4. Retorna resposta imediatamente sem esperar processamento
        //    (RECEIVED indica que o pedido foi aceito e será processado assincronamente)
        return new CreateOrderResponse(
                orderId,
                OrderStatus.RECEIVED

        // Próximos passos:
        // 4. O Kafka serializa o evento (geralmente JSON ou Avro) e o envia ao broker
        // 5. Consumidores (outros serviços) lerão esse evento e processarão
        //    (ex: serviço de pagamento, serviço de estoque, serviço de notificação)
        );
    }
}