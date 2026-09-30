package com.kafkaorder.producer.event;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Producer Kafka responsável por enviar eventos de pedido para o broker.
 *
 * Conceitos importantes:
 * - KafkaTemplate: classe do Spring Kafka que abstrai a comunicação com o broker
 * - Topic: canal onde as mensagens são publicadas e consumidas (como uma fila)
 * - Key: chave da mensagem usada para particionamento (garante ordem para mesma chave)
 * - Value: conteúdo da mensagem (o evento em si)
 */
@Component
public class OrderEventProducer {

    @Value("${app.kafka.topics.orders-created}")
    private String topic;

    // KafkaTemplate gerencia a conexão com o broker e serialização das mensagens
    // <String, OrderCreatedEvent> indica: Key = String, Value = OrderCreatedEvent
    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public OrderEventProducer(
            KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {

        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publica um evento no tópico Kafka.
     *
     * Particionamento no Kafka:
     * - O Kafka divide tópicos em partições para paralelismo
     * - A key (orderId) determina em qual partição a mensagem vai
     * - Mensagens com mesma key sempre vão para mesma partição (garante ordem)
     * - Sem key, as mensagens são distribuídas aleatoriamente entre partições
     *
     * @param event Evento de domínio a ser publicado
     */
    public void publish(OrderCreatedEvent event) {

        kafkaTemplate.send(
                topic,
                event.orderId(),
                event
        );
    }
}