package com.kafkaorder.producer.event;

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

    // Nome do tópico Kafka onde os eventos serão publicados
    // Tópicos são categorias para organizar mensagens (ex: orders.created, payments.processed)
    private static final String TOPIC = "orders.created";

    // KafkaTemplate gerencia a conexão com o broker e serialização das mensagens
    // <String, OrderCreatedEvent> indica: Key = String, Value = OrderCreatedEvent
    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public OrderEventProducer(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {
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
        // send(topic, key, value)
        // - topic: "orders.created" - onde a mensagem será armazenada
        // - key: event.orderId() - garante que eventos do mesmo pedido ficam ordenados
        // - value: event - o conteúdo serializado da mensagem
        kafkaTemplate.send(TOPIC, event.orderId(), event);
    }
}