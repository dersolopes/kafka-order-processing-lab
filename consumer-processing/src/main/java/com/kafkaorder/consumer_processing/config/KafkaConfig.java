package com.kafkaorder.consumer_processing.config;

import com.kafkaorder.consumer_processing.event.OrderCreatedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    /*
     * Cria a configuração que será utilizada pelo nosso Consumer Kafka.
     *
     * Aqui definimos:
     * - onde está o Kafka
     * - qual é o consumer group
     * - como a chave da mensagem será desserializada
     * - como o valor da mensagem será desserializado
     */
    @Bean
    public ConsumerFactory<String, OrderCreatedEvent> consumerFactory() {

        Map<String, Object> config = new HashMap<>();

        // Endereço do Kafka
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Nome do grupo ao qual este consumer pertence
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "order-processing-group");

        /*
         * Em vez de colocar diretamente StringDeserializer e JacksonJsonDeserializer,
         * usamos o ErrorHandlingDeserializer.
         * Ele permite que o Spring Kafka trate erros que acontecem durante a desserialização da mensagem.
         */
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);

        /*
         * Aqui dizemos ao ErrorHandlingDeserializer:
         *  - "Para a KEY, utilize StringDeserializer".
         *  - A chave da nossa mensagem Kafka é o orderId.
         */
        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);

        /*
         * Para o VALUE, utilize JacksonJsonDeserializer.
         *  - O VALUE é o JSON do OrderCreatedEvent.
         */
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JacksonJsonDeserializer.class);

        /*
         * Por segurança, informamos quais pacotes podem ser
         * utilizados durante a desserialização JSON.
         */
        config.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "com.kafkaorder.processing.event");

        /*
         * Dizemos qual classe Java representa o JSON recebido.
         * JSON recebido do Kafka -> OrderCreatedEvent
         */
        config.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, OrderCreatedEvent.class);

        // Cria o ConsumerFactory com todas essas configurações.
        return new DefaultKafkaConsumerFactory<>(config);
    }

    /*
     * Cria a "fábrica" responsável por criar e configurar
     * os listeners Kafka (@KafkaListener).
     *
     * O nosso @KafkaListener utiliza essa configuração
     * para saber como deve consumir as mensagens.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent>
    kafkaListenerContainerFactory() {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent>();

        /*
         * Dizemos para o Listener:
         *
         * "Use o ConsumerFactory que configuramos acima."
         */
        factory.setConsumerFactory(consumerFactory());

        return factory;
    }
}