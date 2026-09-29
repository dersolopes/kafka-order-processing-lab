package com.kafkaorder.producer.config;

import com.kafkaorder.producer.event.OrderCreatedEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    /*
     * Pega o valor definido no application.properties:
     * spring.kafka.bootstrap-servers=localhost:9092
     */
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, OrderCreatedEvent> producerFactory() {

        Map<String, Object> config = new HashMap<>();

        // Endereço do Kafka configurado no application.properties
        config.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        // A chave da mensagem é uma String (nosso orderId)
        config.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );

        // O objeto OrderCreatedEvent será convertido para JSON
        config.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JacksonJsonSerializer.class
        );

        /*
         * Não envia o tipo/classe Java no header da mensagem.
         * Assim o Consumer não precisa conhecer a classe existente dentro do projeto Producer.
         */
        config.put(
                JacksonJsonSerializer.ADD_TYPE_INFO_HEADERS,
                false
        );

        // Cria o ProducerFactory com todas essas configurações.
        return new DefaultKafkaProducerFactory<>(config);
    }

    // Cria o KafkaTemplate com o ProducerFactory.
    @Bean
    public KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}