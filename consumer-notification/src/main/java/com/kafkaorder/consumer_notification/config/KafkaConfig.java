package com.kafkaorder.consumer_notification.config;

import com.kafkaorder.consumer_notification.event.OrderCreatedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.annotation.EnableKafka;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Bean
    public ConsumerFactory<String, OrderCreatedEvent> consumerFactory() {

        Map<String, Object> config = new HashMap<>();

        // Endereço do Kafka vem do application.properties
        config.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        // Consumer Group também vem do application.properties
        config.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                groupId
        );

        // Permite que erros de desserialização sejam tratados pelo Spring Kafka.
        config.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ErrorHandlingDeserializer.class
        );

        config.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ErrorHandlingDeserializer.class
        );

        // A chave Kafka é uma String (nosso orderId).
        config.put(
                ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS,
                StringDeserializer.class
        );

        // O valor é um JSON que será convertido para OrderCreatedEvent.
        config.put(
                ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS,
                JacksonJsonDeserializer.class
        );

        // Por segurança, aceitamos somente o pacote do nosso evento.
        config.put(
                JacksonJsonDeserializer.TRUSTED_PACKAGES,
                "com.kafkaorder.notification.event"
        );

        // Como o producer não envia mais type-info do Java,
        // informamos qual classe deve representar o JSON recebido.
        config.put(
                JacksonJsonDeserializer.VALUE_DEFAULT_TYPE,
                OrderCreatedEvent.class
        );

        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent>
    kafkaListenerContainerFactory() {

        var factory =
                new ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent>();

        factory.setConsumerFactory(consumerFactory());

        return factory;
    }
}