package com.kafkaorder.consumer_processing.config;

import com.kafkaorder.consumer_processing.event.OrderCreatedEvent;
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

import java.util.HashMap;
import java.util.Map;

/**
 * Configuração do Consumer Kafka para o serviço de processamento de pedidos.
 * 
 * Esta classe define como o application vai consumir mensagens do Kafka,
 * incluindo:
 * - Conexão com o broker Kafka
 * - Desserialização de mensagens (JSON → objetos Java)
 * - Tratamento de erros de desserialização
 * - Configuração do listener container factory
 * 
 * O @KafkaListener nos consumers utiliza essa configuração automaticamente.
 */
@Configuration
public class KafkaConfig {

    // Endereço do broker Kafka (ex: localhost:9092)
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // ID do grupo de consumidores (ex: order-processing-test-group)
    // Consumidores no mesmo grupo dividem as partições entre si
    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    // Define de onde começar a ler quando não há offset salvo
    // - earliest: começa do início do tópico
    // - latest: começa apenas das novas mensagens
    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;

    /**
     * Cria a fábrica de consumidores Kafka.
     * 
     * O ConsumerFactory é responsável por criar instâncias de KafkaConsumer
     * com todas as configurações necessárias para conectar ao broker e
     * desserializar mensagens.
     * 
     * @return ConsumerFactory configurado para consumir OrderCreatedEvent
     */
    @Bean
    public ConsumerFactory<String, OrderCreatedEvent> consumerFactory() {

        // O Map está dizendo como o consumer deve se comportar para conversar com o broker
        Map<String, Object> config = new HashMap<>();

        // Endereço do broker Kafka (onde as mensagens estão armazenadas)
        config.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        // ID do grupo de consumidores
        // - Mesmo grupo: divide as partições (load balancing)
        // - Grupos diferentes: cada um recebe todas as mensagens (pub/sub)
        config.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                groupId
        );

        // Comportamento quando não há offset salvo (primeira vez ou offset deletado)
        config.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                autoOffsetReset
        );

        // Usa ErrorHandlingDeserializer para capturar erros de desserialização
        // Em vez de falhar completamente, ele encapsula o erro e permite tratamento
        config.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ErrorHandlingDeserializer.class
        );

        config.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ErrorHandlingDeserializer.class
        );

        // Desserializador real da chave (String)
        // A chave Kafka é o orderId, que é uma String
        config.put(
                ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS,
                StringDeserializer.class
        );

        // Desserializador real do valor (JSON)
        // O valor é o OrderCreatedEvent em formato JSON
        config.put(
                ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS,
                JacksonJsonDeserializer.class
        );

        // Define a classe Java padrão para desserialização do JSON
        // Quando USE_TYPE_INFO_HEADERS=false, o Jackson usa esta classe
        // para converter o JSON recebido em um objeto Java
        config.put(
                JacksonJsonDeserializer.VALUE_DEFAULT_TYPE,
                OrderCreatedEvent.class
        );

        // Desabilita o uso de headers com type-info do Java
        // - Quando true: o producer envia o nome completo da classe no header
        // - Quando false: o consumer usa VALUE_DEFAULT_TYPE para determinar a classe
        // Isso evita problemas de pacotes diferentes entre producer e consumer
        config.put(
                JacksonJsonDeserializer.USE_TYPE_INFO_HEADERS,
                false
        );

        // Cria e retorna a fábrica de consumidores com todas as configurações
        return new DefaultKafkaConsumerFactory<>(config);
    }

    /**
     * Cria a fábrica de containers de listeners Kafka.
     * 
     * O ConcurrentKafkaListenerContainerFactory é responsável por criar
     * e configurar os listeners anotados com @KafkaListener.
     * 
     * Funcionalidades fornecidas:
     * - Gerenciamento de threads (concorrência)
     * - Tratamento de erros
     * - Acknowledgment manual ou automático
     * - Retry de mensagens com falha
     * 
     * @return Factory configurada para criar listeners de OrderCreatedEvent
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent> kafkaListenerContainerFactory() {

        // Cria a factory com os tipos genéricos (Key=String, Value=OrderCreatedEvent)
        var factory = new ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent>();

        // Associa o ConsumerFactory configurado acima
        // Isso garante que os listeners usem as configurações de conexão
        // e desserialização definidas no consumerFactory()
        factory.setConsumerFactory(consumerFactory());

        return factory;
    }
}