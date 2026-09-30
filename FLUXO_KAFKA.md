# Fluxo da API com Kafka

Este documento explica o fluxo completo da arquitetura orientada a eventos usando Kafka neste projeto de processamento de pedidos.

## Arquitetura Geral

```
┌─────────────────┐       ┌──────────────┐      ┌────────────────────────┐
│  Producer       │─────▶│   Kafka      │────▶ │  Consumer Notification │
│ (Order Service) │       │   Broker     │      │  (order-notification)  │
└─────────────────┘       └──────────────┘      └────────────────────────┘
                                                        │
                                                        │
                                                        ▼
                                                 ┌─────────────────────┐
                                                 │   Consumer Audit    │
                                                 │   (order-audit)     │
                                                 └─────────────────────┘
```

## 1. Producer - Serviço de Pedidos

### Endpoint
- Cliente faz uma requisição POST para criar um pedido

### Processo no `OrderService`

```java
public CreateOrderResponse createOrder(CreateOrderRequest request) {
    // 1. Gera um ID único para o pedido
    String orderId = UUID.randomUUID().toString();

    // 2. Cria o evento de domínio com todos os dados
    OrderCreatedEvent event = new OrderCreatedEvent(
        UUID.randomUUID().toString(), // eventId
        orderId,
        request.customerId(),
        request.product(),
        request.quantity(),
        request.amount(),
        Instant.now() // timestamp
    );

    // 3. Publica o evento no Kafka
    orderEventProducer.publish(event);

    // 4. Retorna imediatamente (processamento assíncrono)
    return new CreateOrderResponse(orderId, OrderStatus.RECEIVED);
}
```

### OrderEventProducer

```java
public void publish(OrderCreatedEvent event) {
    // Envia para o tópico "orders.created"
    // Key = orderId (garante ordem para mesmo pedido)
    // Value = evento completo
    kafkaTemplate.send("orders.created", event.orderId(), event);
}
```

**Características:**
- **Assíncrono**: Retorna imediatamente sem esperar processamento
- **Desacoplado**: Não precisa conhecer os consumidores
- **Fire-and-forget**: Envia e não espera confirmação de processamento

## 2. Kafka Broker

O Kafka atua como intermediário e garante:

- **Persistência**: Mensagens são armazenadas em disco
- **Entrega garantida**: Garante que mensagens não são perdidas
- **Particionamento**: Divide tópicos em partições para paralelismo
- **Serialização**: Converte o evento `OrderCreatedEvent` para JSON

### Tópico: `orders.created`

- Canal onde os eventos de pedido são publicados
- Padrão de nomenclatura: `{entidade}.{ação}`
- Múltiplos consumidores podem se inscrever

## 3. Consumers - Processamento Paralelo

### Consumer Notification
**Grupo:** `order-notification-group`

```java
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
```

**Responsabilidade:** Enviar notificações ao cliente sobre o pedido

### Consumer Audit
**Grupo:** `order-audit-group`

```java
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
```

**Responsabilidade:** Registrar logs de auditoria para compliance

## Conceitos Importantes

### Consumer Groups
- Cada grupo mantém seu próprio offset (posição de leitura)
- Múltiplos consumidores no mesmo grupo dividem o trabalho (load balancing)
- Diferentes grupos recebem cópias da mesma mensagem (pub/sub)

### Particionamento
- Kafka divide tópicos em partições
- A `key` (orderId) determina qual partição a mensagem vai
- Mensagens com mesma key sempre vão para mesma partição
- Garante ordem para eventos do mesmo pedido

### Serialização/Deserialização
- **Producer**: Serializa `OrderCreatedEvent` → JSON
- **Consumer**: Deserializa JSON → `OrderCreatedEvent`
- Usa Jackson para conversão automática

### Configuração de Confiança
```java
// Por segurança, apenas pacotes específicos são permitidos
config.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, 
    "com.kafkaorder.consumer_notification.event");
```

## Vantagens desta Arquitetura

1. **Desacoplamento**: Producer não conhece os consumidores
2. **Escalabilidade**: Adicionar novos consumidores sem modificar o producer
3. **Resiliência**: Kafka persiste mensagens, sobrevive a falhas
4. **Assincronismo**: Resposta rápida ao cliente, processamento em background
5. **Flexibilidade**: Fácil adicionar novos consumidores (ex: pagamento, estoque)
6. **Rastreabilidade**: Cada evento tem ID único e timestamp

## Fluxo Completo Step-by-Step

1. Cliente envia POST `/orders` com dados do pedido
2. `OrderService` gera `orderId` e cria `OrderCreatedEvent`
3. `OrderEventProducer` publica evento no tópico `orders.created`
4. Kafka serializa o evento para JSON e persiste
5. Kafka notifica todos os consumidores inscritos
6. **Consumer Notification** recebe e processa (envia notificação)
7. **Consumer Audit** recebe e processa (registra auditoria)
8. Producer retorna `OrderStatus.RECEIVED` ao cliente
9. Clientes continuam processando em paralelo, independentemente

## Estrutura do Evento

```java
public record OrderCreatedEvent(
    String eventId,      // ID único do evento
    String orderId,      // ID do pedido
    String customerId,   // ID do cliente
    String product,      // Nome do produto
    Integer quantity,    // Quantidade
    BigDecimal amount,   // Valor total
    Instant occurredAt   // Timestamp do evento
) {}
```

## Próximas Extensões Possíveis

- **Consumer Payment**: Processar pagamentos
- **Consumer Inventory**: Atualizar estoque
- **Consumer Analytics**: Gerar relatórios e métricas
- **Dead Letter Queue**: Tratar mensagens com erro
- **Schema Registry**: Validar schema dos eventos
