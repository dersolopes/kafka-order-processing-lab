# Kafka Order Processing Lab

Laboratório prático para estudo de **Java, Spring Boot, Apache Kafka, Docker, Observabilidade, Kubernetes, Autoscaling e AWS**.

O projeto simula o processamento de pedidos utilizando uma arquitetura orientada a eventos.

A aplicação é composta por um Producer e três Consumers independentes:

```text
                         ┌──────────────────┐
                         │   Producer API   │
                         │   Spring Boot    │
                         └────────┬─────────┘
                                  │
                                  │ ORDER_CREATED
                                  ▼
                         ┌────────────────┐
                         │     Kafka      │
                         │ orders.created │
                         └───────┬────────┘
                                 │
                ┌────────────────┼────────────────┐
                │                │                │
                ▼                ▼                ▼
        ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
        │ Processing   │ │ Notification │ │    Audit     │
        │ Consumer     │ │ Consumer     │ │   Consumer   │
        └──────────────┘ └──────────────┘ └──────────────┘
```

---

# Objetivo

O objetivo não é criar um sistema comercial completo.

O objetivo é construir um laboratório que permita compreender, na prática:

* Apache Kafka
* Producers
* Consumers
* Topics
* Partitions
* Consumer Groups
* Offsets
* Keys
* Retry
* Dead Letter Topics
* Event-driven architecture
* Observabilidade
* Docker
* Kubernetes
* Horizontal Scaling
* Autoscaling
* Kafka Consumer Lag
* AWS

---

# Arquitetura

## Microservices

O projeto possui quatro aplicações Spring Boot:

### Producer

Responsável por receber pedidos via REST e publicar eventos no Kafka.

```text
POST /orders
```

### Consumer Processing

Responsável pelo processamento do pedido.

Consumer Group:

```text
order-processing-group
```

### Consumer Notification

Responsável por simular a notificação do cliente.

Consumer Group:

```text
order-notification-group
```

### Consumer Audit

Responsável pelo registro de auditoria.

Consumer Group:

```text
order-audit-group
```

---

# Fluxo

Uma requisição:

```http
POST /orders
```

Exemplo:

```json
{
  "customerId": "12345",
  "product": "Notebook",
  "quantity": 1,
  "amount": 4500.00
}
```

gera um evento:

```json
{
  "eventId": "e123456",
  "orderId": "8b7f3c21",
  "customerId": "12345",
  "product": "Notebook",
  "quantity": 1,
  "amount": 4500.00,
  "occurredAt": "2026-09-28T14:30:00Z"
}
```

Esse evento é publicado no topic:

```text
orders.created
```

Os três Consumer Groups recebem o evento independentemente.

---

# Kafka

O Kafka utilizado no laboratório roda localmente através do Docker Compose.

```text
Producer
    │
    ▼
orders.created
    │
    ├── Partition 0
    ├── Partition 1
    └── Partition 2
```

O topic possui inicialmente **3 partitions**.

O `orderId` é utilizado como **Kafka key**:

```text
KafkaTemplate.send(
    topic,
    event.orderId(),
    event
);
```

Isso permite estudar a relação entre:

```text
Key
 ↓
Partition
 ↓
Ordering
```

A ordenação é garantida dentro de uma partition, e não entre todas as partitions do topic.

---

# Consumer Groups

```text
orders.created
       │
       ├── order-processing-group
       │
       ├── order-notification-group
       │
       └── order-audit-group
```

Essa estrutura permite estudar a diferença entre:

* consumidores do mesmo grupo;
* consumidores de grupos diferentes;
* paralelismo;
* distribuição de partitions;
* rebalancing;
* offsets;
* consumer lag.

Cada partition pode ser atribuída a apenas um consumer dentro de um determinado Consumer Group.

Por exemplo:

```text
3 partitions + 3 consumers

P0 → Consumer A
P1 → Consumer B
P2 → Consumer C
```

Com apenas dois consumers:

```text
3 partitions + 2 consumers

P0 → Consumer A
P1 → Consumer A
P2 → Consumer B
```

Com quatro consumers:

```text
3 partitions + 4 consumers

P0 → Consumer A
P1 → Consumer B
P2 → Consumer C
P3 → não existe

Consumer D → idle
```

---

# Offsets e Consumer Lag

O laboratório também utiliza os comandos nativos do Kafka para observar offsets e lag.

```text
LAG = LOG-END-OFFSET - CURRENT-OFFSET
```

Exemplo:

```text
CURRENT-OFFSET = 23
LOG-END-OFFSET = 27

LAG = 4
```

Quando um Consumer Group para de consumir, novas mensagens continuam sendo gravadas no Kafka e o lag aumenta.

Quando o consumer volta a processar, ele avança o offset e o lag diminui.

---

# Experimento: auto.offset.reset

Foi criado um Consumer Group temporário:

```text
order-processing-test-group
```

com:

```properties
spring.kafka.consumer.auto-offset-reset=earliest
```

Como o grupo não possuía offsets previamente registrados, o Kafka encontrou:

```text
Found no committed offset
```

e iniciou as partitions em:

```text
offset=0
```

Isso demonstrou na prática o comportamento de:

```text
auto.offset.reset=earliest
```

A diferença entre `earliest` e `latest` será explorada novamente em experimentos futuros.

---

# Serialização e desserialização

O Producer publica o evento como JSON utilizando `JacksonJsonSerializer`.

Os Consumers utilizam `JacksonJsonDeserializer`.

O projeto evita depender diretamente da classe Java existente no Producer para representar o evento.

Conceitualmente:

```text
Producer Java Object
        │
        ▼
       JSON
        │
        ▼
      Kafka
        │
        ▼
       JSON
        │
        ▼
Consumer Java Object
```

O objetivo é tratar o evento como um contrato de integração entre os serviços, e não como uma dependência direta entre classes Java de diferentes aplicações.

---

# Comandos Kafka úteis

## Listar Consumer Groups

```bash
docker exec -it kafka-order-processing \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list
```

## Ver offsets e lag de um Consumer Group

```bash
docker exec -it kafka-order-processing \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-processing-group
```

## Descrever um Topic

```bash
docker exec -it kafka-order-processing \
  /opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic orders.created \
  --bootstrap-server localhost:9092
```

## Listar Topics

```bash
docker exec -it kafka-order-processing \
  /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

## Consumir mensagens diretamente pelo Kafka

```bash
docker exec -it kafka-order-processing \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders.created \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true
```

---

# Tecnologias

## Backend

* Java
* Spring Boot
* Spring Kafka
* Maven

## Mensageria

* Apache Kafka

## Containerização

* Docker
* Docker Compose

## Observabilidade

* Spring Boot Actuator
* Micrometer
* OpenTelemetry

## Orquestração

* Kubernetes

## Autoscaling

* Kubernetes HPA
* KEDA

## Cloud

* AWS
* Amazon ECR
* Amazon EKS
* CloudWatch

---

# Roadmap

## Phase 1 — Producer

* [x] Criar projeto Spring Boot
* [x] Configurar Maven
* [x] Criar endpoint `POST /orders`
* [x] Criar DTOs
* [x] Gerar Order ID
* [x] Criar evento

## Phase 2 — Kafka Producer

* [x] Configurar Kafka
* [x] Criar topic
* [x] Configurar KafkaTemplate
* [x] Publicar evento
* [x] Configurar JSON serialization
* [x] Utilizar orderId como key

## Phase 3 — Consumers

* [x] Processing Consumer
* [x] Notification Consumer
* [x] Audit Consumer
* [x] Consumer Groups
* [x] JSON deserialization

## Phase 4 — Kafka Advanced

* [x] Partitions
* [x] Keys
* [x] Offsets
* [x] Consumer Groups
* [x] Rebalancing
* [ ] Retry
* [ ] Dead Letter Topic

## Phase 5 — Observability

* [ ] Actuator
* [ ] Micrometer
* [ ] Structured Logs
* [ ] OpenTelemetry
* [ ] Trace ID
* [ ] Metrics

## Phase 6 — Docker

* [ ] Dockerfile Producer
* [ ] Dockerfile Processing
* [ ] Dockerfile Notification
* [ ] Dockerfile Audit
* [x] Docker Compose

## Phase 7 — Kubernetes

* [ ] Pods
* [ ] Deployments
* [ ] Services
* [ ] ConfigMaps
* [ ] Secrets
* [ ] Readiness
* [ ] Liveness
* [ ] Replicas

## Phase 8 — Autoscaling

* [ ] HPA
* [ ] CPU-based scaling
* [ ] Memory-based scaling
* [ ] Kafka Consumer Lag
* [ ] KEDA

## Phase 9 — AWS

* [ ] Docker image no ECR
* [ ] Deploy na AWS
* [ ] Estudar EKS
* [ ] Observabilidade AWS
* [ ] Avaliar Kafka gerenciado

---

# Estrutura

```text
kafka-order-processing-lab/
│
├── producer/
│
├── consumer-processing/
│
├── consumer-notification/
├── consumer-audit/
│
├── docker/
│
├── k8s/
│
├── docker-compose.yml
├── README.md
└── .gitignore
```

---

# Princípio do projeto

O projeto será desenvolvido incrementalmente.

Não serão adicionadas tecnologias apenas para aumentar a quantidade de ferramentas.

Cada tecnologia deverá responder a uma necessidade arquitetural.

Exemplo:

```text
Kafka
→ comunicação assíncrona

Consumer Groups
→ processamento paralelo

Keys
→ direcionamento para partitions

Offsets
→ posição de consumo

Consumer Lag
→ medir mensagens pendentes

Retry/DLT
→ resiliência

Observabilidade
→ entender o comportamento do sistema

Kubernetes
→ orquestração

HPA/KEDA
→ elasticidade

AWS
→ execução em cloud
```

---

# Objetivo final

Ao final do laboratório, o sistema deverá representar aproximadamente:

```text
                           AWS
                            │
                         Kubernetes
                            │
                  ┌─────────┼─────────┐
                  │         │         │
                API       Kafka    Observability
                  │         │
                  │    ┌────┼────┐
                  │    │    │    │
                  │    ▼    ▼    ▼
                  │   Proc Notif Audit
                  │
                  └───────────────┐
                                  │
                              Autoscaling
                                  │
                             Kafka Lag
```

O objetivo final é compreender não apenas como utilizar cada tecnologia isoladamente, mas como elas se relacionam dentro de uma arquitetura distribuída.
