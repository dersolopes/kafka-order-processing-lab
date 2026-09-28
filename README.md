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

## Objetivo

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
  "eventId": "e123",
  "eventType": "ORDER_CREATED",
  "occurredAt": "2026-09-28T14:30:00Z",
  "order": {
    "orderId": "8b7f3c21",
    "customerId": "12345",
    "product": "Notebook",
    "quantity": 1,
    "amount": 4500.00
  }
}
```

Esse evento é publicado no topic:

```text
orders.created
```

Os três Consumer Groups recebem o evento independentemente.

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

Essa estrutura permitirá estudar a diferença entre:

* consumidores do mesmo grupo;
* consumidores de grupos diferentes;
* paralelismo;
* distribuição de partitions;
* rebalancing.

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

* [ ] Criar projeto Spring Boot
* [ ] Configurar Maven
* [ ] Criar endpoint `POST /orders`
* [ ] Criar DTOs
* [ ] Gerar Order ID
* [ ] Criar evento

## Phase 2 — Kafka Producer

* [ ] Configurar Kafka
* [ ] Criar topic
* [ ] Configurar KafkaTemplate
* [ ] Publicar evento
* [ ] Configurar JSON serialization
* [ ] Utilizar orderId como key

## Phase 3 — Consumers

* [ ] Processing Consumer
* [ ] Notification Consumer
* [ ] Audit Consumer
* [ ] Consumer Groups
* [ ] JSON deserialization

## Phase 4 — Kafka Advanced

* [ ] Partitions
* [ ] Keys
* [ ] Offsets
* [ ] Consumer Groups
* [ ] Rebalancing
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
* [ ] Docker Compose

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

# Estrutura inicial

```text
kafka-order-processing-lab/
│
├── producer/
│
├── consumer-processing/
│
├── consumer-notification/
│
├── consumer-audit/
│
├── docker/
│
├── k8s/
│
├── docker-compose.yml
│
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
