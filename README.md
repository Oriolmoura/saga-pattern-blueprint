# Saga Orquestrada para o Fluxo de Compra — E-commerce em Microsserviços

> **Blueprint de Arquitetura** — Padrão Saga (Orquestração) para consistência eventual entre `Order`, `Inventory` e `Payment`.
> Escopo: especificação arquitetural. Não há código de aplicação.

---

## 1. Contexto de negócio

Empresa de grande porte migrando um monólito de e-commerce para microsserviços. O fluxo crítico de compra atravessa três domínios, cada um com **base de dados própria** (*database-per-service*):

| Serviço | Responsabilidade | Dado que possui |
|---|---|---|
| **Order Service** | Ciclo de vida do pedido | `orders` |
| **Inventory Service** | Estoque e reservas temporárias | `stock`, `reservation` |
| **Payment Service** | Cobrança junto a gateway financeiro externo | `payment` |

**Problema:** não existe transação ACID cobrindo três bancos independentes (2PC é inviável: bloqueante, acopla disponibilidade dos serviços e o gateway externo não participa de XA). O requisito é que uma falha tardia (ex.: pagamento recusado) **reverta automaticamente** os efeitos anteriores (estoque reservado, pedido criado).

**Solução:** Saga com **transações locais + transações compensatórias**, consistência eventual, mensageria *at-least-once* e consumidores idempotentes.

### Classificação das transações da Saga

| Passo | Transação local | Serviço | Tipo | Compensação |
|---|---|---|---|---|
| T1 | `CreateOrder` (PENDING) | Order | Compensável | `CancelOrder` |
| T2 | `ReserveStock` | Inventory | Compensável | `ReleaseStock` |
| T3 | `ProcessPayment` | Payment | **Pivô** (ponto de não retorno) | `RefundPayment` (apenas aprovação tardia) |
| T4 | `ApproveOrder` + `ConfirmStock` | Order / Inventory | **Retentável** (deve completar) | — (retry até sucesso) |

Ordem das compensações = inversa à execução: `ReleaseStock` → `CancelOrder`.

---

## 2. Decisão: Orquestração vs. Coreografia

**Decisão: Orquestração** — um *Saga Orchestrator* dedicado detém o estado da saga e emite comandos; os serviços de domínio respondem com eventos/replies.

### Matriz de decisão (pesos e notas = julgamento do arquiteto, escala 1–5)

| Atributo de qualidade | Peso | Orquestração | Coreografia | Racional |
|---|:--:|:--:|:--:|---|
| Visibilidade / rastreabilidade do estado | 5 | **5** | 2 | Estado em uma tabela única (`saga_instance`); na coreografia o estado está implícito na sequência de eventos espalhada por N serviços. |
| Tratamento de falha e compensação | 5 | **5** | 3 | Compensação é decisão central, determinística, em ordem inversa; na coreografia cada serviço precisa saber "quem compensa quem". |
| Acoplamento entre serviços | 4 | 3 | **4** | Coreografia: ninguém conhece o fluxo. Orquestração: serviços conhecem apenas o contrato do orquestrador (comandos/replies), **não** uns aos outros. Ausência de dependências cíclicas entre serviços. |
| Complexidade de evolução (novos passos: antifraude, frete) | 4 | **4** | 2 | Novo passo = alterar um orquestrador; na coreografia, alterar assinaturas e ordem de eventos em vários serviços (risco de dependência cíclica). |
| Complexidade de testes | 3 | **4** | 2 | O fluxo é testável isoladamente (máquina de estados). |
| Escalabilidade | 4 | 4 | **4** | Orquestrador é *stateless* (estado no DB), escala horizontalmente com partição por `saga_id`. Sem diferença material. |
| Ponto único de falha / gargalo | 4 | 3 | **4** | Risco real da orquestração; mitigado com N réplicas + estado durável + Outbox. |
| Simplicidade inicial (poucos passos) | 2 | 3 | **4** | Coreografia é mais simples com 2–3 passos. |
| **Total ponderado** (máx. 155) | | **124** | **95** | Σ(peso × nota) |

> Cálculo: Orquestração = 25+25+12+16+12+16+12+6 = **124**; Coreografia = 10+15+16+8+6+16+16+8 = **95**. Pesos e notas são julgamento do arquiteto, não medição: a vantagem é sensível a mudar o peso de "acoplamento" e "ponto único de falha".
> **Leitura honesta:** os pesos refletem o contexto (fluxo financeiro, alta exigência de auditoria). Com 2–3 passos estáveis e sem requisito de auditoria, coreografia seria defensável. Aqui, a necessidade de **compensação ordenada e rastreável** pesa mais que o acoplamento adicional ao orquestrador.

**Riscos assumidos da orquestração e mitigação:** (i) orquestrador como SPOF → instâncias *stateless* + DB replicado; (ii) "orquestrador deus" concentrando regra de negócio → ele só contém **fluxo**, regras de domínio permanecem nos serviços; (iii) acoplamento → contratos versionados.

---

## 3. Arquitetura (resumo)

- **Comunicação:** cliente→Order síncrona (HTTP, resposta `202 Accepted`); todo o resto **assíncrono** via broker (Kafka ou RabbitMQ), chave de partição = `saga_id` (ordenação por saga).
- **Garantia de entrega:** *at-least-once* + **Idempotent Consumer** ⇒ efeito *effectively-once*.
- **Atomicidade DB+mensagem:** **Transactional Outbox** em todos os serviços (elimina *dual-write*).
- **Estado:** `saga_instance` + `saga_step` (com `deadline_at`) no `saga_db` do orquestrador; `Timeout Watcher` varre passos vencidos.
- **Integração externa:** Payment → Gateway com `Idempotency-Key`, timeout, retry com backoff+jitter e Circuit Breaker.
- **Falhas permanentes:** DLQ por tópico + alertas.

### Envelope de mensagem (contrato comum)

| Campo | Descrição |
|---|---|
| `message_id` | UUID estável por mensagem lógica (chave de deduplicação; **não muda entre reenvios**) |
| `saga_id` / `correlation_id` | Identifica a saga (também chave de partição) |
| `causation_id` | `message_id` da mensagem que originou esta |
| `type`, `version` | Ex.: `ReserveStock`, `v1` (evolução de schema compatível) |
| `step`, `attempt` | Passo da saga e nº da tentativa (informativo; não altera `message_id`) |
| `occurred_at`, `trace_id` | Tempo de origem e rastreamento distribuído |
| `payload` | Dados de negócio |

### Catálogo de mensagens

| Mensagem | Tipo | De → Para | Efeito |
|---|---|---|---|
| `OrderCreated` | event | Order → Orchestrator | Inicia a saga |
| `ReserveStock` / `StockReserved` / `StockRejected` | cmd / reply | Orch ↔ Inventory | T2 |
| `ProcessPayment` / `PaymentApproved` / `PaymentRejected` | cmd / reply | Orch ↔ Payment | T3 |
| `GetPaymentStatus` | cmd | Orch → Payment | Reconciliação após timeout |
| `ApproveOrder` / `OrderApproved` | cmd / reply | Orch ↔ Order | T4 |
| `ConfirmStock` / `StockConfirmed` | cmd / reply | Orch ↔ Inventory | T4 |
| `ReleaseStock` / `StockReleased` | cmd / reply | Orch ↔ Inventory | **Compensa T2** |
| `CancelOrder` / `OrderCancelled` | cmd / reply | Orch ↔ Order | **Compensa T1** |
| `RefundPayment` / `PaymentRefunded` | cmd / event | Payment ↔ Gateway | Compensa T3 (aprovação tardia) |

---

## 4. Diagramas

| Arquivo | Conteúdo |
|---|---|
| [`diagrams/saga-architecture-overview.png`](diagrams/saga-architecture-overview.png) | Microsserviços, DBs próprios, broker, orquestrador, DLQ e gateway externo. |
| [`diagrams/sequence-success-flow.png`](diagrams/sequence-success-flow.png) | Fluxo feliz: `OrderCreated` → `ReserveStock` → `ProcessPayment` → `ApproveOrder`/`ConfirmStock` → `COMPLETED`. Mostra TX locais + Outbox em cada passo. |
| [`diagrams/sequence-compensation-flow.png`](diagrams/sequence-compensation-flow.png) | Pagamento recusado após reserva: `PaymentRejected` → `COMPENSATING` → **`ReleaseStock`** → **`CancelOrder`** → `COMPENSATED`. |
| [`diagrams/sequence-timeout-recovery.png`](diagrams/sequence-timeout-recovery.png) | *(extra)* Timeout de rede no pagamento: resultado ambíguo, reconciliação por `GetPaymentStatus`, estorno de aprovação tardia. |
| [`diagrams/saga-state-machine.png`](diagrams/saga-state-machine.png) | *(extra)* Máquina de estados da `saga_instance` com transições, pivô e estado terminal `FAILED_MANUAL`. |

Fontes editáveis em **PlantUML** (`diagrams/*.puml`). Para regenerar:

```bash
java -jar plantuml.jar -tpng -Sdpi=150 diagrams/*.puml
```

---

## 5. Idempotência e timeouts (síntese)

Detalhamento completo em [`docs/decisoes-arquiteturais.md`](docs/decisoes-arquiteturais.md).

| Ameaça | Mecanismo |
|---|---|
| Requisição HTTP duplicada do cliente | `Idempotency-Key` + `UNIQUE(customer_id, idempotency_key)` em `orders` |
| Mensagem duplicada (at-least-once) | `processed_messages(consumer, message_id)` PK, gravada **na mesma TX** do efeito; duplicata ⇒ ACK + reenvio do reply original |
| Cobrança duplicada no gateway | `Idempotency-Key = paymentId` determinístico por saga |
| Reserva duplicada | `UNIQUE(saga_id, sku)` em `reservation` |
| Resposta atrasada / fora de ordem | Orquestrador aceita reply só se `step` e `status` batem com o estado atual; senão registra e descarta |
| `ReleaseStock` chega antes de `ReserveStock` | Tombstone em `reservation(saga_id, CANCELLED)`; reserva tardia é rejeitada |
| Timeout de rede | `saga_step.deadline_at` + Timeout Watcher; política por passo (retry → reconciliação → compensação) |

**Regra-chave:** *timeout ≠ recusa*. Em pagamento, o resultado é ambíguo; compensar às cegas pode liberar estoque de pedido já pago.

---

## 6. Estrutura do repositório

```
.
├── README.md
├── diagrams/
│   ├── saga-architecture-overview.{png,puml}
│   ├── sequence-success-flow.{png,puml}
│   ├── sequence-compensation-flow.{png,puml}
│   ├── sequence-timeout-recovery.{png,puml}
│   └── saga-state-machine.{png,puml}
└── docs/
    └── decisoes-arquiteturais.md
```
#
