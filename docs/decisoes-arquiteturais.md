# Decisões Arquiteturais — Saga Orquestrada (Order · Inventory · Payment)

Documento técnico complementar ao [README](../README.md). Cobre: ADRs, riscos mitigados, idempotência, timeouts, Dead-Letter Queues e resiliência (Retry, Circuit Breaker).

---

## 1. Registro de decisões (ADRs)

| ID | Decisão | Alternativa descartada | Motivo |
|---|---|---|---|
| ADR-01 | Saga por **Orquestração** | Coreografia | Compensação ordenada, estado auditável, evolução do fluxo em um ponto (ver README §2). |
| ADR-02 | **Transactional Outbox** em todos os serviços | Dual-write (DB + publish) | Dual-write perde mensagem ou publica sem commit. Outbox torna "estado + mensagem" atômico. Relay por CDC (Debezium) ou *polling publisher*. |
| ADR-03 | Entrega **at-least-once** + **Idempotent Consumer** | Exactly-once do broker | Exactly-once de broker não cobre efeitos em DB externo/gateway. Idempotência na aplicação é a única garantia de ponta a ponta. |
| ADR-04 | Partição por `saga_id` | Partição por serviço | Ordenação total das mensagens de uma saga sem lock distribuído. |
| ADR-05 | Pagamento como **transação-pivô** (reserva → pagamento → confirmação) | Pagamento antes da reserva | Pagar antes exigiria estorno a cada falta de estoque (custo financeiro + taxa de gateway). Reserva é barata e reversível; pagamento é caro e quase irreversível ⇒ executar o passo irreversível o mais tarde possível. |
| ADR-06 | Reserva de estoque com **TTL** (`expires_at`) | Reserva sem expiração | Evita estoque preso se a saga travar; TTL é a rede de segurança final. |
| ADR-07 | Resposta ao cliente `202 Accepted` + consulta/webhook | Request síncrono até o fim | Latência e disponibilidade do gateway externo não podem bloquear a requisição HTTP. |

**Trade-off assumido:** consistência eventual implica janela em que o pedido está `PENDING` e o estoque reservado sem pagamento confirmado. A UI deve refletir o estado `PENDING`.

---

## 2. Modelo de dados de controle

### 2.1 `saga_instance` (orquestrador)

| Coluna | Tipo | Notas |
|---|---|---|
| `saga_id` | UUID PK | = `correlation_id` |
| `order_id` | UUID UNIQUE | 1 saga ativa por pedido |
| `state` | enum | `STOCK_RESERVING`, `PAYMENT_PROCESSING`, `PAYMENT_UNKNOWN`, `ORDER_APPROVING`, `COMPENSATING_STOCK`, `CANCELLING_ORDER`, `COMPLETED`, `COMPENSATED`, `FAILED_MANUAL` |
| `version` | int | **Optimistic locking**: `UPDATE … WHERE saga_id=? AND version=?` |
| `failure_reason` | text | Ex.: `PAYMENT_REJECTED/INSUFFICIENT_FUNDS` |
| `created_at`, `updated_at` | timestamptz | |

### 2.2 `saga_step` (controle de timeout e retry)

| Coluna | Notas |
|---|---|
| `saga_id`, `step` | PK composta (`RESERVE_STOCK`, `PROCESS_PAYMENT`, `APPROVE_ORDER`, `RELEASE_STOCK`, `CANCEL_ORDER`, …) |
| `command_id` | `message_id` **estável** do comando (reenvios reutilizam o mesmo) |
| `status` | `WAITING`, `DONE`, `FAILED`, `TIMED_OUT` |
| `attempt` | contador de tentativas |
| `deadline_at` | instante-limite da resposta |
| `next_retry_at` | backoff calculado |

### 2.3 `processed_messages` (cada serviço, inclusive orquestrador)

| Coluna | Notas |
|---|---|
| `(consumer, message_id)` | **PK** — a violação de unicidade é o mecanismo de deduplicação |
| `processed_at` | retenção ≥ janela máxima de redelivery do broker (sugestão: 7–14 dias; **confiança média**, depende da retenção configurada no broker) |
| `reply_snapshot` | reply já emitido (permite reenviá-lo em duplicatas) |

### 2.4 `outbox` (cada serviço)

`id`, `aggregate_id` (= `saga_id`), `topic`, `payload`, `headers`, `created_at`, `published_at`. Relay publica em ordem por `aggregate_id`.

---

## 3. Matriz de idempotência

Princípio: **toda mensagem pode chegar 0, 1 ou N vezes, atrasada ou fora de ordem; o efeito de negócio deve ocorrer no máximo uma vez.**

| # | Cenário | Quem detecta | Mecanismo | Resultado |
|---|---|---|---|---|
| I1 | Cliente reenvia `POST /orders` (retry, duplo clique) | Order Service | `Idempotency-Key` + `UNIQUE(customer_id, idempotency_key)`; mesma chave ⇒ devolve o pedido existente | 1 pedido, 1 saga |
| I2 | `OrderCreated` entregue 2× | Orchestrator | `processed_messages` + `UNIQUE(order_id)` em `saga_instance` | 1 saga |
| I3 | `ReserveStock` entregue 2× | Inventory | `processed_messages` (mesma TX da baixa) + `UNIQUE(saga_id, sku)` | 1 reserva; duplicata reenvia `StockReserved` do `reply_snapshot` |
| I4 | `ProcessPayment` entregue 2× | Payment | `processed_messages` + `UNIQUE(saga_id)` em `payment` | 1 tentativa de cobrança |
| I5 | Retry HTTP ao gateway após timeout | Gateway | `Idempotency-Key = paymentId` (determinístico: `hash(saga_id, "charge")`) | **0 cobranças duplicadas** |
| I6 | Reply duplicado ao orquestrador | Orchestrator | `processed_messages` + validação `step`/`state` + optimistic lock | Transição aplicada 1× |
| I7 | Reply **atrasado** (ex.: `StockReserved` após saga já em `COMPENSATING`) | Orchestrator | Reply não casa com o estado esperado ⇒ registra, **não avança**, e garante que `ReleaseStock` foi emitido | Estoque liberado, sem estado inconsistente |
| I8 | `ReleaseStock` chega **antes** de `ReserveStock` | Inventory | Grava tombstone `reservation(saga_id, status=CANCELLED)`; `ReserveStock` posterior encontra tombstone ⇒ responde `StockRejected` sem baixar estoque | Sem reserva órfã |
| I9 | `CancelOrder` entregue 2× / após `APPROVED` | Order | Transição só a partir de `PENDING`; a partir de `CANCELLED` é *no-op* idempotente; a partir de `APPROVED` ⇒ erro de domínio → DLQ (não deveria ocorrer; indica bug de orquestração) | Estado do pedido protegido |
| I10 | Orquestrador cai após commitar estado e antes de publicar | Outbox | Relay publica na recuperação (mensagem fica na `outbox`) | Sem perda; possível duplicata ⇒ tratada por I3–I6 |
| I11 | Dois orquestradores processam a mesma saga | Orchestrator | Partição por `saga_id` (1 consumidor ativo) + optimistic lock `version` | Um dos dois falha e reprocessa |

**Regra do Idempotent Consumer (ordem das operações dentro da TX local):**
1. `INSERT processed_messages (consumer, message_id)` — se violar PK ⇒ **rollback + ACK + reenviar reply anterior**.
2. Aplicar efeito de negócio.
3. `INSERT outbox (reply)`.
4. `COMMIT` ⇒ somente depois, ACK ao broker.

---

## 4. Timeouts de rede e a tabela de controle da Saga

O **Timeout Watcher** executa periodicamente (ex.: a cada 1–2 s):

```
SELECT … FROM saga_step
 WHERE status = 'WAITING' AND deadline_at < now()
 ORDER BY deadline_at
 FOR UPDATE SKIP LOCKED  -- múltiplas réplicas sem disputa
 LIMIT <batch>
```

Para cada passo vencido aplica a política do passo:

| Passo | Deadline (sugestão) | Política no vencimento | Esgotadas as tentativas |
|---|---|---|---|
| `RESERVE_STOCK` | 5 s | Reenviar **mesmo `command_id`**; até 3 tentativas, backoff exponencial + jitter | Emitir `ReleaseStock` de segurança (ambíguo se reservou) → `CancelOrder` → `COMPENSATED` |
| `PROCESS_PAYMENT` | 30 s | **Não compensar.** Emitir `GetPaymentStatus` (consulta ao gateway por `Idempotency-Key`); até 5 tentativas | `PAYMENT_UNKNOWN`: mantém reserva até `expires_at`, alerta, reconciliação periódica |
| `APPROVE_ORDER` / `CONFIRM_STOCK` | 10 s | Retry **sem limite lógico** (passo retentável pós-pivô), backoff até teto de 5 min | Após N tentativas ⇒ DLQ + `FAILED_MANUAL` + alerta (nunca compensar após pivô sem decisão operacional) |
| `RELEASE_STOCK` / `CANCEL_ORDER` | 10 s | Retry sem limite lógico (compensação **deve** completar) | DLQ + `FAILED_MANUAL` + alerta de severidade alta |

> Os valores numéricos são parâmetros iniciais plausíveis, **a calibrar** com p95/p99 reais de cada dependência (confiança: média).

### Pagamento aprovado tardiamente (o caso mais perigoso)

Sequência: timeout → saga entra em `PAYMENT_UNKNOWN` → TTL expira → saga compensa (`COMPENSATED`) → gateway confirma `APPROVED` depois. Mitigação: o **Payment Service** executa reconciliação periódica contra o gateway; ao detectar cobrança aprovada cuja saga está `COMPENSATED`, emite **`RefundPayment`** automaticamente e publica `PaymentRefunded` (auditoria). Diagrama: `diagrams/sequence-timeout-recovery.png`.

---

## 5. Resiliência

### 5.1 Retry

| Camada | Estratégia | Parâmetros iniciais |
|---|---|---|
| Consumo de mensagem (erro **transitório**: DB indisponível, timeout) | *Non-blocking retry* com tópicos de espera (`.retry-5s`, `.retry-1m`, `.retry-10m`) para não bloquear a partição | 3 estágios → DLQ |
| Chamada HTTP ao gateway | Retry com backoff exponencial + **jitter** (evita *thundering herd*) | 3 tentativas, base 200 ms, teto 2 s; **somente com `Idempotency-Key`** |
| Reenvio de comando pelo orquestrador | Governado por `saga_step` (§4) | Conforme tabela §4 |

Regra: **só se faz retry de operação idempotente.** Erros de negócio (ex.: `DECLINED`, `INSUFFICIENT_STOCK`) **não** são retentados — viram replies de falha que disparam compensação.

### 5.2 Circuit Breaker (Payment → Gateway)

| Parâmetro | Valor inicial |
|---|---|
| Janela deslizante | 20 chamadas |
| Limiar de falhas (erro/timeout) | ≥ 50 % |
| Chamadas lentas | > 5 s contam como falha |
| Estado ABERTO | 30 s (fail-fast) |
| Estado SEMI-ABERTO | 5 chamadas de prova |
| Timeouts | conexão 1 s · leitura 10 s |

Com o circuito **aberto**: o Payment Service **não** responde `PaymentRejected` (seria falso negativo). Mantém o comando em retry via tópicos de espera/`deadline` da saga; o pedido segue `PENDING`. Isso preserva a distinção *indisponível ≠ recusado*.

Complementos: **Bulkhead** (pool de threads/conexões dedicado ao gateway, para que sua lentidão não esgote recursos do Payment Service) e **Rate limiting** no consumo de `payment.commands` (backpressure).

### 5.3 Dead-Letter Queues

| Item | Definição |
|---|---|
| Topologia | 1 DLQ por tópico de entrada: `inventory.commands.dlq`, `payment.commands.dlq`, `order.commands.dlq`, `saga.replies.dlq` |
| Entra na DLQ | (a) erro **permanente** (payload inválido, violação de contrato, erro de domínio inesperado) — direto, sem retry; (b) erro transitório após esgotar os estágios de retry |
| Metadados obrigatórios | mensagem original intacta + `error_class`, `error_message`, `stack_hash`, `first_failed_at`, `attempts`, `consumer`, `trace_id` |
| Alertas | **Qualquer** mensagem em DLQ de passo de compensação ou pós-pivô = severidade alta (risco financeiro/estoque). Profundidade > 0 por > 5 min em demais filas = alerta |
| Saga associada | O orquestrador observa `saga.replies.dlq` / timeouts e marca a saga `FAILED_MANUAL` para não ficar presa em `WAITING` |
| Reprocessamento | Ferramenta operacional de **replay** após correção (mensagens são idempotentes, então reprocessar é seguro). Replay exige aprovação e é auditado |
| Poison message | Isolada pela DLQ; não bloqueia a partição |

---

## 6. Riscos mitigados

| # | Risco | Impacto | Mitigação | Resíduo |
|---|---|---|---|---|
| R1 | Dual-write (commit sem publicar, ou publicar sem commit) | Saga travada ou estado fantasma | Transactional Outbox (ADR-02) | Latência do relay |
| R2 | Mensagens duplicadas | Estoque baixado 2×, cobrança 2× | Idempotent Consumer + chaves de idempotência (§3) | Retenção de `processed_messages` finita |
| R3 | Mensagens fora de ordem/atrasadas | Estado inconsistente | Partição por `saga_id`, validação de estado, tombstone (I7, I8) | Ordem entre partições diferentes não garantida (não relevante por saga) |
| R4 | **Falta de isolamento** (Sagas são ACD, sem "I") | Leitura de estado intermediário (pedido `PENDING`, estoque reservado) | *Semantic lock* (estado `PENDING`/`RESERVED` explícito), reserva com TTL, UI mostra `PENDING`; *commutative updates* no estoque | Anomalias de leitura toleradas e documentadas |
| R5 | Resultado ambíguo de pagamento (timeout) | Compensar pedido já pago / cobrar sem entregar | *timeout ≠ recusa*, `GetPaymentStatus`, `PAYMENT_UNKNOWN`, reconciliação + `RefundPayment` | Janela curta de inconsistência |
| R6 | Falha de compensação | Estoque preso / pedido inconsistente | Compensações retentáveis sem limite lógico, idempotentes; DLQ + `FAILED_MANUAL` + alerta alto | Requer intervenção humana em falha permanente |
| R7 | Orquestrador como SPOF | Sagas paradas | Réplicas stateless, estado no DB replicado, Outbox, Timeout Watcher com `SKIP LOCKED`; retomada a partir de `saga_step` | Disponibilidade do DB do orquestrador |
| R8 | Orquestrador concentrando regra de negócio | Acoplamento, "deus" | Orquestrador contém só fluxo; regras ficam nos serviços | Disciplina de design/code review |
| R9 | Indisponibilidade/lentidão do gateway | Cascata de falhas | Timeout, Retry com jitter, Circuit Breaker, Bulkhead | Pedidos ficam `PENDING` mais tempo |
| R10 | Poison message | Partição bloqueada | Classificação transitório/permanente + DLQ | — |
| R11 | Evolução de contrato | Quebra entre versões | Campo `version`, mudanças *backward-compatible*, consumidores tolerantes (*tolerant reader*) | Governança de schema (Schema Registry recomendado) |
| R12 | Falta de observabilidade | Diagnóstico impossível | `trace_id`/`saga_id` em logs, métricas (sagas por estado, tempo em estado, nº de compensações, DLQ depth), tracing distribuído | — |
| R13 | Reserva órfã (saga morre) | Estoque indisponível sem venda | TTL (`expires_at`) + job de expurgo | Estoque indisponível até o TTL |

---

## 7. Métricas e alertas (SLIs sugeridos)

| Métrica | Alerta |
|---|---|
| Sagas por estado (`gauge`) | `PAYMENT_UNKNOWN` > 0 por > 10 min; `FAILED_MANUAL` > 0 |
| Tempo em estado (histograma) | p99 `STOCK_RESERVING` > 2× deadline |
| Taxa de compensação | Aumento > 3σ da linha de base (sinal de gateway/estoque degradado) |
| Profundidade de DLQ | Ver §5.3 |
| Estado do Circuit Breaker | ABERTO > 1 min |
| Reservas expiradas sem confirmação | Tendência de alta |

---

## 8. Limitações e itens fora do escopo

- Não há especificação de código, IaC nem escolha final Kafka vs RabbitMQ (ambos atendem; Kafka facilita partição por `saga_id` e replay; RabbitMQ simplifica roteamento de comandos). **Recomendação: Kafka** pelo replay e pela ordenação por chave — confiança média, dependente da competência operacional do time.
- Sagas não oferecem isolamento ACID; as contramedidas de R4 reduzem, não eliminam, anomalias de leitura.
- Parâmetros numéricos (timeouts, janelas, retenção) são pontos de partida; exigem calibração com carga real.
