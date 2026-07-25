# DeferMQ — контекст проекта

## Назначение

DeferMQ — open-source прототип брокера отложенных сообщений на Go.
Клиент создаёт сообщение с точным `deliver_at`, после чего DeferMQ сохраняет
расписание и payload, переносит ближайшие сообщения в горячий слой и доставляет
их во внешнюю систему.

PostgreSQL является единственным source of truth. NATS JetStream используется
как восстанавливаемый горячий scheduler и durable ready queue.

## Гарантии

- Успешный ответ Gateway означает, что сообщение зафиксировано в PostgreSQL.
- Сообщение намеренно не отправляется раньше `deliver_at`.
- После `deliver_at` возможна положительная задержка.
- Семантика доставки — at-least-once.
- Стабильный `delivery_id` передаётся downstream как idempotency key.
- Transactional outbox защищает границу PostgreSQL → NATS.
- `schedule_revision` отсекает устаревшие события после переноса или отмены.
- Processing leases и reconciliation восстанавливают работу после падений.
- Очистка или потеря NATS не приводит к окончательной потере delivery.
- Потеря PostgreSQL без резервной копии означает потерю source of truth.
- Возможна повторная внешняя доставка, если destination принял сообщение,
  а Pusher завершился до фиксации результата в PostgreSQL.

## Технологии

- Go 1.26;
- PostgreSQL и `pgx/v5`;
- NATS Server 2.12+ и современный `nats.go/jetstream`;
- chi;
- zap;
- Prometheus client;
- franz-go;
- amqp091-go.

HTTP API не использует framework кроме chi. Runtime-логи пишутся через zap.
SQL хранится явно, ORM отсутствует.

## Бинарные приложения

### `defermq-gateway`

Публичный REST API:

- создаёт payload и delivery одной PostgreSQL-транзакцией;
- поддерживает idempotency через `Idempotency-Key`;
- возвращает состояние delivery;
- отменяет и переносит запланированные сообщения;
- создаёт outbox сразу для due или попавших в hot horizon сообщений;
- публикует `/livez`, `/readyz` и `/metrics`.

### `defermq-postgres-manager`

Фоновые процессы:

- promoter переносит сообщения в hot horizon;
- outbox workers публикуют события с JetStream PubAck;
- overdue reconciler восстанавливает потерянные пробуждения;
- processing reaper освобождает истёкшие leases;
- retention cleaner удаляет завершённые записи batch-ами;
- manager создаёт и проверяет JetStream stream;
- admin server публикует health и metrics.

### `defermq-pusher`

Durable pull consumers и ограниченные worker pools:

- атомарно claim-ят delivery в PostgreSQL;
- загружают payload после успешного claim;
- поддерживают heartbeat processing lease;
- вызывают HTTP, Kafka, RabbitMQ или PostgreSQL adapter;
- фиксируют `delivered`, планируют retry или переводят delivery в `dead`;
- ACK JetStream выполняется только после PostgreSQL commit.

## Структура репозитория

```text
cmd/
  defermq-gateway/
  defermq-postgres-manager/
  defermq-pusher/

internal/
  api/httpapi/
  app/gateway/
  app/postgresmanager/
  app/pusher/
  buildinfo/
  config/
  delivery/
  domain/
  health/
  hotstorage/natsjs/
  manager/
  observability/
  storage/postgres/

migrations/
configs/
deploy/docker/
deploy/compose/
integration/
```

Локальные сборки трёх бинарей находятся в `build/`.

## Доменные состояния

`DeliveryStatus`:

```text
scheduled
processing
delivered
cancelled
dead
```

Hot/cold состояние не является business status. Регистрация в горячем слое
отражается полем `hot_registered_revision`.

Основные поля delivery:

```text
id
payload_id
idempotency_key
destination_type
destination
deliver_at
status
schedule_revision
hot_registered_revision
attempts
max_attempts
processing_owner
processing_until
last_error
last_attempt_at
delivered_at
cancelled_at
created_at
updated_at
```

Время хранится и передаётся в UTC.

## Destination

Поддерживаются типы:

```text
http
kafka
rabbit
postgres
```

Destination представлен tagged union: заполнена ровно одна секция,
соответствующая `type`.

- HTTP содержит URL, `POST|PUT|PATCH` и headers.
- Kafka содержит topic, optional key и headers; brokers задаются глобально.
- RabbitMQ содержит exchange, routing key и headers; connection задаётся глобально.
- PostgreSQL содержит logical channel и metadata; произвольный SQL не принимается.

Gateway ограничивает доступные типы через
`DEFERMQ_ENABLED_DESTINATIONS`.

## Payload

API принимает один из вариантов:

- JSON в `payload.body`;
- arbitrary bytes в `payload.body_base64`.

Тело хранится в PostgreSQL как `BYTEA`, content type и headers — отдельно.
Размер ограничивается `DEFERMQ_MAX_PAYLOAD_BYTES`, по умолчанию 1 MiB.
Payload body не передаётся через NATS и не записывается в логи.

## PostgreSQL

Основные таблицы:

- `message_payloads`;
- `deliveries`;
- `nats_outbox`.

Основные ограничения:

- глобальный partial unique index на непустой `idempotency_key`;
- scheduler index по `(deliver_at, id)` для `status='scheduled'`;
- processing lease index;
- unique `(delivery_id, schedule_revision, kind)` в outbox.

Outbox kinds:

```text
schedule
ready
```

NATS publish и внешняя доставка не выполняются внутри PostgreSQL transaction.

## Gateway API

```text
POST   /v1/messages
GET    /v1/messages/{id}
PATCH  /v1/messages/{id}/schedule
DELETE /v1/messages/{id}

GET /livez
GET /readyz
GET /metrics
```

`POST /v1/messages` возвращает `202 Accepted`.
Повторный запрос с тем же `Idempotency-Key` возвращает существующую delivery
и header `Idempotent-Replay: true`.

Reschedule разрешён только для `scheduled`, увеличивает revision и сбрасывает
hot registration. Delete является soft cancellation и также увеличивает
revision.

Ошибки имеют единый envelope:

```json
{
  "error": {
    "code": "validation_failed",
    "message": "deliver_at is required",
    "request_id": "...",
    "details": {}
  }
}
```

## Hot horizon и JetStream

Manager выбирает `scheduled` delivery с
`deliver_at <= database_now + hot_horizon`.

Полный promoter batch читается дальше без sleep. Неполный batch завершает
текущий drain cycle до следующего poll interval.

JetStream stream:

```text
name: DEFERMQ
subjects:
  defermq.schedule.*
  defermq.ready.*
storage: file
allow_msg_schedules: true
```

Scheduled subject:

```text
defermq.schedule.<delivery_id>
```

Target ready subject:

```text
defermq.ready.<destination_type>
```

Ready event:

```json
{
  "schema_version": 1,
  "delivery_id": "...",
  "schedule_revision": 1,
  "deliver_at": "2026-07-25T14:00:00Z",
  "destination_type": "http"
}
```

JetStream message ID состоит из delivery ID, revision и kind.
Просроченный schedule outbox публикуется непосредственно в ready subject.

## Pusher и retries

Claim изменяет `scheduled` на `processing`, устанавливает owner/lease и
увеличивает attempts. Claim допускается только для совпадающей revision и уже
наступившего `deliver_at` с малым clock-skew tolerance.

Retryable failure:

1. вычисляется bounded exponential backoff с jitter;
2. status возвращается в `scheduled`;
3. увеличивается `schedule_revision`;
4. задаётся новый `deliver_at`;
5. создаётся новый schedule outbox.

Non-retryable failure или исчерпание attempts переводит delivery в `dead`.

Downstream получает:

```text
Idempotency-Key
X-DeferMQ-Delivery-ID
X-DeferMQ-Schedule-Revision
X-DeferMQ-Attempt
X-DeferMQ-Scheduled-At
```

## Adapters

### HTTP

- общий connection-pooled client;
- обязательные timeouts;
- SSRF-фильтрация private, loopback, link-local и multicast адресов;
- redirects отключены;
- retryable: network errors, timeout, 408, 425, 429 и 5xx;
- остальные 4xx являются non-retryable;
- учитывается bounded `Retry-After`.

### Kafka

- переиспользуемый franz-go producer;
- idempotent writes и подтверждение результата;
- TLS/SASL;
- optional topic allowlist;
- delivery metadata в headers.

### RabbitMQ

- reconnect и channel recreation;
- сериализованный publish;
- persistent messages;
- publisher confirms;
- optional exchange allowlist и mandatory mode.

### PostgreSQL target

Используется отдельный pool и фиксированная валидированная таблица.
Идемпотентность обеспечивается `delivery_id UUID PRIMARY KEY` и
`ON CONFLICT DO NOTHING`.

## Observability

Все сервисы публикуют:

- стандартные Go/process metrics;
- `defermq_build_info`;
- process start time;
- dependency readiness;
- service-specific counters, histograms и gauges;
- pgxpool metrics.

DB-derived gauges собираются фоновыми goroutine, а `/metrics` не выполняет SQL.
Labels имеют ограниченную cardinality и не содержат delivery ID, URL, topic или
error text.

Liveness не выполняет network calls. Readiness проверяет необходимые
dependencies и состояние инициализации компонентов.

## Конфигурация

Конфигурация задаётся environment variables. Полный каталог, значения для
локального demo и комментарии находятся в `.env.example`.

Ключевые общие параметры:

```text
DEFERMQ_POSTGRES_DSN
DEFERMQ_NATS_URL
DEFERMQ_HOT_HORIZON
DEFERMQ_MAX_PAYLOAD_BYTES
DEFERMQ_ENABLED_DESTINATIONS
DEFERMQ_SHUTDOWN_TIMEOUT
```

Секреты и DSN с credentials не записываются в логи.

## Локальный запуск

Из исходников:

```bash
cp .env.example .env
docker compose -f deploy/compose/docker-compose.build.yml up --build
```

С готовыми images:

```bash
cp .env.example .env
docker compose -f deploy/compose/docker-compose.images.yml pull
docker compose -f deploy/compose/docker-compose.images.yml up -d
```

Default Compose запускает PostgreSQL, NATS и три DeferMQ-сервиса.
Kafka, RabbitMQ и target PostgreSQL доступны через profiles.

## Developer commands

Проект использует Task:

```bash
task --list
task deps
task fmt
task vet
task test
task test-race
task test-integration
task build
```

`task build` создаёт:

```text
build/defermq-gateway
build/defermq-postgres-manager
build/defermq-pusher
```

На Windows файлы имеют расширение `.exe`.

## Тесты

- unit tests покрывают validation, payload decoding, backoff, events,
  subjects, adapters, middleware и worker transitions;
- PostgreSQL и NATS integration tests включаются environment variables;
- Kafka, RabbitMQ и target PostgreSQL tests являются opt-in;
- HTTP E2E использует запущенные Gateway, Manager и Pusher.

Переменные test environment описаны в `.env.example` и README.

## Ограничения прототипа

- Нет авторизации, OAuth, billing и UI.
- Нет recurring/cron messages и массовых кампаний.
- Нет exactly-once гарантии для произвольных внешних систем.
- Нет Kubernetes/Helm manifests.
- Payload загружается в память целиком в пределах настроенного лимита.
- Простая DNS-проверка HTTP destination не устраняет DNS rebinding полностью;
  production deployment также ограничивает egress на сетевом уровне.
- PostgreSQL остаётся единственной невосстанавливаемой из NATS копией состояния.
