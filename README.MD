# DeferMQ

DeferMQ — прототип open-source брокера отложенных сообщений. Клиент задаёт точное
`deliver_at`, DeferMQ надёжно сохраняет состояние и payload в PostgreSQL, переносит
ближайшие сообщения в NATS JetStream и доставляет их в HTTP, Kafka, RabbitMQ или
PostgreSQL.

> Статус проекта: **prototype**. API, схема данных и operational defaults могут
> меняться до первого стабильного релиза.

## Гарантии доставки

1. После успешного ответа Gateway delivery и payload зафиксированы в PostgreSQL.
2. PostgreSQL — единственный source of truth. JetStream — восстанавливаемый горячий
   слой и durable ready queue.
3. DeferMQ намеренно не отправляет сообщение раньше `deliver_at`. После этого
   времени возможна положительная задержка из-за нагрузки или недоступности систем.
4. Семантика доставки — **at-least-once**. Повтор возможен, если destination принял
   сообщение, а Pusher завершился до записи результата в PostgreSQL.
5. Стабильный `delivery_id` передаётся downstream как idempotency key.
6. Transactional outbox защищает границу PostgreSQL → NATS; `schedule_revision`
   отсекает устаревшие события после отмены или переноса.
7. Потеря данных NATS восстанавливается overdue reconciliation из PostgreSQL.
   Потеря PostgreSQL без резервной копии необратима.

Exactly-once для произвольных внешних систем не обещается. Получатель должен
дедуплицировать запросы по `delivery_id`.

## Архитектура

```mermaid
sequenceDiagram
    participant C as Client
    participant G as Gateway
    participant PG as PostgreSQL
    participant M as Postgres Manager
    participant JS as NATS JetStream
    participant P as Pusher
    participant D as Destination

    C->>G: POST /v1/messages
    G->>PG: payload + delivery (+ outbox if hot)
    PG-->>G: COMMIT
    G-->>C: 202 Accepted

    loop until hot horizon is drained
        M->>PG: candidates → transactional outbox
    end
    M->>PG: claim outbox
    M->>JS: scheduled/ready publish
    JS-->>M: PubAck
    M->>PG: mark outbox published

    JS->>P: ready event near deliver_at
    P->>PG: claim delivery + load payload
    P->>D: push with delivery_id
    D-->>P: result
    P->>PG: delivered or retry/dead
    P->>JS: ACK
```

Три исполняемых файла имеют разные роли:

- `defermq-gateway` — REST API, валидация, idempotent create, status, отмена и
  перенос. Gateway зависит от PostgreSQL, но принимает сообщения при недоступном
  NATS благодаря outbox.
- `defermq-postgres-manager` — promotion в hot horizon, публикация outbox с
  JetStream PubAck, reconciliation, reaping leases и retention.
- `defermq-pusher` — durable pull consumers, PostgreSQL claim, worker pools,
  destination adapters, application retries и terminal state.

### Hot horizon

Далёкая delivery хранится только в PostgreSQL. Когда
`deliver_at <= database_now + DEFERMQ_HOT_HORIZON`, Manager создаёт outbox и
публикует scheduled event в JetStream. NATS выпускает ready event около
`deliver_at`. Если scheduled event потерян, overdue reconciler создаёт direct
ready event. Payload никогда не передаётся через NATS.

## Требования

- Docker Engine 28+ и Docker Compose v2 для рекомендуемого запуска;
- либо Go 1.26.x, PostgreSQL и NATS Server 2.12+ для локальной разработки;
- Task 3.x (`task`) для developer-команд.

SQL-миграции встроены в бинарники и применяются при
`DEFERMQ_POSTGRES_AUTO_MIGRATE=true`; контейнеру не нужен каталог migrations.

## Быстрый запуск из исходников

```bash
git clone https://github.com/defermq/defermq.git
cd defermq
cp .env.example .env
docker compose -f deploy/compose/docker-compose.build.yml up --build -d
docker compose -f deploy/compose/docker-compose.build.yml ps
```

Core-профиль запускает source PostgreSQL, NATS и три DeferMQ-сервиса. Порты
публикуются только на loopback:

- Gateway: `http://127.0.0.1:8080`;
- Manager admin: `http://127.0.0.1:8081`;
- Pusher admin: `http://127.0.0.1:8082`;
- NATS monitoring: `http://127.0.0.1:8222`.

Остановка сохраняет persistent volumes:

```bash
docker compose -f deploy/compose/docker-compose.build.yml down
```

Для полного удаления локальных данных добавьте `--volumes`.

## Запуск готовых images

В `.env` замените три значения `DEFERMQ_*_IMAGE` на опубликованные образы:

```bash
cp .env.example .env
# DEFERMQ_GATEWAY_IMAGE=registry.example.com/team/defermq-gateway:v0.1.0
# DEFERMQ_POSTGRES_MANAGER_IMAGE=registry.example.com/team/defermq-postgres-manager:v0.1.0
# DEFERMQ_PUSHER_IMAGE=registry.example.com/team/defermq-pusher:v0.1.0
docker compose -f deploy/compose/docker-compose.images.yml pull
docker compose -f deploy/compose/docker-compose.images.yml up -d
```

`docker-compose.images.yml` не содержит `build:` и использует ту же сеть,
конфигурацию, healthchecks и volumes, что build-вариант.

## API

Время принимается в RFC3339/RFC3339Nano и приводится к UTC. Создание возвращает
`202 Accepted`. Подставьте будущее значение `deliver_at`:

```bash
curl --fail-with-body -i http://127.0.0.1:8080/v1/messages \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: invoice-inv-123-overdue-v1' \
  -d '{
    "deliver_at": "2030-07-25T14:00:00Z",
    "max_attempts": 10,
    "destination": {
      "type": "http",
      "http": {
        "url": "https://webhook.example.com/defermq",
        "method": "POST",
        "headers": {"X-Event-Type": "invoice.overdue"}
      }
    },
    "payload": {
      "content_type": "application/json",
      "body": {"invoice_id": "inv-123"}
    }
  }'
```

Повтор с тем же `Idempotency-Key` возвращает существующую delivery и
`Idempotent-Replay: true`. Глобальная уникальность ключа — ограничение прототипа.

```bash
DELIVERY_ID=0198f4f4-1b9e-7d3e-9c65-96c7af740db1
curl --fail-with-body "http://127.0.0.1:8080/v1/messages/${DELIVERY_ID}"

curl --fail-with-body -X PATCH \
  "http://127.0.0.1:8080/v1/messages/${DELIVERY_ID}/schedule" \
  -H 'Content-Type: application/json' \
  -d '{"deliver_at":"2030-07-26T10:00:00Z"}'

curl --fail-with-body -X DELETE \
  "http://127.0.0.1:8080/v1/messages/${DELIVERY_ID}"
```

Перенос и отмена разрешены только для `scheduled`. Обе операции увеличивают
`schedule_revision`, поэтому уже выпущенные NATS events становятся stale.

Для binary payload передайте ровно один `body_base64`:

```json
{
  "payload": {
    "content_type": "application/octet-stream",
    "body_base64": "AAECAwQ="
  }
}
```

### Destination: Kafka

Запустите broker, затем в `.env` установите
`DEFERMQ_KAFKA_ENABLED=true` и добавьте `kafka` в
`DEFERMQ_ENABLED_DESTINATIONS`. Перезапустите Pusher/Gateway.

```bash
docker compose -f deploy/compose/docker-compose.build.yml --profile kafka up --build -d
```

```json
{
  "type": "kafka",
  "kafka": {
    "topic": "billing-events",
    "key": "inv-123",
    "headers": {"event_type": "invoice.overdue"}
  }
}
```

Brokers и TLS/SASL credentials задаются глобально через env. Если key пуст,
используется delivery ID. Producer ожидает подтверждение и передаёт idempotency
metadata в headers.

### Destination: RabbitMQ

Установите `DEFERMQ_RABBIT_ENABLED=true`, добавьте `rabbit` в разрешённые
destinations и запустите профиль:

```bash
docker compose -f deploy/compose/docker-compose.build.yml --profile rabbit up --build -d
```

```json
{
  "type": "rabbit",
  "rabbit": {
    "exchange": "events",
    "routing_key": "billing.invoice.overdue",
    "headers": {"event_type": "invoice.overdue"}
  }
}
```

Pusher отправляет persistent messages и признаёт успех только после publisher
confirm. Demo management UI доступен на `127.0.0.1:15672`.

### Destination: PostgreSQL

Установите `DEFERMQ_TARGET_POSTGRES_ENABLED=true`, добавьте `postgres` в
разрешённые destinations и запустите профиль:

```bash
docker compose -f deploy/compose/docker-compose.build.yml --profile target-postgres up --build -d
```

```json
{
  "type": "postgres",
  "postgres": {
    "channel": "billing",
    "metadata": {"source": "defermq-demo"}
  }
}
```

Adapter выполняет параметризованный `INSERT` в фиксированную таблицу
`DEFERMQ_TARGET_POSTGRES_TABLE` с `ON CONFLICT (delivery_id) DO NOTHING`.
Произвольный SQL от API не принимается.

## Retry semantics

Pusher увеличивает `attempts` во время atomic claim. Network errors, timeout,
HTTP 408/425/429/5xx и временные ошибки adapters повторяются с bounded exponential
backoff и jitter. `Retry-After` для HTTP 429/503 учитывается в пределах max
backoff. Остальные HTTP 4xx и ошибки постоянной конфигурации переводят delivery в
`dead`. Каждый retry получает новую revision и новый `deliver_at`.

JetStream `AckWait` не используется как application scheduler. ACK отправляется
только после commit состояния `delivered`, `scheduled` (retry) или `dead`.
Истёкший processing lease возвращается Manager-ом в очередь.

Downstream получает следующие логические поля:

- `Idempotency-Key` / `X-DeferMQ-Delivery-ID`;
- `X-DeferMQ-Schedule-Revision`;
- `X-DeferMQ-Attempt`;
- `X-DeferMQ-Scheduled-At`.

Kafka и RabbitMQ получают эквивалентные headers; target PostgreSQL имеет
`delivery_id UUID PRIMARY KEY`.

## Health и метрики

У каждого бинаря доступны `GET /livez`, `GET /readyz` и `GET /metrics`.
Liveness не обращается к сети. Readiness проверяет необходимые зависимости:
Gateway — PostgreSQL; Manager — PostgreSQL, NATS и совместимость stream; Pusher —
PostgreSQL, NATS, consumers и включённые глобальные adapters.

Prometheus scrape targets:

```text
127.0.0.1:8080/metrics  # gateway
127.0.0.1:8081/metrics  # manager
127.0.0.1:8082/metrics  # pusher
```

Основные группы метрик:

- Gateway: HTTP rate/latency/size, created/replayed/cancelled/rescheduled;
- Manager: promoter batches, outbox publish, overdue/reaper/retention,
  `unpromoted_headroom_seconds`, pending и oldest outbox;
- Pusher: received/claims/attempts/retries/dead, inflight, start/completion lag,
  early events и consumer pending;
- все сервисы: `defermq_build_info`, dependency readiness, Go/process и pgxpool.

`/metrics` не выполняет SQL: DB/NATS gauges обновляют фоновые collectors.
Практичные alerts: readiness дольше нескольких минут; pending/oldest outbox
растёт; unpromoted headroom приближается к нулю; processing expired > 0; delivery
lag или dead rate растёт; metrics collector success равен 0.

## Конфигурация

Полный каталог переменных с русскими комментариями, форматами и безопасными
production-рекомендациями находится в `.env.example`. Общие значения PostgreSQL,
NATS, subject prefixes, hot horizon и payload limit должны быть согласованы между
тремя процессами. Credentials передавайте через secret manager или mounted files,
а не коммитьте в `.env`.

## Разработка

```bash
task --list
task deps
task fmt
task vet
task test
task test-race
task lint
task build
task test-integration
```

`task lint` ожидает `golangci-lint` в `PATH`; migration tasks — `goose`.
Integration tests могут требовать Docker. Отдельные команды:

- `DEFERMQ_TEST_POSTGRES_DSN` включает repository integration tests;
- `DEFERMQ_NATS_INTEGRATION_URL` включает JetStream scheduling test;
- `DEFERMQ_TEST_KAFKA_*`, `DEFERMQ_TEST_RABBIT_*` и
  `DEFERMQ_TEST_TARGET_POSTGRES_DSN` включают adapter tests;
- `DEFERMQ_E2E_GATEWAY_URL=http://127.0.0.1:8080` включает HTTP E2E для
  запущенных локально Gateway, Manager и Pusher
  (`DEFERMQ_HTTP_ALLOW_PRIVATE_NETWORKS=true`).

```bash
task build-gateway
task build-manager
task build-pusher
task run-gateway
task run-manager
task run-pusher
task migrate-up
task migrate-status
```

Build information (`version`, `commit`, `build time`) передаётся через ldflags и
публикуется в liveness/startup logs/`defermq_build_info`.

## Security

- Demo-пароли и разрешение private HTTP networks небезопасны для production.
  Замените credentials, установите `DEFERMQ_HTTP_ALLOW_PRIVATE_NETWORKS=false`
  и задайте `DEFERMQ_HTTP_ALLOWED_HOSTS`.
- Простой DNS validator не устраняет DNS rebinding полностью. Ограничьте egress
  на уровне сети.
- NATS demo не имеет auth/TLS и публикуется Compose только на loopback. Для
  внешней сети настройте NATS accounts/credentials и TLS.
- Admin endpoints, включая optional pprof, требуют сетевого ACL или reverse proxy
  с аутентификацией. Не публикуйте pprof в Internet.
- Не логируются payload, DSN, credentials и sensitive headers. Всё равно
  контролируйте доступ к stdout и Prometheus.
- Используйте TLS для PostgreSQL, NATS, Kafka, RabbitMQ и HTTP destinations вне
  доверенной локальной сети; регулярно делайте backup source PostgreSQL.
- Контейнеры запускаются non-root, с read-only root filesystem и
  `no-new-privileges`.

## Ограничения прототипа

- Нет пользовательской авторизации, multi-tenancy, quotas, billing и UI.
- Нет recurring/cron messages и массовых кампаний.
- Нет exactly-once для внешних HTTP/Kafka/RabbitMQ; возможен ambiguous success.
- PostgreSQL — единственный cold storage backend и единая точка сохранности.
- Payload целиком читается в память, но ограничен `DEFERMQ_MAX_PAYLOAD_BYTES`.
- Exact DB counts фоновых metrics collectors могут стать дорогими на очень
  больших таблицах.
- HTTP SSRF-защита не является полноценной защитой от DNS rebinding.
- Kafka/RabbitMQ/target PostgreSQL в Compose предназначены для локальной проверки,
  а не production deployment.
- Kubernetes/Helm, distributed tracing, OAuth и dynamic plugins не входят в scope.

## Лицензия

Проект распространяется по лицензии MIT. См. [LICENSE](LICENSE).
