# Тестирование DeferMQ

## Уровни тестирования

Проект использует несколько уровней проверок.

### Unit tests

Unit tests не требуют внешних сервисов и проверяют:

- domain и destination validation;
- parsing конфигурации;
- payload JSON/base64;
- backoff и retry classification;
- NATS ingest/ready subjects и event schema;
- ожидание PubAck перед завершением Gateway publish;
- ingest ACK только после commit PostgreSQL;
- Valkey buckets, leases, reclaim и fencing;
- Gateway middleware и handlers через `httptest`;
- HTTP/Kafka/Rabbit/PostgreSQL adapter logic;
- manager loops;
- pusher claim/ACK/retry/dead orchestration;
- load-test planner, receiver, resource parser и report aggregation.

Запуск:

```bash
task test-unit
```

### Race tests

```bash
task test-race
```

Проверяются worker pools, receiver, collectors и остальные Go packages с
race detector.

### Fault tests

```bash
task fault
```

Это быстрые детерминированные тесты, а не нагрузка и не остановка контейнеров.
Они проверяют важные окна отказов:

- Gateway ждёт JetStream PubAck;
- ingest writer делает ACK только после PostgreSQL commit;
- ошибка commit приводит к NAK и повторной выдаче;
- повтор после commit идемпотентен;
- частичный ready PubAck batch сохраняет успехи и освобождает ошибки для retry;
- Valkey lease выдаётся только одному scheduler, fencing не принимает старый token;
- зависшая Valkey inflight-запись возвращается в schedule;
- PostgreSQL обновляется до удаления Valkey inflight marker.

Команда запускает пакеты `internal/ingest`, `internal/manager`,
`internal/hotstorage/valkey` и `internal/hotstorage/natsjs`.

### Integration tests

Integration tests подключаются переменными окружения:

```text
DEFERMQ_TEST_POSTGRES_DSN
DEFERMQ_NATS_INTEGRATION_URL
DEFERMQ_TEST_KAFKA_BROKERS
DEFERMQ_TEST_KAFKA_TOPIC
DEFERMQ_TEST_RABBIT_URL
DEFERMQ_TEST_RABBIT_EXCHANGE
DEFERMQ_TEST_RABBIT_ROUTING_KEY
DEFERMQ_TEST_TARGET_POSTGRES_DSN
```

Без соответствующей переменной конкретный внешний тест пропускается.

```bash
task test-integration
```

### HTTP E2E

E2E использует запущенные Gateway, Manager, Pusher, PostgreSQL, NATS и Valkey.
Receiver запускается тестом на host, а контейнер Pusher обращается к нему через
`host.docker.internal`.

```bash
task compose-build-up
task test-e2e
```

Проверяются:

- доставка не раньше `deliver_at`;
- retry после HTTP 503;
- системные idempotency headers;
- переход в `delivered`;
- cancellation без внешней доставки.

Параметры:

```text
DEFERMQ_E2E_GATEWAY_URL=http://127.0.0.1:8080
DEFERMQ_E2E_RECEIVER_PUBLIC_HOST=host.docker.internal
```

## Конфигурируемый load/E2E test

Инструмент находится в `tools/loadtest`:

```bash
task load-test
```

Другой config:

```bash
task load-test LOADTEST_CONFIG=configs/loadtest.smoke.yml
```

Общий timeout:

```bash
task load-test LOADTEST_CONFIG=configs/loadtest.example.yml \
  CLI_ARGS="-timeout 30m"
```

Эквивалентный прямой запуск:

```bash
go run ./tools/loadtest \
  -config configs/loadtest.example.yml \
  -timeout 30m
```

Smoke-набор:

```bash
task load-smoke
```

Профиль 1000 create requests/s в течение двух минут:

```bash
task load-1000rps
```

Это 120 000 сообщений и заметная нагрузка. Команда не запускается из
`task full` и должна выполняться только осознанно.

Полный локальный цикл без load test:

```bash
task compose-build-up
task full
```

`task full` последовательно запускает unit, race, fault, core integration и HTTP
E2E. Load test всегда запускается отдельной командой.

## Модель нагрузки

Один YAML может содержать несколько `message_groups`. Каждая группа независимо
описывает момент поступления сообщений, scheduled delay, размер payload и
ошибки destination.

Для каждого сообщения план вычисляется детерминированно по `seed`.

```text
admit_at = test_origin + Normal(admission_offset.mean,
                                admission_offset.stddev)

deliver_at = admit_at + Normal(delivery_delay.mean,
                               delivery_delay.stddev)
```

Результат normal distribution ограничивается `min` и `max`. Одинаковый config,
seed и test origin дают одинаковые относительные смещения и действия.

`admission_offset` моделирует момент попадания сообщения в DeferMQ относительно
начала сценария:

- малый `stddev` создаёт burst;
- большой `stddev` распределяет admission во времени;
- разные mean позволяют создать несколько волн.

`delivery_delay` моделирует расстояние между admission и исходным
`deliver_at`:

- delay меньше hot horizon проверяет immediate schedule registration;
- delay больше hot horizon проверяет cold PostgreSQL schedule и promotion;
- малый `stddev` создаёт delivery burst;
- большой `stddev` создаёт сглаженную очередь.

## Параметры YAML

### Верхний уровень

```yaml
name: mixed-high-load
seed: 20260725
```

- `name` попадает в отчёт;
- `seed` управляет normal distributions и выбором actions.

### Gateway

```yaml
gateway:
  url: http://127.0.0.1:8080
  timeout: 15s
```

`timeout` применяется к одному Gateway request.

### Receiver

```yaml
receiver:
  listen_address: 127.0.0.1:18080
  public_url: http://host.docker.internal:18080
  path: /defermq
```

- `listen_address` — bind на машине, где запущен test tool;
- `public_url` — адрес, доступный Pusher;
- `path` — HTTP destination path.

Для Pusher без Docker `public_url` обычно равен
`http://127.0.0.1:18080`.

На Windows receiver по умолчанию bind-ится только к `127.0.0.1`. Docker Desktop
маршрутизирует `host.docker.internal` к host loopback, поэтому разрешение
Windows Firewall для Public networks не требуется. Bind вида `:18080` или
`0.0.0.0:18080` может вызвать firewall dialog и нужен только при осознанном
доступе с других машин.

### Load orchestration

```yaml
load:
  create_concurrency: 64
  status_concurrency: 32
  poll_interval: 250ms
  await_timeout: 15m
  warmup: 5s
  cooldown: 5s
  early_tolerance: 100ms
```

- `create_concurrency` ограничивает одновременные create/cancel/reschedule;
- `status_concurrency` ограничивает GET status;
- `poll_interval` задаёт частоту проверки незавершённых delivery;
- `await_timeout` ограничивает ожидание terminal statuses;
- `warmup` даёт сервисам выйти на steady state до admission;
- `cooldown` оставляет время для последних samples;
- `early_tolerance` учитывает малую погрешность часов.

### Message group

```yaml
message_groups:
  - name: retry-burst
    count: 10000
    admission_offset:
      mean: 30s
      stddev: 1s
      min: 25s
      max: 35s
    delivery_delay:
      mean: 2m
      stddev: 5s
      min: 90s
      max: 150s
    payload_bytes: 4096
    max_attempts: 8
    fail_first_attempts: 1
    failure_status: 503
    cancel_fraction: 0.01
    reschedule_fraction: 0.05
    reschedule_delay:
      mean: 30s
      stddev: 5s
      min: 10s
      max: 1m
```

- `count` — число создаваемых сообщений;
- `payload_bytes` — размер binary payload;
- `max_attempts` — limit delivery attempts;
- `fail_first_attempts` — первые N attempts получают `failure_status`;
- `failure_status` — HTTP error receiver, обычно 503 или 429;
- `cancel_fraction` — доля сообщений, отменяемых сразу после create;
- `reschedule_fraction` — доля сообщений, получающих PATCH schedule;
- остальные сообщения доставляются с исходным schedule;
- сумма cancel/reschedule fractions не превышает 1.

Для rescheduled сообщения:

```text
new_deliver_at = original_deliver_at + Normal(reschedule_delay.mean,
                                              reschedule_delay.stddev)
```

Отрицательное `reschedule_delay.mean` переносит доставку ближе, положительное —
дальше.

Каждое распределение поддерживает `kind: normal` (по умолчанию) и
`kind: uniform`. Normal использует `mean`, `stddev` и ограничивается
`min`/`max`. Uniform равномерно выбирает значение между обязательными
`min` и `max`; этот режим подходит для устойчивого заданного RPS. Готовый
двухминутный сценарий на 120 000 сообщений находится в
`configs/loadtest.1000rps.yml`.

### Resource sampling

```yaml
resources:
  enabled: true
  sample_interval: 1s
  command_timeout: 10s
  compose_project: defermq
  go_services: [gateway, postgres-manager, pusher]
  non_go_services: [postgres, nats, valkey, kafka, rabbitmq, target-postgres]
```

Sampler использует:

```text
docker ps
docker stats --no-stream
```

Контейнеры фильтруются label
`com.docker.compose.project=<compose_project>`.

Метрики агрегируются в три группы:

1. `go` — Gateway, Manager, Pusher;
2. `non_go` — PostgreSQL, NATS, Valkey и optional destination infrastructure;
3. `all` — сумма двух предыдущих групп.

По каждой группе рассчитываются:

- CPU: mean, p50, p95, p99, max;
- memory: mean, p50, p95, p99, max;
- PIDs: mean, p50, p95, p99, max;
- network RX/TX cumulative maximum;
- block read/write cumulative maximum.

P95/P99 и max используются как burst indicators. Sampling через CLI имеет
ограниченную точность: очень короткий burst между samples может быть не замечен.
Для коротких всплесков уменьшается `sample_interval`, ценой дополнительной
нагрузки Docker Engine.

### Assertions

```yaml
assertions:
  max_create_errors: 0
  max_action_errors: 0
  max_missing: 0
  max_duplicates: 0
  max_early_deliveries: 0
  min_delivery_success_rate: 1
  max_delivery_lag: 10s
```

- `max_create_errors` ограничивает ошибки POST;
- `max_action_errors` ограничивает ошибки cancel/reschedule;
- `max_missing` ограничивает ожидаемые, но не принятые receiver-ом сообщения;
- `max_duplicates` ограничивает повторные успешные внешние доставки;
- `max_early_deliveries` ограничивает ранние attempts;
- `min_delivery_success_rate` задаётся от 0 до 1; 0 отключает threshold;
- `max_delivery_lag=0s` отключает limit, но lag остаётся в отчёте.

Отчёты записываются до проверки assertions, поэтому данные сохраняются и при
проваленном тесте.

### Report

```yaml
report:
  directory: test-results/load
  json_file: report.json
  markdown_file: report.md
  include_resource_samples: false
```

- JSON предназначен для CI и дальнейшего анализа;
- Markdown — для человека и PR artifacts;
- individual delivery IDs не попадают в Markdown;
- raw resource samples включаются в JSON только при
  `include_resource_samples=true`.

## Метрики результата

Основные counters:

- `planned` — сообщений в плане;
- `accepted` — успешных POST;
- `create_errors` — ошибок POST;
- `action_errors` — ошибок cancel/reschedule после успешного POST;
- `expected_deliveries` — accepted за исключением успешно отменённых;
- `delivered_unique` — delivery IDs с успешным HTTP 2xx;
- `delivery_attempts` — все HTTP attempts, включая retryable failures;
- `duplicates` — успешные 2xx после уже зафиксированного 2xx для того же ID;
- `early_deliveries` — attempts раньше scheduled time по timestamps PostgreSQL
  с учётом tolerance;
- `receiver_observed_early` — диагностический результат сравнения с часами
  load generator;
- `missing` — ожидаемые ID без успешного 2xx;
- `cancelled`, `dead` — итоговые Gateway statuses.

Latency distributions:

- create latency;
- delivery lag: `delivered_at - deliver_at` по timestamps PostgreSQL;
- receiver-observed lag:
  `receiver_time - X-DeferMQ-Scheduled-At`, диагностический показатель для
  оценки полного внешнего пути.

Percentiles используют nearest-rank:

```text
index = ceil(percentile * sample_count)
```

Отчёт содержит min, mean, p50, p95, p99 и max.

## Интерпретация duplicates

Retry после фактического HTTP failure не является duplicate:

```text
503 -> 204
```

считается двумя attempts и одной успешной доставкой.

Duplicate — второй успешный результат для одного delivery:

```text
204 -> 204
```

Это соответствует ambiguous external success: destination принял первый push,
но Pusher не смог надёжно зафиксировать результат.

## Рекомендованные сценарии

### Smoke

`configs/loadtest.smoke.yml`:

- десятки сообщений;
- near-term schedules;
- retry;
- cancellation и reschedule;
- resource sampling;
- строгие assertions.

### Sustained throughput

- admission mean около середины тестового окна;
- большой admission stddev;
- delivery stddev сопоставим с admission stddev;
- payload близок к production median;
- длительность не менее 15–30 минут.

### Delivery burst

- admission распределён широко;
- delivery times сконцентрированы малым stddev;
- проверяется Pusher worker pool и ready backlog.

### Admission burst

- admission stddev 100–500 ms;
- delivery delay распределён шире;
- проверяется Gateway/PostgreSQL write path.

### Cold horizon

- delivery delay существенно больше `DEFERMQ_HOT_HORIZON`;
- test timeout включает ожидание promotion;
- проверяются promoter, регистрация в Valkey и headroom.

### Retry storm

- `fail_first_attempts` 1–3;
- HTTP 503 или 429;
- достаточный `max_attempts`;
- test duration учитывает exponential backoff.

## Ограничения измерений

- Host load generator также потребляет CPU и сеть, но не входит в Docker groups.
- Docker CPU может превышать 100% на многоядерной системе.
- Docker memory — usage container cgroup, а не полный host working set.
- CLI sampling менее точен, чем Prometheus/cAdvisor с высокой частотой.
- Network и block I/O в текущем отчёте являются cumulative counters.
- Рассинхронизация часов Windows host и Docker VM влияет только на
  `receiver_observed_lag_ms` и `receiver_observed_early`; основные delivery lag
  и early assertions используют timestamps PostgreSQL.
- `host.docker.internal` является Docker Desktop-specific именем; на Linux
  задаётся доступный контейнерам host address.
- Очень большой `status_concurrency` и малый `poll_interval` сами создают
  заметную GET-нагрузку на Gateway.

## Артефакты CI

Рекомендуемые artifacts:

```text
test-results/load/report.json
test-results/load/report.md
```

Для сравнения запусков используются одинаковые config, seed, Docker resource
limits и начальное состояние volumes. Перед чистым benchmark volumes удаляются
явно, поскольку обычный `compose-down` их сохраняет.
