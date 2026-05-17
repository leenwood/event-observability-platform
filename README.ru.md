# Event Observability Platform

[🇬🇧 English](README.md)

Производственно-ориентированный Go-сервис для приёма вебхуков, асинхронной обработки событий
и аналитики. Демонстрирует реальные паттерны наблюдаемости, распределённых систем и пайплайнов данных.

---

## Что демонстрирует репозиторий

- **OpenTelemetry tracing** (распределённая трассировка) — сквозное распространение трейсов через HTTP-сервер и асинхронные воркеры
- **Prometheus metrics** — RED-метрики, отставание очереди, счётчики обработки событий
- **Structured logging** (структурированное логирование) — JSON-логи с `request_id`, `trace_id` на каждой строке
- **Idempotent webhook processing** (идемпотентная обработка) — дубликаты запросов безопасно поглощаются без повторной обработки
- **Kafka-based event pipeline** — развязка приёма и обработки с семантикой retry и DLQ (dead letter queue, очередь неудачных сообщений)
- **ClickHouse analytics** — высокопроизводительное хранение событий с агрегацией через материализованные представления
- **Circuit breaker + retry** (предохранитель + повторные попытки) — устойчивый HTTP-клиент для внешних интеграций
- **Graceful shutdown** (корректное завершение) — активные запросы и коммиты консьюмера отправляются перед остановкой

---

## Обзор архитектуры

```
                        ┌─────────────────────────────────────┐
                        │           HTTP Server                │
                        │  POST /webhooks/events               │
                        │  GET  /health  GET /ready            │
                        │  GET  /metrics GET /analytics/...    │
                        └────────────┬────────────────────────┘
                                     │ 1. валидация + дедупликация
                                     │ 2. сохранение (PostgreSQL)
                                     │ 3. публикация
                                     ▼
                        ┌────────────────────────┐
                        │    Kafka / Redpanda     │
                        │  topic: events          │
                        │  topic: events.retry    │
                        │  topic: events.dlq      │
                        └────────────┬────────────┘
                                     │ consume
                                     ▼
                        ┌────────────────────────┐
                        │    Async Worker         │
                        │  - обработка события    │
                        │  - обновление PostgreSQL│
                        │  - запись в ClickHouse  │
                        │  - retry при ошибке     │
                        │  - DLQ после max retry  │
                        └────────────────────────┘
                                     │
               ┌─────────────────────┴──────────────────┐
               ▼                                         ▼
  ┌────────────────────┐                  ┌──────────────────────┐
  │     PostgreSQL     │                  │      ClickHouse       │
  │  events            │                  │  events_log           │
  │  idempotency_keys  │                  │  daily_event_stats MV │
  └────────────────────┘                  └──────────────────────┘

  Стек наблюдаемости:
  Prometheus ← scrapes /metrics
  Grafana    ← дашборды из Prometheus
  OTLP       ← трейсы от сервера и воркера
```

---

## Жизненный цикл запроса

1. Клиент отправляет `POST /webhooks/events` с полем `idempotency_key` в теле
2. Middleware присваивает `request_id` (UUID) и открывает OTel span
3. Хендлер валидирует поля запроса
4. Хранилище idempotency key проверяет, был ли ключ уже обработан — при дубликате возвращает закешированный ответ
5. Событие сохраняется в PostgreSQL со статусом `pending`
6. Событие публикуется в Kafka topic `events`
7. Хендлер возвращает `202 Accepted` с `event_id` и `trace_id`
8. Воркер читает из Kafka, переводит событие в статус `processing`
9. При успехе: статус → `processed`, строка записывается в ClickHouse
10. При ошибке: retry до N раз, затем сообщение уходит в `events.dlq`

---

## Наблюдаемость

### Логи

Все логи в формате JSON. Каждая строка запроса содержит:
```json
{"time":"...","level":"INFO","msg":"request","method":"POST","path":"/webhooks/events",
 "status":202,"latency":"1.2ms","request_id":"uuid","trace_id":"otel-trace-id"}
```

### Трейсы

OpenTelemetry spans покрывают:
- HTTP-хендлер (корневой span)
- Запросы к PostgreSQL
- Публикацию в Kafka / чтение из Kafka
- Запись в ClickHouse

### Метрики

| Метрика | Тип | Лейблы |
|---|---|---|
| `http_requests_total` | Counter | method, path, status |
| `http_request_duration_seconds` | Histogram | method, path |
| `events_processed_total` | Counter | source, status |
| `events_failed_total` | Counter | source, reason |
| `events_duplicate_total` | Counter | — |
| `queue_lag_messages` | Gauge | topic |

---

## Аналитика с ClickHouse

Обработанные события записываются в `events_log` (ReplacingMergeTree).
Материализованное представление `daily_event_stats` агрегирует счётчики по дате, источнику и типу события.

Запрос через API:
```
GET /analytics/daily-events?from=2024-01-01&to=2024-01-31
```

---

## Идемпотентность и повторные попытки

**Idempotency key (ключ идемпотентности):** каждый вебхук должен содержать `idempotency_key`. Платформа хранит обработанные ключи с настраиваемым TTL (по умолчанию 24 часа). Повторные запросы в рамках TTL-окна возвращают исходный ответ без побочных эффектов.

**Retry (повторные попытки):** неудачные сообщения из Kafka повторно публикуются в `events.retry` с увеличенным счётчиком попыток. После `KAFKA_MAX_RETRIES` попыток сообщение перемещается в `events.dlq`.

**Ограничение exactly-once:** платформа обеспечивает at-least-once (доставку как минимум один раз). Idempotency key предотвращает двойную обработку на уровне приложения, но транзакционной гарантии между записью в PostgreSQL и публикацией в Kafka нет. Сбой между этими двумя шагами оставит событие в статусе `pending` и потребует реконсиляции. Это осознанный компромисс — настоящий exactly-once требует паттерна transactional outbox.

---

## Локальный запуск

**Требования:** Docker, Docker Compose, Go 1.22+, [go-task](https://taskfile.dev/installation/)

```bash
# Установить go-task (однократно)
go install github.com/go-task/task/v3/cmd/task@latest

# 1. Клонировать репозиторий
git clone https://github.com/leenwood/event-observability-platform
cd event-observability-platform

# 2. Скопировать конфиг окружения
cp .env.example .env

# 3. Запустить инфраструктуру
task docker:up

# 4. Применить миграции
task migrate:up

# 5. Запустить сервер
task run
```

Сервисы доступны локально:
| Сервис | URL |
|---|---|
| API-сервер | http://localhost:8080 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin/admin) |
| Redpanda Console | http://localhost:18082 |
| ClickHouse HTTP | http://localhost:8123 |

---

## Примеры API

```bash
# Healthcheck
curl http://localhost:8080/health

# Readiness probe
curl http://localhost:8080/ready

# Отправить вебхук-событие
curl -X POST http://localhost:8080/webhooks/events \
  -H "Content-Type: application/json" \
  -d '{
    "idempotency_key": "order-shipped-12345",
    "source": "shopify",
    "event_type": "order.shipped",
    "payload": {"order_id": "12345", "tracking": "1Z999AA1"}
  }'

# Дубликат — возвращает тот же ответ, повторная обработка не происходит
curl -X POST http://localhost:8080/webhooks/events \
  -H "Content-Type: application/json" \
  -d '{
    "idempotency_key": "order-shipped-12345",
    "source": "shopify",
    "event_type": "order.shipped",
    "payload": {"order_id": "12345", "tracking": "1Z999AA1"}
  }'

# Аналитический запрос
curl "http://localhost:8080/analytics/daily-events?from=2024-01-01&to=2024-01-31"

# Prometheus-метрики
curl http://localhost:8080/metrics
```

---

## Команды Taskfile

Запустите `task --list`, чтобы увидеть все доступные команды.

| Команда | Описание |
|---|---|
| `task build` | Собрать бинарники сервера и воркера |
| `task build:server` | Собрать только сервер |
| `task build:worker` | Собрать только воркер |
| `task run` | Запустить HTTP-сервер локально |
| `task run:worker` | Запустить асинхронный воркер |
| `task test` | Запустить юнит-тесты с race detector |
| `task test:integration` | Запустить интеграционные тесты (требует Docker) |
| `task test:cover` | Тесты с HTML-отчётом покрытия |
| `task lint` | Запустить golangci-lint |
| `task vet` | Запустить go vet |
| `task fmt` | Форматировать код (gofmt + goimports) |
| `task docker:up` | Запустить все контейнеры инфраструктуры |
| `task docker:down` | Остановить контейнеры |
| `task docker:reset` | Остановить контейнеры и удалить тома |
| `task docker:logs` | Стримить логи контейнеров |
| `task migrate:up` | Применить все миграции |
| `task migrate:down` | Откатить все миграции |
| `task migrate:create -- <name>` | Создать пару файлов новой миграции |
| `task deps` | Скачать и привести в порядок зависимости |

---

## Тестирование

Юнит-тесты покрывают: хранилище idempotency key, логику retry/backoff, state machine circuit breaker,
хендлер вебхуков (table-driven с `httptest`) и построитель аналитических запросов.

Интеграционные тесты используют `testcontainers-go` для запуска реальных PostgreSQL и ClickHouse-контейнеров,
проверяя полный путь: webhook → БД → воркер → ClickHouse.

```bash
task test
task test:integration
```

---

## Возможные улучшения для продакшена

- **Transactional outbox** — записывать событие в таблицу `outbox` в рамках одной PostgreSQL-транзакции, а отдельный relay-процесс публикует её в Kafka. Устраняет окно рассинхронизации между записью и публикацией.
- **Schema registry** — принудительная валидация Avro/Protobuf-схем на Kafka-топиках, предотвращает дрейф формата.
- **Rate limiting** — лимиты на входящие вебхуки по источнику для защиты от всплесков нагрузки.
- **Alerting rules** — Prometheus-алерты на отставание очереди выше порога, всплеск ошибок, рост DLQ.
- **Horizontal scaling** — HTTP-сервер без состояния масштабируется тривиально; воркер масштабируется по количеству партиций Kafka.
- **Key rotation** — хранилище idempotency key на базе Redis с TTL для распределённых инсталляций.
- **Audit log** — append-only таблица в PostgreSQL, фиксирующая каждый переход статуса события.
