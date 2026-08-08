# order-service

> Scaffolded from `user-service` (the template repo) per its `TEMPLATE.md`.

## Architecture

`order-service` owns order placement and (in a later session) the order-placement
saga orchestrator:

- Owns the `orders` MongoDB collection.
- Exposes REST for creating and reading orders.
- `POST /orders` persists the order as `PENDING`, writes the first saga command
  to the transactional outbox in the same transaction, and calls
  `OrchestratorService.start(order)` — an explicit stub seam. The real
  orchestrator (state machine, RabbitMQ command/reply consumption, saga-log,
  compensations) is **not implemented in this session** — see
  `src/order-orchestrator/`.
- Status transitions are enforced in one place (`OrdersService`): only
  `PENDING → CONFIRMED` and `PENDING → CANCELLED` are legal; anything else is
  rejected with `409 Conflict`.

### What this service owns

| Resource | Type | Notes |
|----------|------|-------|
| `orders` | MongoDB collection | order documents; status machine enforced in `OrdersService` |
| `outbox` | MongoDB collection | transactional outbox rows; marked `published: true` after delivery |
| `saga_log` | MongoDB collection | **owned by the order-orchestrator (next session)** — schema stubbed only |
| `domain.events` | RabbitMQ exchange (topic) | orchestrator will publish `order.confirmed` / `order.cancelled` here (next session) |

### Request flow

```
Client → POST /orders
         ↓
OrdersController   (validation only)
         ↓
OrdersService       (business rules: total computation, status-transition guard)
         ↓                              ↓
OrdersRepository    OutboxService.writeInTx (same Mongo transaction)
         ↓
MongoDB orders collection        →  OrchestratorService.start(order)  [stub — TODO next session]
```

### Not in this session

- `src/order-orchestrator/` is an empty module — `OrchestratorService.start()` only logs
  a TODO. The real saga state machine, RabbitMQ command dispatch, reply consumption,
  compensations, and `saga_log` persistence are built in the next session.
- `src/order-orchestrator/schemas/saga-log.schema.ts` is an empty stub schema — TODO,
  owned by the orchestrator work.

---

## Running locally

### Prerequisites

```bash
# 1. Build the shared contracts package (provides DTOs + types)
cd ../contracts && npm install && npm run build && cd -

# 2. Install service dependencies
npm install

# 3. Copy env and start the Mongo + Redis + RabbitMQ compose stack
cp .env.example .env
docker compose -f ../platform-infra/docker-compose.yml up -d
```

### Start in dev mode

```bash
npm run start:dev
# Service listens on http://localhost:3000
# Swagger UI at    http://localhost:3000/api
```

### Build

```bash
npm run build
# Output in dist/
```

### Tests

```bash
# Unit tests (no external deps — repository is mocked)
npm test

# E2E tests (uses mongodb-memory-server — no compose required)
npm run test:e2e
```

### Generate openapi.yaml

```bash
# MongoDB must be reachable at MONGO_URI
npm run generate:openapi
```

### curl round-trip

```bash
BASE=http://localhost:3000

# Create
curl -s -X POST $BASE/orders \
  -H 'Content-Type: application/json' \
  -d '{"userEmail":"alice@example.com","items":[{"sku":"WIDGET-1","qty":2,"price":9.99}]}' | jq

# Get
curl -s $BASE/orders/<id> | jq

# List by user
curl -s "$BASE/orders?userEmail=alice@example.com&page=1&limit=10" | jq
```

### Verifying the transactional outbox

Every `POST /orders` atomically writes a row to the `outbox` MongoDB collection inside
the same transaction as the order insert (the first saga command, currently a
placeholder routing key until the orchestrator session defines the real saga command
set). The outbox publisher picks it up via a change stream and delivers it to
RabbitMQ, then marks it `published: true`.

**1. Check MongoDB**

```bash
mongosh "mongodb://localhost:27017/orders?replicaSet=rs0&directConnection=true" \
  --eval 'db.outbox.find({}, {routingKey:1, published:1, publishedAt:1}).pretty()'
```

**2. Check the service logs**

```
Outbox row published  { messageId: '...', type: 'order.saga.start-requested' }
```

If you see `Outbox publisher disabled via OUTBOX_PUBLISHER_ENABLED` on startup, set
`OUTBOX_PUBLISHER_ENABLED=true` in `.env`.
