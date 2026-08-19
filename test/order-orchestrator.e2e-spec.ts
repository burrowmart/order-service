/**
 * order-orchestrator integration tests — real Mongo replica set + real
 * RabbitMQ + real Redis (same infra as outbox.e2e-spec.ts; this config
 * reuses its global-setup/teardown).
 *
 * Prerequisites:
 *   docker compose -f ../platform-infra/docker-compose.yml up -d redis rabbitmq
 *
 * catalog-service and payment-service are stubbed: this suite binds its own
 * queues to the SAGA_COMMANDS exchange to capture the orchestrator's
 * commands, and publishes canned replies to the SAGA_REPLIES exchange in
 * their place — exercising the real OrchestratorService, SagaLogRepository,
 * OutboxService, SagaRepliesConsumerService and Order status writes exactly
 * as they'd run against real participants.
 */
import { INestApplication } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { getConnectionToken, getModelToken } from '@nestjs/mongoose';
import { Connection, Model } from 'mongoose';
import * as amqplib from 'amqplib';
import {
  createEnvelope,
  EXCHANGES,
  ROUTING_KEYS,
  type InventoryReservedPayload,
  type InventoryRejectedPayload,
  type PaymentSucceededPayload,
  type PaymentFailedPayload,
  type ReserveInventoryPayload,
  type ChargePayload,
  type ReleaseInventoryPayload,
  type OrderConfirmedPayload,
  type OrderCancelledPayload,
} from '@demo/contracts';
import { AppModule } from '../src/app.module';
import { OrdersService } from '../src/orders/orders.service';
import { OrderEntity, OrderDocument } from '../src/orders/schemas/order.schema';
import { SagaLogEntity, SagaLogDocument } from '../src/order-orchestrator/schemas/saga-log.schema';
import { OutboxEntity, OutboxDocument } from '../src/common/outbox/schemas/outbox.schema';
import { ProcessedMessageEntity, ProcessedMessageDocument } from '../src/common/outbox/schemas/processed-message.schema';

const AMQP_URL = process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672';
const delay = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function consumeOne(conn: amqplib.ChannelModel, queue: string, timeoutMs = 10_000): Promise<any> {
  const ch = await conn.createChannel();
  try {
    return await new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`Timeout waiting on ${queue}`)), timeoutMs);
      void ch.consume(
        queue,
        (msg) => {
          if (!msg) return;
          clearTimeout(timer);
          ch.ack(msg);
          resolve(JSON.parse(msg.content.toString()));
        },
        { noAck: false },
      );
    });
  } finally {
    await ch.close().catch(() => undefined);
  }
}

async function expectNoMessage(conn: amqplib.ChannelModel, queue: string, timeoutMs = 800): Promise<void> {
  const ch = await conn.createChannel();
  try {
    const got = await new Promise<boolean>((resolve) => {
      const timer = setTimeout(() => resolve(false), timeoutMs);
      void ch.consume(
        queue,
        (msg) => {
          if (!msg) return;
          clearTimeout(timer);
          ch.nack(msg, false, true);
          resolve(true);
        },
        { noAck: false },
      );
    });
    expect(got).toBe(false);
  } finally {
    await ch.close().catch(() => undefined);
  }
}

async function waitFor<T>(fn: () => Promise<T | undefined | null>, timeoutMs = 10_000, intervalMs = 100): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const result = await fn();
    if (result) return result;
    if (Date.now() > deadline) throw new Error('waitFor: timed out');
    await delay(intervalMs);
  }
}

describe('order-orchestrator saga (e2e, real RabbitMQ)', () => {
  let app: INestApplication;
  let ordersService: OrdersService;
  let orderModel: Model<OrderDocument>;
  let sagaLogModel: Model<SagaLogDocument>;
  let outboxModel: Model<OutboxDocument>;
  let processedModel: Model<ProcessedMessageDocument>;
  let amqpConn: amqplib.ChannelModel;
  let amqpCh: amqplib.Channel;

  const RESERVE_Q = 'test.commands.reserve-inventory';
  const CHARGE_Q = 'test.commands.charge';
  const RELEASE_Q = 'test.commands.release-inventory';
  const ORDER_CONFIRMED_Q = 'test.events.order-confirmed';
  const ORDER_CANCELLED_Q = 'test.events.order-cancelled';

  beforeAll(async () => {
    const module: TestingModule = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = module.createNestApplication();
    await app.init();

    ordersService = module.get(OrdersService);
    // OrderEntity/OrderSchema is registered via MongooseModule.forFeature in
    // both OrdersModule and OrderOrchestratorModule (same schema object, see
    // order-orchestrator.module.ts) — module.get(getModelToken(...)) is
    // ambiguous across two module scopes, so fetch the (single, shared)
    // compiled model straight off the connection instead.
    const conn: Connection = module.get(getConnectionToken());
    orderModel = conn.model<OrderDocument>(OrderEntity.name);
    sagaLogModel = module.get(getModelToken(SagaLogEntity.name));
    outboxModel = module.get(getModelToken(OutboxEntity.name));
    processedModel = module.get(getModelToken(ProcessedMessageEntity.name));

    amqpConn = await amqplib.connect(AMQP_URL);
    amqpCh = await amqpConn.createChannel();
    await amqpCh.assertExchange(EXCHANGES.SAGA_COMMANDS, 'direct', { durable: true });
    await amqpCh.assertExchange(EXCHANGES.SAGA_REPLIES, 'direct', { durable: true });
    await amqpCh.assertExchange(EXCHANGES.DOMAIN_EVENTS, 'topic', { durable: true });

    for (const [queue, routingKey] of [
      [RESERVE_Q, ROUTING_KEYS.RESERVE_INVENTORY],
      [CHARGE_Q, ROUTING_KEYS.CHARGE],
      [RELEASE_Q, ROUTING_KEYS.RELEASE_INVENTORY],
    ] as const) {
      await amqpCh.deleteQueue(queue).catch(() => undefined);
      await amqpCh.assertQueue(queue, { exclusive: false, autoDelete: false, durable: false });
      await amqpCh.bindQueue(queue, EXCHANGES.SAGA_COMMANDS, routingKey);
    }
    for (const [queue, routingKey] of [
      [ORDER_CONFIRMED_Q, ROUTING_KEYS.ORDER_CONFIRMED],
      [ORDER_CANCELLED_Q, ROUTING_KEYS.ORDER_CANCELLED],
    ] as const) {
      await amqpCh.deleteQueue(queue).catch(() => undefined);
      await amqpCh.assertQueue(queue, { exclusive: false, autoDelete: false, durable: false });
      await amqpCh.bindQueue(queue, EXCHANGES.DOMAIN_EVENTS, routingKey);
    }
  });

  afterAll(async () => {
    await amqpCh?.close().catch(() => undefined);
    await amqpConn?.close().catch(() => undefined);
    await app.close();
  });

  afterEach(async () => {
    const CLEANUP_TIMEOUT = 8_000;
    const cap = <T>(p: Promise<T>) =>
      Promise.race([p, new Promise<void>((_, r) => setTimeout(() => r(new Error('cleanup timeout')), CLEANUP_TIMEOUT))]);
    await cap(orderModel.deleteMany({})).catch(() => undefined);
    await cap(sagaLogModel.deleteMany({})).catch(() => undefined);
    await cap(outboxModel.deleteMany({})).catch(() => undefined);
    await cap(processedModel.deleteMany({})).catch(() => undefined);
    for (const q of [RESERVE_Q, CHARGE_Q, RELEASE_Q, ORDER_CONFIRMED_Q, ORDER_CANCELLED_Q]) {
      await amqpCh.purgeQueue(q).catch(() => undefined);
    }
  });

  async function createTestOrder(userEmail: string) {
    return ordersService.create({
      userEmail,
      items: [{ sku: 'WIDGET-1', qty: 2, price: 9.99 }],
    });
  }

  async function replyTo(routingKey: string, payload: unknown, sagaId: string, correlationId: string) {
    const envelope = createEnvelope(routingKey, payload, { correlationId, sagaId });
    amqpCh.publish(EXCHANGES.SAGA_REPLIES, routingKey, Buffer.from(JSON.stringify(envelope)), {
      contentType: 'application/json',
      persistent: true,
    });
    return envelope;
  }

  it('payment-failed path: release-inventory is sent and the order ends CANCELLED', async () => {
    const order = await createTestOrder('payfail@saga.test');
    expect(order.status).toBe('PENDING');

    const reserveCmd = await consumeOne(amqpConn, RESERVE_Q);
    expect(reserveCmd.type).toBe(ROUTING_KEYS.RESERVE_INVENTORY);
    const reservePayload = reserveCmd.payload as ReserveInventoryPayload;
    expect(reservePayload.orderId).toBe(order.id);
    const { sagaId } = reservePayload;
    expect(sagaId).toBeTruthy();

    await replyTo(
      ROUTING_KEYS.INVENTORY_RESERVED,
      { sagaId, orderId: order.id } satisfies InventoryReservedPayload,
      sagaId,
      reserveCmd.correlationId,
    );

    const chargeCmd = await consumeOne(amqpConn, CHARGE_Q);
    expect(chargeCmd.type).toBe(ROUTING_KEYS.CHARGE);
    const chargePayload = chargeCmd.payload as ChargePayload;
    expect(chargePayload.sagaId).toBe(sagaId);
    expect(chargePayload.userId).toBe('payfail@saga.test');

    await replyTo(
      ROUTING_KEYS.PAYMENT_FAILED,
      { sagaId, orderId: order.id, reason: 'card declined' } satisfies PaymentFailedPayload,
      sagaId,
      chargeCmd.correlationId,
    );

    const releaseCmd = await consumeOne(amqpConn, RELEASE_Q);
    expect(releaseCmd.type).toBe(ROUTING_KEYS.RELEASE_INVENTORY);
    const releasePayload = releaseCmd.payload as ReleaseInventoryPayload;
    expect(releasePayload.sagaId).toBe(sagaId);
    expect(releasePayload.orderId).toBe(order.id);
    expect(releasePayload.items).toEqual([{ catalogItemId: 'WIDGET-1', quantity: 2 }]);

    const cancelledEvent = await consumeOne(amqpConn, ORDER_CANCELLED_Q);
    const cancelledPayload = cancelledEvent.payload as OrderCancelledPayload;
    expect(cancelledPayload.orderId).toBe(order.id);
    expect(cancelledPayload.reason).toBe('card declined');

    const finalOrder = await waitFor(async () => {
      const doc = await orderModel.findById(order.id).lean();
      return doc?.status === 'CANCELLED' ? doc : null;
    });
    expect(finalOrder.status).toBe('CANCELLED');
    expect(finalOrder.sagaId).toBeUndefined();

    const sagaLog = await sagaLogModel.findOne({ sagaId }).lean();
    expect(sagaLog?.state).toBe('CANCELLED');
    const stepStatuses = sagaLog!.stepHistory.map((e) => `${e.step}:${e.status}`);
    expect(stepStatuses).toEqual([
      'reserve-inventory:sent',
      'reserve-inventory:succeeded',
      'charge:sent',
      'charge:failed',
      'release-inventory:sent',
    ]);

    await expectNoMessage(amqpConn, RELEASE_Q);
  });

  it('inventory-rejected path: no compensation is sent and the order ends CANCELLED', async () => {
    const order = await createTestOrder('invreject@saga.test');

    const reserveCmd = await consumeOne(amqpConn, RESERVE_Q);
    const { sagaId } = reserveCmd.payload as ReserveInventoryPayload;

    await replyTo(
      ROUTING_KEYS.INVENTORY_REJECTED,
      { sagaId, orderId: order.id, reason: 'out of stock' } satisfies InventoryRejectedPayload,
      sagaId,
      reserveCmd.correlationId,
    );

    const cancelledEvent = await consumeOne(amqpConn, ORDER_CANCELLED_Q);
    expect((cancelledEvent.payload as OrderCancelledPayload).reason).toBe('out of stock');

    const finalOrder = await waitFor(async () => {
      const doc = await orderModel.findById(order.id).lean();
      return doc?.status === 'CANCELLED' ? doc : null;
    });
    expect(finalOrder.status).toBe('CANCELLED');

    // No release-inventory should ever be dispatched — nothing had succeeded yet.
    await expectNoMessage(amqpConn, RELEASE_Q);
    await expectNoMessage(amqpConn, CHARGE_Q);
  });

  it('happy path: order ends CONFIRMED after inventory-reserved + payment-succeeded', async () => {
    const order = await createTestOrder('happy@saga.test');

    const reserveCmd = await consumeOne(amqpConn, RESERVE_Q);
    const { sagaId } = reserveCmd.payload as ReserveInventoryPayload;

    await replyTo(
      ROUTING_KEYS.INVENTORY_RESERVED,
      { sagaId, orderId: order.id } satisfies InventoryReservedPayload,
      sagaId,
      reserveCmd.correlationId,
    );

    const chargeCmd = await consumeOne(amqpConn, CHARGE_Q);
    expect((chargeCmd.payload as ChargePayload).sagaId).toBe(sagaId);

    await replyTo(
      ROUTING_KEYS.PAYMENT_SUCCEEDED,
      { sagaId, orderId: order.id, paymentId: 'pay_123' } satisfies PaymentSucceededPayload,
      sagaId,
      chargeCmd.correlationId,
    );

    const confirmedEvent = await consumeOne(amqpConn, ORDER_CONFIRMED_Q);
    expect((confirmedEvent.payload as OrderConfirmedPayload).orderId).toBe(order.id);

    const finalOrder = await waitFor(async () => {
      const doc = await orderModel.findById(order.id).lean();
      return doc?.status === 'CONFIRMED' ? doc : null;
    });
    expect(finalOrder.status).toBe('CONFIRMED');
    expect(finalOrder.sagaId).toBeUndefined();

    const sagaLog = await sagaLogModel.findOne({ sagaId }).lean();
    expect(sagaLog?.state).toBe('CONFIRMED');

    await expectNoMessage(amqpConn, RELEASE_Q);
  });

  it('duplicate reply (same messageId) is deduped — charge is issued exactly once', async () => {
    const order = await createTestOrder('duplicate@saga.test');
    const reserveCmd = await consumeOne(amqpConn, RESERVE_Q);
    const { sagaId } = reserveCmd.payload as ReserveInventoryPayload;

    const envelope = createEnvelope(
      ROUTING_KEYS.INVENTORY_RESERVED,
      { sagaId, orderId: order.id } satisfies InventoryReservedPayload,
      { correlationId: reserveCmd.correlationId, sagaId },
    );
    for (let i = 0; i < 2; i++) {
      amqpCh.publish(EXCHANGES.SAGA_REPLIES, ROUTING_KEYS.INVENTORY_RESERVED, Buffer.from(JSON.stringify(envelope)), {
        contentType: 'application/json',
        persistent: true,
      });
    }

    await consumeOne(amqpConn, CHARGE_Q);
    await expectNoMessage(amqpConn, CHARGE_Q, 1500);
  });
});
