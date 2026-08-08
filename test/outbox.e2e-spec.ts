/**
 * Outbox module integration tests for order-service.
 *
 * order-service does not write outbox rows on its own yet (OrchestratorService
 * is a stub — see src/order-orchestrator/). These tests drive
 * OutboxService.writeInTx() directly with an OrderConfirmedPayload envelope —
 * the domain event the orchestrator is expected to publish once implemented
 * (see contracts/src/messages/events.ts) — to exercise the untouched outbox
 * module machinery ahead of that work landing.
 *
 * Prerequisites:
 *   docker compose -f ../platform-infra/docker-compose.yml up -d redis rabbitmq
 *
 * Mongo RS is started automatically by outbox-global-setup.ts.
 */
import { INestApplication, ValidationPipe } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { getConnectionToken, getModelToken } from '@nestjs/mongoose';
import { Connection, Model } from 'mongoose';
import * as amqplib from 'amqplib';
import Redis from 'ioredis';
import { randomUUID } from 'crypto';
import { createEnvelope, ROUTING_KEYS, EXCHANGES } from '@demo/contracts';
import type { OrderConfirmedPayload } from '@demo/contracts';
import { AppModule } from '../src/app.module';
import { OutboxService } from '../src/common/outbox/outbox.service';
import { OutboxPublisherService } from '../src/common/outbox/outbox-publisher.service';
import { LeaderLockService } from '../src/common/outbox/leader-lock.service';
import { IdempotentConsumerService } from '../src/common/outbox/idempotent-consumer.service';
import { OutboxEntity, OutboxDocument } from '../src/common/outbox/schemas/outbox.schema';
import { OutboxStateEntity, OutboxStateDocument } from '../src/common/outbox/schemas/outbox-state.schema';
import { ProcessedMessageEntity, ProcessedMessageDocument } from '../src/common/outbox/schemas/processed-message.schema';

const AMQP_URL = process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672';
const TEST_QUEUE = 'test.outbox.order-confirmed';

async function consumeOne(conn: amqplib.ChannelModel, queue: string, timeoutMs = 7_000): Promise<any> {
  const ch = await conn.createChannel();
  try {
    return await new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`Timeout on ${queue}`)), timeoutMs);
      void ch.consume(queue, (msg) => {
        if (!msg) return;
        clearTimeout(timer);
        ch.ack(msg);
        resolve(JSON.parse(msg.content.toString()));
      }, { noAck: false });
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
      void ch.consume(queue, (msg) => {
        if (!msg) return;
        clearTimeout(timer);
        ch.nack(msg, false, true);
        resolve(true);
      }, { noAck: false });
    });
    expect(got).toBe(false);
  } finally {
    await ch.close().catch(() => undefined);
  }
}

const delay = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('Outbox module (order-service e2e)', () => {
  let app: INestApplication;
  let outboxService: OutboxService;
  let publisher: OutboxPublisherService;
  let idempotentConsumer: IdempotentConsumerService;
  let conn: Connection;
  let outboxModel: Model<OutboxDocument>;
  let stateModel: Model<OutboxStateDocument>;
  let processedModel: Model<ProcessedMessageDocument>;
  let amqpConn: amqplib.ChannelModel;
  let amqpCh: amqplib.Channel;

  /** Writes one OrderConfirmedPayload envelope through the outbox in its own transaction. */
  async function writeOrderConfirmed(payload: OrderConfirmedPayload) {
    const envelope = createEnvelope<OrderConfirmedPayload>(ROUTING_KEYS.ORDER_CONFIRMED, payload);
    const session = await conn.startSession();
    try {
      await session.withTransaction(async () => {
        await outboxService.writeInTx(session, envelope, EXCHANGES.DOMAIN_EVENTS, ROUTING_KEYS.ORDER_CONFIRMED);
      });
    } finally {
      await session.endSession();
    }
    return envelope;
  }

  beforeAll(async () => {
    const module: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = module.createNestApplication();
    app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true, forbidNonWhitelisted: true }));
    await app.init();

    outboxService = module.get(OutboxService);
    publisher = module.get(OutboxPublisherService);
    idempotentConsumer = module.get(IdempotentConsumerService);
    conn = module.get(getConnectionToken());
    outboxModel = module.get(getModelToken(OutboxEntity.name));
    stateModel = module.get(getModelToken(OutboxStateEntity.name));
    processedModel = module.get(getModelToken(ProcessedMessageEntity.name));

    amqpConn = await amqplib.connect(AMQP_URL);
    amqpCh = await amqpConn.createChannel();
    await amqpCh.assertExchange(EXCHANGES.DOMAIN_EVENTS, 'topic', { durable: true });
    {
      const tmp = await amqpConn.createChannel();
      await tmp.deleteQueue(TEST_QUEUE).catch(() => undefined);
      await tmp.close().catch(() => undefined);
    }
    await amqpCh.assertQueue(TEST_QUEUE, { exclusive: false, autoDelete: false, durable: false });
    await amqpCh.bindQueue(TEST_QUEUE, EXCHANGES.DOMAIN_EVENTS, ROUTING_KEYS.ORDER_CONFIRMED);
  });

  afterAll(async () => {
    await amqpCh?.close();
    await amqpConn?.close();
    await app.close();
  });

  afterEach(async () => {
    publisher.prePublishHook = undefined;
    const CLEANUP_TIMEOUT = 8_000;
    const cap = <T>(p: Promise<T>) =>
      Promise.race([p, new Promise<void>((_, r) => setTimeout(() => r(new Error('cleanup timeout')), CLEANUP_TIMEOUT))]);
    await cap(outboxModel.deleteMany({})).catch(() => undefined);
    await cap(stateModel.deleteMany({})).catch(() => undefined);
    await cap(processedModel.deleteMany({})).catch(() => undefined);
    await amqpCh.purgeQueue(TEST_QUEUE).catch(() => undefined);
  });

  it('(a) outbox write → message in RabbitMQ exactly once', async () => {
    const orderId = randomUUID();
    await writeOrderConfirmed({ orderId, userId: 'alice@outbox.test', sagaId: randomUUID() });

    const msg = await consumeOne(amqpConn, TEST_QUEUE);
    expect(msg.type).toBe(ROUTING_KEYS.ORDER_CONFIRMED);
    expect(msg.payload.orderId).toBe(orderId);
    expect(msg.messageId).toBeTruthy();
    expect(msg.correlationId).toBeTruthy();

    await delay(200);
    const row = await outboxModel.findOne({ 'envelope.payload.orderId': orderId }).lean();
    expect(row?.published).toBe(true);

    await expectNoMessage(amqpConn, TEST_QUEUE);
  });

  it('(b) crash after Mongo commit → republishes on restart via resumeToken', async () => {
    await writeOrderConfirmed({ orderId: randomUUID(), userId: 'warmup@outbox.test', sagaId: randomUUID() });
    await consumeOne(amqpConn, TEST_QUEUE, 7_000);
    await delay(500);

    const stateAfterWarmup = await stateModel.findById('default').lean();
    expect(stateAfterWarmup?.resumeToken).toBeDefined();

    let hookFired = false;
    publisher.prePublishHook = async () => {
      if (hookFired) return;
      hookFired = true;
      publisher.prePublishHook = undefined;
      throw new Error('simulated crash before publish');
    };

    const orderId = randomUUID();
    await writeOrderConfirmed({ orderId, userId: 'crash@outbox.test', sagaId: randomUUID() });

    await delay(300);
    expect(hookFired).toBe(true);

    const unpub = await outboxModel.findOne({ 'envelope.payload.orderId': orderId }).lean();
    expect(unpub?.published).toBe(false);

    await publisher.drainBacklog();

    const msg = await consumeOne(amqpConn, TEST_QUEUE);
    expect(msg.payload.orderId).toBe(orderId);

    await expectNoMessage(amqpConn, TEST_QUEUE);

    await delay(200);
    const pub = await outboxModel.findOne({ 'envelope.payload.orderId': orderId }).lean();
    expect(pub?.published).toBe(true);
  });

  it('(c) two instances → only the leader publishes (Redis lock)', async () => {
    const redis2 = new Redis(process.env.REDIS_URL ?? 'redis://localhost:6379');
    const lock2 = new LeaderLockService(redis2);

    const acquired = await lock2.acquire();
    expect(acquired).toBe(false); // leader slot already taken by app

    await redis2.quit();

    const orderId = randomUUID();
    await writeOrderConfirmed({ orderId, userId: 'leader@outbox.test', sagaId: randomUUID() });

    const msg = await consumeOne(amqpConn, TEST_QUEUE);
    expect(msg.payload.orderId).toBe(orderId);

    await expectNoMessage(amqpConn, TEST_QUEUE);
  });

  it('(d) duplicate messageId → handler invoked exactly once', async () => {
    const envelope = createEnvelope<OrderConfirmedPayload>(ROUTING_KEYS.ORDER_CONFIRMED, {
      orderId: randomUUID(),
      userId: 'dedup@outbox.test',
      sagaId: randomUUID(),
    });

    let callCount = 0;
    const handler = async (_payload: OrderConfirmedPayload) => {
      callCount++;
    };

    const r1 = await idempotentConsumer.handle(envelope, handler);
    expect(r1).toBe('ack');
    expect(callCount).toBe(1);

    const r2 = await idempotentConsumer.handle(envelope, handler);
    expect(r2).toBe('ack');
    expect(callCount).toBe(1);

    const r3 = await idempotentConsumer.handle(envelope, handler);
    expect(r3).toBe('ack');
    expect(callCount).toBe(1);
  });

  it('handler failure → nack, then dlq after maxRetries', async () => {
    const envelope = createEnvelope<OrderConfirmedPayload>(ROUTING_KEYS.ORDER_CONFIRMED, {
      orderId: randomUUID(),
      userId: 'fail@outbox.test',
      sagaId: randomUUID(),
    });
    const boom = async () => {
      throw new Error('transient');
    };

    expect(
      await idempotentConsumer.handle(envelope, boom, { retryCount: 0, maxRetries: 2 }),
    ).toBe('nack');
    expect(
      await idempotentConsumer.handle(envelope, boom, { retryCount: 2, maxRetries: 2 }),
    ).toBe('dlq');
  });
});
