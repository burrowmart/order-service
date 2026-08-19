import {
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { context, propagation } from '@opentelemetry/api';
import * as amqplib from 'amqplib';
import { EXCHANGES, QUEUES, ROUTING_KEYS, type MessageEnvelope } from '@demo/contracts';
import { OrchestratorService } from './orchestrator.service';
import { SAGA_EVENTS, type SagaEvent } from './state-machine';

/** Dead-letter exchange for all four saga reply queues. */
const REPLIES_DLX = 'saga.replies.dlx';

interface ReplyQueueBinding {
  queue: string;
  routingKey: string;
  event: SagaEvent;
}

const REPLY_QUEUE_BINDINGS: ReplyQueueBinding[] = [
  { queue: QUEUES.INVENTORY_RESERVED, routingKey: ROUTING_KEYS.INVENTORY_RESERVED, event: SAGA_EVENTS.INVENTORY_RESERVED },
  { queue: QUEUES.INVENTORY_REJECTED, routingKey: ROUTING_KEYS.INVENTORY_REJECTED, event: SAGA_EVENTS.INVENTORY_REJECTED },
  { queue: QUEUES.PAYMENT_SUCCEEDED, routingKey: ROUTING_KEYS.PAYMENT_SUCCEEDED, event: SAGA_EVENTS.PAYMENT_SUCCEEDED },
  { queue: QUEUES.PAYMENT_FAILED, routingKey: ROUTING_KEYS.PAYMENT_FAILED, event: SAGA_EVENTS.PAYMENT_FAILED },
];

/**
 * Consumes the order-orchestrator's four saga reply queues.
 *
 * Retries for a reply *handler* throwing (e.g. a transient Mongo error) are
 * done by hand: classic RabbitMQ queues don't expose a reliable per-message
 * delivery count, so a retryable failure is ack'd and republished with an
 * incremented `x-retry-count` header rather than nack'd-with-requeue. Once
 * IdempotentConsumerService reports 'dlq' (retries exhausted), the message is
 * nack'd without requeue and the queue's `x-dead-letter-exchange` argument
 * routes it to `<queue>.dlq` automatically — no manual publish needed there.
 *
 * This is a distinct retry mechanism from the saga's own step timeouts
 * (SagaTimeoutSchedulerService): this one covers "we received a reply but
 * failed to process it"; that one covers "no reply arrived at all".
 */
@Injectable()
export class SagaRepliesConsumerService implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly logger = new Logger(SagaRepliesConsumerService.name);
  private conn?: amqplib.ChannelModel;
  private ch?: amqplib.Channel;

  constructor(
    private readonly orchestrator: OrchestratorService,
    private readonly config: ConfigService,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    // Same flag OutboxPublisherService uses to stay off in unit/plain-e2e
    // test environments that don't run RabbitMQ.
    if (!this.config.get<boolean>('outboxPublisherEnabled')) {
      this.logger.log('Saga reply consumer disabled via OUTBOX_PUBLISHER_ENABLED');
      return;
    }
    await this.connect();
  }

  private async connect(): Promise<void> {
    const url = this.config.get<string>('rabbitmqUrl')!;
    this.conn = await amqplib.connect(url);
    this.ch = await this.conn.createChannel();

    await this.ch.assertExchange(EXCHANGES.SAGA_REPLIES, 'direct', { durable: true });
    await this.ch.assertExchange(REPLIES_DLX, 'direct', { durable: true });

    for (const binding of REPLY_QUEUE_BINDINGS) {
      await this.ch.assertQueue(binding.queue, {
        durable: true,
        arguments: { 'x-dead-letter-exchange': REPLIES_DLX },
      });
      await this.ch.bindQueue(binding.queue, EXCHANGES.SAGA_REPLIES, binding.routingKey);

      const dlq = `${binding.queue}.dlq`;
      await this.ch.assertQueue(dlq, { durable: true });
      await this.ch.bindQueue(dlq, REPLIES_DLX, binding.routingKey);

      await this.ch.consume(
        binding.queue,
        (msg) => {
          if (!msg) return;
          this.onMessage(msg, binding.event).catch((err: Error) => {
            this.logger.error({ err, queue: binding.queue }, 'Unhandled error processing reply — nacking without requeue');
            this.ch!.nack(msg, false, false);
          });
        },
        { noAck: false },
      );
    }

    this.logger.log('Saga reply consumer connected and bound to saga.replies queues');
  }

  private async onMessage(msg: amqplib.ConsumeMessage, event: SagaEvent): Promise<void> {
    const envelope = JSON.parse(msg.content.toString()) as MessageEnvelope;
    const retryCount = Number(msg.properties.headers?.['x-retry-count'] ?? 0);

    // Extract the trace context the publisher attached as a `traceparent`
    // header (see OutboxService.writeInTx / OutboxPublisherService on the
    // sending side — the change-stream-driven publish has no span of its
    // own to propagate automatically) so any outbox row handleReply causes
    // to be written (the next saga step's command) captures a continuation
    // of the original request's trace rather than starting a new one.
    const traceCtx = propagation.extract(context.active(), msg.properties.headers ?? {});
    const result = await context.with(traceCtx, () =>
      this.orchestrator.handleReply(envelope, event, retryCount),
    );

    switch (result) {
      case 'ack':
        this.ch!.ack(msg);
        return;
      case 'nack':
        this.ch!.ack(msg);
        this.ch!.publish(EXCHANGES.SAGA_REPLIES, msg.fields.routingKey, msg.content, {
          ...msg.properties,
          headers: { ...msg.properties.headers, 'x-retry-count': retryCount + 1 },
        });
        return;
      case 'dlq':
        this.ch!.nack(msg, false, false);
        return;
    }
  }

  async onApplicationShutdown(): Promise<void> {
    try {
      await this.ch?.close();
      await this.conn?.close();
    } catch {
      // Ignore errors during shutdown
    }
  }
}
