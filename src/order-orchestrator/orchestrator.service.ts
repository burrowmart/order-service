import { randomUUID } from 'crypto';
import { Injectable, Logger } from '@nestjs/common';
import { InjectConnection, InjectModel } from '@nestjs/mongoose';
import { Connection, Model } from 'mongoose';
import {
  createEnvelope,
  EXCHANGES,
  ROUTING_KEYS,
  type MessageEnvelope,
  type Order,
  type ReserveInventoryPayload,
  type ReleaseInventoryPayload,
  type ChargePayload,
  type RefundPaymentPayload,
  type OrderConfirmedPayload,
  type OrderCancelledPayload,
} from '@demo/contracts';
import { OutboxService, IdempotentConsumerService, type ConsumerResult } from '../common/outbox';
import { getCorrelationId } from '../common/correlation/correlation.context';
// Read-only import: the orchestrator writes Order.status/sagaId itself (see
// module doc below for why) but never modifies orders/ — same OrderSchema
// object registered a second time on the same connection is safe (Mongoose
// reuses the compiled model for an identical schema reference).
import { OrderEntity, OrderDocument } from '../orders/schemas/order.schema';
import { SagaLogRepository } from './saga-log.repository';
import type { SagaLogDocument, SagaOrderSnapshot } from './schemas/saga-log.schema';
import {
  SAGA_EVENTS,
  SAGA_STEPS,
  STEP_COMPENSATIONS,
  nextTransition,
  type SagaEvent,
  type SagaState,
  type SagaStep,
} from './state-machine';
import { SAGA_STEP_TIMEOUT_MS, SAGA_REPLY_CONSUMER_MAX_RETRIES } from './saga.config';

type SagaStepHistoryEntryInput = {
  step: string;
  status: 'sent' | 'succeeded' | 'failed';
  messageId: string;
  at: Date;
};

/**
 * Order-placement saga orchestrator.
 *
 * Sequences and compensates only — no inventory or payment logic lives here.
 * All cross-service communication is RabbitMQ commands/replies, dispatched
 * exclusively through the outbox (OutboxService.writeInTx), atomically with
 * the saga_log transition that caused them (see doIssueCommand/doCancelOrder/
 * doConfirmOrder/retryCurrentStep — each opens exactly one Mongo transaction
 * per saga_log transition).
 *
 * OrdersService.create() (out of scope for this module) calls start(order)
 * after persisting the order as PENDING; this class then owns every
 * subsequent Order.status/sagaId write for that order via a second Mongoose
 * model bound to the same 'orders' collection (see import above) — the
 * alternative (threading a Mongo session through OrdersService.transitionStatus)
 * would require modifying src/orders/, which this session is scoped not to touch.
 */
@Injectable()
export class OrchestratorService {
  private readonly logger = new Logger(OrchestratorService.name);

  constructor(
    private readonly sagaLogRepo: SagaLogRepository,
    private readonly outbox: OutboxService,
    private readonly idempotentConsumer: IdempotentConsumerService,
    @InjectModel(OrderEntity.name) private readonly orderModel: Model<OrderDocument>,
    @InjectConnection() private readonly conn: Connection,
  ) {}

  async start(order: Order): Promise<void> {
    const sagaId = randomUUID();
    const correlationId = getCorrelationId() ?? randomUUID();
    const orderSnapshot: SagaOrderSnapshot = {
      userEmail: order.userEmail,
      items: order.items.map((i) => ({ catalogItemId: i.sku, quantity: i.qty })),
      amount: order.total,
    } as SagaOrderSnapshot;

    const session = await this.conn.startSession();
    try {
      await session.withTransaction(async () => {
        const envelope = createEnvelope<ReserveInventoryPayload>(
          ROUTING_KEYS.RESERVE_INVENTORY,
          { sagaId, orderId: order.id, items: orderSnapshot.items },
          { correlationId, sagaId, step: SAGA_STEPS.RESERVE_INVENTORY },
        );

        await this.sagaLogRepo.create(session, {
          sagaId,
          orderId: order.id,
          correlationId,
          firstStep: SAGA_STEPS.RESERVE_INVENTORY,
          firstStepMessageId: envelope.messageId,
          currentStepDeadlineAt: new Date(Date.now() + SAGA_STEP_TIMEOUT_MS),
          orderSnapshot,
        });

        await this.orderModel.updateOne(
          { _id: order.id, status: 'PENDING' },
          { $set: { sagaId } },
          { session },
        );

        await this.outbox.writeInTx(session, envelope, EXCHANGES.SAGA_COMMANDS, ROUTING_KEYS.RESERVE_INVENTORY);
      });
    } finally {
      await session.endSession();
    }

    this.logger.log({ sagaId, orderId: order.id }, 'Order-placement saga started');
  }

  /**
   * Entry point for reply consumers: dedupes by messageId (IdempotentConsumer),
   * then applies the saga transition. Returns the ack/nack/dlq outcome for the
   * caller to wire into RabbitMQ ack mechanics.
   */
  async handleReply(
    envelope: MessageEnvelope,
    event: SagaEvent,
    retryCount: number,
  ): Promise<ConsumerResult> {
    return this.idempotentConsumer.handle(
      envelope,
      async (payload) => {
        if (!envelope.sagaId) {
          this.logger.warn({ messageId: envelope.messageId }, 'Reply envelope missing sagaId — dropping');
          return;
        }
        await this.applyEvent(envelope.sagaId, event, envelope.messageId, payload as Record<string, unknown>);
      },
      { retryCount, maxRetries: SAGA_REPLY_CONSUMER_MAX_RETRIES },
    );
  }

  /**
   * Applies one event to a saga via the transition table. Used both by
   * handleReply() (real replies) and by the timeout scheduler (synthesized
   * timeout-failure events + the CONFIRM retry for a saga stuck in
   * PAYMENT_SUCCEEDED). No entry in the table for (state, event) is treated
   * uniformly as "ignore, log, ack" — this is both the illegal-transition
   * guard AND how a reply for an already-completed step is dropped.
   */
  async applyEvent(
    sagaId: string,
    event: SagaEvent,
    messageId: string,
    payload: Record<string, unknown>,
  ): Promise<void> {
    const saga = await this.sagaLogRepo.findBySagaId(sagaId);
    if (!saga) {
      this.logger.warn({ sagaId, event }, 'No saga_log for sagaId — ignoring');
      return;
    }

    const transition = nextTransition(saga.state, event);
    if (!transition) {
      this.logger.log(
        { sagaId, event, state: saga.state },
        'Event not legal from current saga state — stale/duplicate reply or already-completed step, ignoring',
      );
      return;
    }

    switch (transition.action.kind) {
      case 'ISSUE_COMMAND':
        await this.doIssueCommand(saga, transition.to, transition.action.step, messageId);
        return;
      case 'RECORD_ONLY':
        await this.doRecordOnly(saga, transition.to, messageId);
        // Internal auto-advance: PAYMENT_SUCCEEDED always immediately confirms.
        // Re-fetches the saga (now in PAYMENT_SUCCEEDED) rather than reusing
        // the stale in-memory copy.
        await this.applyEvent(sagaId, SAGA_EVENTS.CONFIRM, randomUUID(), {});
        return;
      case 'CONFIRM_ORDER':
        await this.doConfirmOrder(saga, transition.to);
        return;
      case 'CANCEL_ORDER': {
        const reason = typeof payload.reason === 'string' ? payload.reason : `saga cancelled on ${event}`;
        await this.doCancelOrder(saga, transition.to, messageId, reason);
        return;
      }
    }
  }

  /** Re-sends currentStep's command with the same sagaId+step, a fresh messageId. Called by the timeout scheduler. */
  async retryCurrentStep(saga: SagaLogDocument): Promise<void> {
    const session = await this.conn.startSession();
    try {
      await session.withTransaction(async () => {
        const step = saga.currentStep as SagaStep;
        const envelope = createEnvelope(step, this.buildCommandPayload(step, saga), {
          correlationId: saga.correlationId,
          sagaId: saga.sagaId,
          step,
        });
        const now = new Date();
        const updated = await this.sagaLogRepo.applyTransition(session, saga.sagaId, saga.state, {
          toState: saga.state,
          deadlineAt: new Date(now.getTime() + SAGA_STEP_TIMEOUT_MS),
          incrementRetryCount: true,
          stepHistoryAdditions: [{ step, status: 'sent', messageId: envelope.messageId, at: now }],
        });
        if (!updated) {
          this.logger.log({ sagaId: saga.sagaId }, 'Lost race retrying step — saga already advanced');
          return;
        }
        await this.outbox.writeInTx(session, envelope, EXCHANGES.SAGA_COMMANDS, step);
        this.logger.warn(
          { sagaId: saga.sagaId, step, retryCount: saga.retryCount + 1 },
          'Saga step timed out — retrying command',
        );
      });
    } finally {
      await session.endSession();
    }
  }

  private async doIssueCommand(
    saga: SagaLogDocument,
    toState: SagaState,
    nextStep: SagaStep,
    replyMessageId: string,
  ): Promise<void> {
    const session = await this.conn.startSession();
    try {
      await session.withTransaction(async () => {
        const envelope = createEnvelope(nextStep, this.buildCommandPayload(nextStep, saga), {
          correlationId: saga.correlationId,
          sagaId: saga.sagaId,
          step: nextStep,
        });
        const now = new Date();
        const additions: SagaStepHistoryEntryInput[] = [
          { step: saga.currentStep, status: 'succeeded', messageId: replyMessageId, at: now },
          { step: nextStep, status: 'sent', messageId: envelope.messageId, at: now },
        ];

        const updated = await this.sagaLogRepo.applyTransition(session, saga.sagaId, saga.state, {
          toState,
          currentStep: nextStep,
          deadlineAt: new Date(now.getTime() + SAGA_STEP_TIMEOUT_MS),
          resetRetryCount: true,
          stepHistoryAdditions: additions,
        });
        if (!updated) {
          this.logger.log({ sagaId: saga.sagaId }, 'Lost race on transition — saga already advanced');
          return;
        }
        await this.outbox.writeInTx(session, envelope, EXCHANGES.SAGA_COMMANDS, nextStep);
      });
    } finally {
      await session.endSession();
    }
  }

  private async doRecordOnly(saga: SagaLogDocument, toState: SagaState, replyMessageId: string): Promise<void> {
    const session = await this.conn.startSession();
    try {
      await session.withTransaction(async () => {
        const updated = await this.sagaLogRepo.applyTransition(session, saga.sagaId, saga.state, {
          toState,
          clearDeadline: true,
          stepHistoryAdditions: [
            { step: saga.currentStep, status: 'succeeded', messageId: replyMessageId, at: new Date() },
          ],
        });
        if (!updated) {
          this.logger.log({ sagaId: saga.sagaId }, 'Lost race on transition — saga already advanced');
        }
      });
    } finally {
      await session.endSession();
    }
  }

  private async doConfirmOrder(saga: SagaLogDocument, toState: SagaState): Promise<void> {
    const session = await this.conn.startSession();
    try {
      await session.withTransaction(async () => {
        const updated = await this.sagaLogRepo.applyTransition(session, saga.sagaId, saga.state, {
          toState,
          clearDeadline: true,
        });
        if (!updated) {
          this.logger.log({ sagaId: saga.sagaId }, 'Lost race confirming saga — already terminal');
          return;
        }

        const orderUpdate = await this.orderModel.updateOne(
          { _id: saga.orderId, status: 'PENDING' },
          { $set: { status: 'CONFIRMED' }, $unset: { sagaId: '' } },
          { session },
        );
        if (orderUpdate.matchedCount === 0) {
          this.logger.warn({ orderId: saga.orderId }, 'Order was not PENDING at saga confirmation');
        }

        const envelope = createEnvelope<OrderConfirmedPayload>(
          ROUTING_KEYS.ORDER_CONFIRMED,
          { orderId: saga.orderId, userId: saga.orderSnapshot.userEmail, sagaId: saga.sagaId },
          { correlationId: saga.correlationId, sagaId: saga.sagaId },
        );
        await this.outbox.writeInTx(session, envelope, EXCHANGES.DOMAIN_EVENTS, ROUTING_KEYS.ORDER_CONFIRMED);
      });
    } finally {
      await session.endSession();
    }
  }

  private async doCancelOrder(
    saga: SagaLogDocument,
    toState: SagaState,
    replyMessageId: string,
    reason: string,
  ): Promise<void> {
    const session = await this.conn.startSession();
    try {
      await session.withTransaction(async () => {
        // Compensate whatever completed, strictly in reverse order — driven
        // by stepHistory, not by which specific failure event fired.
        const succeededStepsReversed = saga.stepHistory
          .filter((e) => e.status === 'succeeded')
          .map((e) => e.step)
          .reverse();

        const compensations: Array<{ envelope: MessageEnvelope; step: SagaStep }> = [];
        const now = new Date();
        const additions: SagaStepHistoryEntryInput[] = [
          { step: saga.currentStep, status: 'failed', messageId: replyMessageId, at: now },
        ];

        for (const step of succeededStepsReversed) {
          const compensationStep = STEP_COMPENSATIONS[step as SagaStep];
          if (!compensationStep) continue;
          const envelope = createEnvelope(compensationStep, this.buildCommandPayload(compensationStep, saga), {
            correlationId: saga.correlationId,
            sagaId: saga.sagaId,
            step: compensationStep,
          });
          compensations.push({ envelope, step: compensationStep });
          additions.push({ step: compensationStep, status: 'sent', messageId: envelope.messageId, at: now });
        }

        const updated = await this.sagaLogRepo.applyTransition(session, saga.sagaId, saga.state, {
          toState,
          clearDeadline: true,
          stepHistoryAdditions: additions,
        });
        if (!updated) {
          this.logger.log({ sagaId: saga.sagaId }, 'Lost race cancelling saga — already terminal');
          return;
        }

        for (const { envelope, step } of compensations) {
          await this.outbox.writeInTx(session, envelope, EXCHANGES.SAGA_COMMANDS, step);
        }

        const orderUpdate = await this.orderModel.updateOne(
          { _id: saga.orderId, status: 'PENDING' },
          { $set: { status: 'CANCELLED' }, $unset: { sagaId: '' } },
          { session },
        );
        if (orderUpdate.matchedCount === 0) {
          this.logger.warn({ orderId: saga.orderId }, 'Order was not PENDING at saga cancellation');
        }

        const envelope = createEnvelope<OrderCancelledPayload>(
          ROUTING_KEYS.ORDER_CANCELLED,
          { orderId: saga.orderId, userId: saga.orderSnapshot.userEmail, sagaId: saga.sagaId, reason },
          { correlationId: saga.correlationId, sagaId: saga.sagaId },
        );
        await this.outbox.writeInTx(session, envelope, EXCHANGES.DOMAIN_EVENTS, ROUTING_KEYS.ORDER_CANCELLED);
      });
    } finally {
      await session.endSession();
    }
  }

  private buildCommandPayload(
    step: SagaStep,
    saga: SagaLogDocument,
  ): ReserveInventoryPayload | ReleaseInventoryPayload | ChargePayload | RefundPaymentPayload {
    const { sagaId, orderId, orderSnapshot } = saga;
    switch (step) {
      case SAGA_STEPS.RESERVE_INVENTORY:
      case SAGA_STEPS.RELEASE_INVENTORY:
        return {
          sagaId,
          orderId,
          items: orderSnapshot.items.map((i) => ({ catalogItemId: i.catalogItemId, quantity: i.quantity })),
        };
      case SAGA_STEPS.CHARGE:
        return { sagaId, orderId, userId: orderSnapshot.userEmail, amount: orderSnapshot.amount };
      case SAGA_STEPS.REFUND_PAYMENT:
        // Unreachable via this state machine — see STEP_COMPENSATIONS doc comment.
        throw new Error('refund-payment is unreachable: no step follows a successful charge in this saga');
      default: {
        const exhaustive: never = step;
        throw new Error(`Unhandled saga step: ${String(exhaustive)}`);
      }
    }
  }
}
