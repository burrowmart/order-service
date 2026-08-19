import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import type { ClientSession } from 'mongoose';
import {
  SagaLogEntity,
  SagaLogDocument,
  SagaStepHistoryEntry,
  SagaOrderSnapshot,
} from './schemas/saga-log.schema';
import { SagaState } from './state-machine';

export interface CreateSagaLogInput {
  sagaId: string;
  orderId: string;
  correlationId: string;
  /** The first command's step name — the saga is created already "sent". */
  firstStep: string;
  firstStepMessageId: string;
  currentStepDeadlineAt: Date;
  orderSnapshot: SagaOrderSnapshot;
}

/**
 * Conditional-update payload for one saga transition. Every field maps
 * directly onto the single atomic findOneAndUpdate in applyTransition() —
 * this is intentionally one generic shape rather than four near-duplicate
 * methods, since all four saga actions (ISSUE_COMMAND/RECORD_ONLY/
 * CONFIRM_ORDER/CANCEL_ORDER) really are the same operation: conditionally
 * move state, optionally append stepHistory, optionally touch currentStep/
 * deadline/retryCount.
 */
export interface SagaTransitionUpdate {
  toState: SagaState;
  stepHistoryAdditions?: SagaStepHistoryEntry[];
  currentStep?: string;
  clearDeadline?: boolean;
  deadlineAt?: Date;
  resetRetryCount?: boolean;
  incrementRetryCount?: boolean;
}

@Injectable()
export class SagaLogRepository {
  constructor(
    @InjectModel(SagaLogEntity.name)
    private readonly model: Model<SagaLogDocument>,
  ) {}

  async create(session: ClientSession, input: CreateSagaLogInput): Promise<SagaLogDocument> {
    const docs = await this.model.create(
      [
        {
          sagaId: input.sagaId,
          orderId: input.orderId,
          correlationId: input.correlationId,
          currentStep: input.firstStep,
          state: 'STARTED',
          currentStepDeadlineAt: input.currentStepDeadlineAt,
          retryCount: 0,
          orderSnapshot: input.orderSnapshot,
          stepHistory: [
            { step: input.firstStep, status: 'sent', messageId: input.firstStepMessageId, at: new Date() },
          ],
        },
      ],
      { session },
    );
    return docs[0];
  }

  async findBySagaId(sagaId: string, session?: ClientSession): Promise<SagaLogDocument | null> {
    return this.model.findOne({ sagaId }, undefined, { session }).exec();
  }

  /**
   * Atomically applies one transition, conditioned on the saga still being in
   * `fromState`. Returns null if the precondition didn't match — i.e. a
   * concurrent/duplicate reply already moved the saga on, or a reply arrived
   * for an already-completed step. Callers must treat null as "ignore, log,
   * ack" and must NOT write any outbox row for a transition that returned null.
   */
  async applyTransition(
    session: ClientSession,
    sagaId: string,
    fromState: SagaState,
    update: SagaTransitionUpdate,
  ): Promise<SagaLogDocument | null> {
    const set: Record<string, unknown> = { state: update.toState };
    const unset: Record<string, ''> = {};

    if (update.currentStep !== undefined) set.currentStep = update.currentStep;
    if (update.resetRetryCount) set.retryCount = 0;
    if (update.clearDeadline) unset.currentStepDeadlineAt = '';
    else if (update.deadlineAt !== undefined) set.currentStepDeadlineAt = update.deadlineAt;

    const mongoUpdate: Record<string, unknown> = { $set: set };
    if (Object.keys(unset).length > 0) mongoUpdate.$unset = unset;
    if (update.incrementRetryCount) mongoUpdate.$inc = { retryCount: 1 };
    if (update.stepHistoryAdditions && update.stepHistoryAdditions.length > 0) {
      mongoUpdate.$push = { stepHistory: { $each: update.stepHistoryAdditions } };
    }

    return this.model
      .findOneAndUpdate({ sagaId, state: fromState }, mongoUpdate, { session, new: true })
      .exec();
  }

  /**
   * Sagas the timeout scheduler (and boot-time resume, which just runs one
   * scan immediately) must act on right now:
   *  - STARTED/INVENTORY_RESERVED sagas whose reply deadline has passed
   *    (retry the command, or — once retries are exhausted — fail+compensate)
   *  - PAYMENT_SUCCEEDED sagas (the internal CONFIRM step never got applied,
   *    e.g. a crash between the two atomic transitions) — always safe/cheap
   *    to retry immediately since CONFIRM is idempotent.
   */
  async findStale(now: Date): Promise<SagaLogDocument[]> {
    return this.model
      .find({
        $or: [
          { state: { $in: ['STARTED', 'INVENTORY_RESERVED'] }, currentStepDeadlineAt: { $lte: now } },
          { state: 'PAYMENT_SUCCEEDED' },
        ],
      })
      .exec();
  }

  async countNonTerminal(): Promise<number> {
    return this.model.countDocuments({ state: { $nin: ['CONFIRMED', 'CANCELLED'] } }).exec();
  }
}
