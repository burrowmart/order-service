import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument } from 'mongoose';
import { SAGA_STATES, SagaState } from '../state-machine';

export type SagaStepStatus = 'sent' | 'succeeded' | 'failed';

export type SagaLogDocument = HydratedDocument<SagaLogEntity>;

/** One entry per command/compensation dispatch or reply outcome for a step. */
@Schema({ _id: false })
export class SagaStepHistoryEntry {
  @Prop({ required: true })
  step!: string;

  @Prop({ required: true, enum: ['sent', 'succeeded', 'failed'] })
  status!: SagaStepStatus;

  @Prop({ required: true })
  messageId!: string;

  @Prop({ required: true })
  at!: Date;
}

export const SagaStepHistoryEntrySchema = SchemaFactory.createForClass(SagaStepHistoryEntry);

/**
 * Reserve-inventory items in the shape catalog-service's command payload
 * expects (catalogItemId/quantity) — order-service's own Order.items uses
 * sku/qty/price. Captured once at start() so retries/compensation can
 * reconstruct command payloads without re-reading (and re-trusting) a
 * possibly-mutated Order document mid-saga.
 */
@Schema({ _id: false })
export class SagaOrderSnapshotItem {
  @Prop({ required: true })
  catalogItemId!: string;

  @Prop({ required: true, min: 1 })
  quantity!: number;
}
export const SagaOrderSnapshotItemSchema = SchemaFactory.createForClass(SagaOrderSnapshotItem);

@Schema({ _id: false })
export class SagaOrderSnapshot {
  @Prop({ required: true })
  userEmail!: string;

  @Prop({ type: [SagaOrderSnapshotItemSchema], required: true })
  items!: SagaOrderSnapshotItem[];

  /** Carried through as OrdersService computed it (see ChargePayload re: units). */
  @Prop({ required: true, min: 0 })
  amount!: number;
}
export const SagaOrderSnapshotSchema = SchemaFactory.createForClass(SagaOrderSnapshot);

@Schema({ timestamps: true, collection: 'saga_log' })
export class SagaLogEntity {
  @Prop({ required: true, unique: true, index: true })
  sagaId!: string;

  @Prop({ required: true, index: true })
  orderId!: string;

  /** Propagated to every outgoing command/event envelope for this saga. */
  @Prop({ required: true })
  correlationId!: string;

  /** The step currently awaiting a reply (or last dispatched, once terminal). */
  @Prop({ required: true })
  currentStep!: string;

  @Prop({ type: [SagaStepHistoryEntrySchema], default: [] })
  stepHistory!: SagaStepHistoryEntry[];

  @Prop({
    required: true,
    enum: SAGA_STATES as string[],
    default: 'STARTED',
    index: true,
  })
  state!: SagaState;

  /**
   * Deadline for currentStep's reply. The timeout scheduler scans on this
   * field; cleared once the saga reaches a terminal state.
   */
  @Prop()
  currentStepDeadlineAt?: Date;

  /** Number of times currentStep's command has been (re)sent. Resets to 0 whenever currentStep advances. */
  @Prop({ default: 0 })
  retryCount!: number;

  @Prop({ type: SagaOrderSnapshotSchema, required: true })
  orderSnapshot!: SagaOrderSnapshot;
}

export const SagaLogSchema = SchemaFactory.createForClass(SagaLogEntity);

// Timeout scheduler's scan query: non-terminal sagas whose deadline has passed.
SagaLogSchema.index({ state: 1, currentStepDeadlineAt: 1 });
