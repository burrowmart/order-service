import { ROUTING_KEYS } from '@demo/contracts';

/**
 * Explicit state machine for the order-placement saga.
 *
 * STARTED -> INVENTORY_RESERVED -> PAYMENT_SUCCEEDED -> CONFIRMED is the happy
 * path. Both failure replies (inventory-rejected, payment-failed) go straight
 * to CANCELLED; CANCEL_ORDER compensates whatever steps already succeeded, in
 * reverse order, driven by saga_log.stepHistory (see orchestrator.service.ts).
 *
 * PAYMENT_SUCCEEDED is a real, persisted state (not folded into CONFIRMED) so
 * the 'charge' step's success is durably recorded before the separate
 * "mark order confirmed" action runs. CONFIRM is an internal-only event (never
 * arrives over AMQP) that the orchestrator fires immediately after recording
 * PAYMENT_SUCCEEDED, and is also what a crash-recovery resume replays for a
 * saga stuck in PAYMENT_SUCCEEDED.
 */
export type SagaState =
  | 'STARTED'
  | 'INVENTORY_RESERVED'
  | 'PAYMENT_SUCCEEDED'
  | 'CONFIRMED'
  | 'CANCELLED';

export const SAGA_STATES: ReadonlyArray<SagaState> = [
  'STARTED',
  'INVENTORY_RESERVED',
  'PAYMENT_SUCCEEDED',
  'CONFIRMED',
  'CANCELLED',
];

export const TERMINAL_SAGA_STATES: ReadonlyArray<SagaState> = ['CONFIRMED', 'CANCELLED'];

/**
 * Event names mirror the reply routing keys the orchestrator consumes
 * (envelope.type mirrors ROUTING_KEYS — see contracts/src/messages/envelope.ts)
 * so a consumed envelope's `type` can be fed into nextTransition() directly,
 * with no separate mapping layer. CONFIRM is the one exception: it never
 * appears on the wire, it is synthesized internally (see module doc above).
 */
export const SAGA_EVENTS = {
  INVENTORY_RESERVED: ROUTING_KEYS.INVENTORY_RESERVED,
  INVENTORY_REJECTED: ROUTING_KEYS.INVENTORY_REJECTED,
  PAYMENT_SUCCEEDED: ROUTING_KEYS.PAYMENT_SUCCEEDED,
  PAYMENT_FAILED: ROUTING_KEYS.PAYMENT_FAILED,
  CONFIRM: 'confirm',
} as const;

export type SagaEvent = (typeof SAGA_EVENTS)[keyof typeof SAGA_EVENTS];

export const SAGA_EVENT_VALUES: ReadonlyArray<SagaEvent> = Object.values(SAGA_EVENTS);

/** Command/compensation step names, tied to the command routing keys. */
export const SAGA_STEPS = {
  RESERVE_INVENTORY: ROUTING_KEYS.RESERVE_INVENTORY,
  CHARGE: ROUTING_KEYS.CHARGE,
  RELEASE_INVENTORY: ROUTING_KEYS.RELEASE_INVENTORY,
  REFUND_PAYMENT: ROUTING_KEYS.REFUND_PAYMENT,
} as const;

export type SagaStep = (typeof SAGA_STEPS)[keyof typeof SAGA_STEPS];

/**
 * Compensation for each step that can be "completed" (succeeded) before a
 * later step fails. Reverse-order compensation walks stepHistory's succeeded
 * entries and looks each one up here.
 *
 * REFUND_PAYMENT is structurally wired but unreachable via this state
 * machine: PAYMENT_SUCCEEDED only ever advances to CONFIRMED (there is no
 * step after charge that can fail), so 'charge' never needs compensating.
 * Kept for fidelity to ARCHITECTURE.md's participant contract and in case a
 * future step is added after charge.
 */
export const STEP_COMPENSATIONS: Readonly<Partial<Record<SagaStep, SagaStep>>> = {
  [SAGA_STEPS.RESERVE_INVENTORY]: SAGA_STEPS.RELEASE_INVENTORY,
  [SAGA_STEPS.CHARGE]: SAGA_STEPS.REFUND_PAYMENT,
};

/**
 * When a step's timeout/retry budget is exhausted with no reply at all, the
 * scheduler treats it as the corresponding failure reply would have been
 * treated — looked up here by the step that timed out, so the exact same
 * CANCEL_ORDER transition/compensation logic runs either way.
 */
export const STEP_TIMEOUT_EVENT: Readonly<Partial<Record<SagaStep, SagaEvent>>> = {
  [SAGA_STEPS.RESERVE_INVENTORY]: SAGA_EVENTS.INVENTORY_REJECTED,
  [SAGA_STEPS.CHARGE]: SAGA_EVENTS.PAYMENT_FAILED,
};

export type SagaAction =
  /** Record the just-completed step as succeeded and dispatch the next command. */
  | { readonly kind: 'ISSUE_COMMAND'; readonly step: SagaStep }
  /** Record the just-completed step as succeeded; no new command is issued. */
  | { readonly kind: 'RECORD_ONLY' }
  /** Mark the Order CONFIRMED and publish order-confirmed. No stepHistory change. */
  | { readonly kind: 'CONFIRM_ORDER' }
  /** Mark the Order CANCELLED, publish order-cancelled, and compensate in reverse. */
  | { readonly kind: 'CANCEL_ORDER' };

export interface SagaTransition {
  readonly to: SagaState;
  readonly action: SagaAction;
}

/**
 * The transition table, encoded as one data map — not scattered if/else.
 * Absence of a (state, event) entry means the transition is illegal: the
 * orchestrator treats that uniformly as "ignore" (see applyEvent), which is
 * also exactly the correct behavior for a reply belonging to an
 * already-completed/superseded step.
 */
export const SAGA_TRANSITIONS: Readonly<
  Record<SagaState, Readonly<Partial<Record<SagaEvent, SagaTransition>>>>
> = {
  STARTED: {
    [SAGA_EVENTS.INVENTORY_RESERVED]: {
      to: 'INVENTORY_RESERVED',
      action: { kind: 'ISSUE_COMMAND', step: SAGA_STEPS.CHARGE },
    },
    [SAGA_EVENTS.INVENTORY_REJECTED]: {
      to: 'CANCELLED',
      action: { kind: 'CANCEL_ORDER' },
    },
  },
  INVENTORY_RESERVED: {
    [SAGA_EVENTS.PAYMENT_SUCCEEDED]: {
      to: 'PAYMENT_SUCCEEDED',
      action: { kind: 'RECORD_ONLY' },
    },
    [SAGA_EVENTS.PAYMENT_FAILED]: {
      to: 'CANCELLED',
      action: { kind: 'CANCEL_ORDER' },
    },
  },
  PAYMENT_SUCCEEDED: {
    [SAGA_EVENTS.CONFIRM]: {
      to: 'CONFIRMED',
      action: { kind: 'CONFIRM_ORDER' },
    },
  },
  CONFIRMED: {},
  CANCELLED: {},
};

/** Pure lookup — returns undefined for any illegal (state, event) pair. */
export function nextTransition(state: SagaState, event: SagaEvent): SagaTransition | undefined {
  return SAGA_TRANSITIONS[state]?.[event];
}
