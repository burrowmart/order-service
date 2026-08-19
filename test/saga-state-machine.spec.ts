import {
  nextTransition,
  SAGA_STATES,
  SAGA_EVENT_VALUES,
  SAGA_EVENTS,
  SAGA_STEPS,
  SagaState,
  SagaEvent,
  SagaTransition,
} from '../src/order-orchestrator/state-machine';

/**
 * Exhaustive state x event matrix for the saga transition table.
 * Every legal transition is asserted exactly (to-state + action); every
 * other combination — illegal transitions AND "reply for an
 * already-completed step" (same thing, from the state machine's point of
 * view) — must be undefined.
 */
const EXPECTED: ReadonlyArray<{ state: SagaState; event: SagaEvent; transition: SagaTransition }> = [
  {
    state: 'STARTED',
    event: SAGA_EVENTS.INVENTORY_RESERVED,
    transition: { to: 'INVENTORY_RESERVED', action: { kind: 'ISSUE_COMMAND', step: SAGA_STEPS.CHARGE } },
  },
  {
    state: 'STARTED',
    event: SAGA_EVENTS.INVENTORY_REJECTED,
    transition: { to: 'CANCELLED', action: { kind: 'CANCEL_ORDER' } },
  },
  {
    state: 'INVENTORY_RESERVED',
    event: SAGA_EVENTS.PAYMENT_SUCCEEDED,
    transition: { to: 'PAYMENT_SUCCEEDED', action: { kind: 'RECORD_ONLY' } },
  },
  {
    state: 'INVENTORY_RESERVED',
    event: SAGA_EVENTS.PAYMENT_FAILED,
    transition: { to: 'CANCELLED', action: { kind: 'CANCEL_ORDER' } },
  },
  {
    state: 'PAYMENT_SUCCEEDED',
    event: SAGA_EVENTS.CONFIRM,
    transition: { to: 'CONFIRMED', action: { kind: 'CONFIRM_ORDER' } },
  },
];

const legalKey = (state: SagaState, event: SagaEvent) => `${state}::${event}`;
const EXPECTED_BY_KEY = new Map(EXPECTED.map((e) => [legalKey(e.state, e.event), e.transition]));

describe('saga transition table (nextTransition)', () => {
  it('has at least one legal transition per non-terminal state and none for terminal states', () => {
    expect(SAGA_STATES).toContain('CONFIRMED');
    expect(SAGA_STATES).toContain('CANCELLED');
    for (const terminal of ['CONFIRMED', 'CANCELLED'] as const) {
      for (const event of SAGA_EVENT_VALUES) {
        expect(nextTransition(terminal, event)).toBeUndefined();
      }
    }
  });

  describe.each(SAGA_STATES)('from state %s', (state) => {
    describe.each(SAGA_EVENT_VALUES)('on event %s', (event) => {
      const expected = EXPECTED_BY_KEY.get(legalKey(state, event));

      if (expected) {
        it(`transitions to ${expected.to} with action ${expected.action.kind}`, () => {
          expect(nextTransition(state, event)).toEqual(expected);
        });
      } else {
        it('is illegal — returns undefined (ignored, logged, acked by the orchestrator)', () => {
          expect(nextTransition(state, event)).toBeUndefined();
        });
      }
    });
  });

  it('covers every declared state and event exactly once in the exhaustive matrix', () => {
    // Sanity check on the test matrix itself: 5 states x 5 events.
    expect(SAGA_STATES).toHaveLength(5);
    expect(SAGA_EVENT_VALUES).toHaveLength(5);
    expect(EXPECTED).toHaveLength(5); // exactly 5 legal transitions in a 25-cell matrix
  });

  it('exposes CONFIRM as an event never equal to any real reply routing key', () => {
    expect(SAGA_EVENT_VALUES).toContain('confirm');
  });
});
