// TODO(order-orchestrator-session): define the SagaLog Mongoose schema here.
//
// Owned by the order-orchestrator module — intentionally left empty in this
// session. Expected shape once implemented: one document per (sagaId, step)
// recording the saga's command/reply history, keyed for idempotent step
// lookups and crash-resume. See ARCHITECTURE.md "Async + Saga (RabbitMQ,
// orchestration)" and contracts/src/messages/commands.ts + replies.ts for the
// message shapes it will need to record. Collection name: saga_log.
