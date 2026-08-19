/**
 * Env-configurable knobs for saga step timeouts/retries, read directly from
 * process.env (rather than src/config/*, which is out of scope for this
 * module) with the same "parse once at import time, sane default" approach
 * configuration.ts uses elsewhere in this service.
 */
export const SAGA_STEP_TIMEOUT_MS = parseInt(process.env.SAGA_STEP_TIMEOUT_MS ?? '30000', 10);
export const SAGA_STEP_MAX_RETRIES = parseInt(process.env.SAGA_STEP_MAX_RETRIES ?? '3', 10);
export const SAGA_TIMEOUT_SCAN_INTERVAL_MS = parseInt(
  process.env.SAGA_TIMEOUT_SCAN_INTERVAL_MS ?? '10000',
  10,
);
/** Retries for a reply *handler* throwing (e.g. a transient Mongo error) — distinct from SAGA_STEP_MAX_RETRIES, which covers a step that never got a reply at all. */
export const SAGA_REPLY_CONSUMER_MAX_RETRIES = parseInt(
  process.env.SAGA_REPLY_CONSUMER_MAX_RETRIES ?? '3',
  10,
);
