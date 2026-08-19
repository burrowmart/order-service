import { randomUUID } from 'crypto';
import {
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { SagaLogRepository } from './saga-log.repository';
import { OrchestratorService } from './orchestrator.service';
import { SAGA_EVENTS, STEP_TIMEOUT_EVENT, type SagaStep } from './state-machine';
import type { SagaLogDocument } from './schemas/saga-log.schema';
import { SAGA_STEP_MAX_RETRIES, SAGA_TIMEOUT_SCAN_INTERVAL_MS } from './saga.config';

/**
 * Per-step timeouts, retries, and crash-resume for the order-placement saga.
 *
 * Polls saga_log (rather than a per-saga setTimeout) for two kinds of stale
 * rows, both found by SagaLogRepository.findStale():
 *  - STARTED/INVENTORY_RESERVED sagas whose currentStepDeadlineAt has
 *    passed: retry the command (same sagaId+step, fresh messageId) until
 *    SAGA_STEP_MAX_RETRIES, then synthesize the step's failure event so
 *    the normal CANCEL_ORDER/compensation path runs.
 *  - PAYMENT_SUCCEEDED sagas: the internal CONFIRM step never completed
 *    (e.g. a crash between the PAYMENT_SUCCEEDED and CONFIRMED
 *    transitions). CONFIRM is idempotent, so this is retried unconditionally.
 *
 * A polling scheduler is itself what satisfies "resume on boot, re-arm
 * timeouts": onApplicationBootstrap runs one scan immediately (which finds
 * anything that went stale while the process was down), then starts the
 * interval — there is no per-saga timer state to separately reconstruct.
 */
@Injectable()
export class SagaTimeoutSchedulerService implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly logger = new Logger(SagaTimeoutSchedulerService.name);
  private timer?: NodeJS.Timeout;

  constructor(
    private readonly sagaLogRepo: SagaLogRepository,
    private readonly orchestrator: OrchestratorService,
    private readonly config: ConfigService,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    // Same flag OutboxPublisherService/SagaRepliesConsumerService use — keeps
    // this scheduler (and its RabbitMQ-dependent writes) off in unit/plain-e2e
    // test environments that don't run RabbitMQ.
    if (!this.config.get<boolean>('outboxPublisherEnabled')) {
      this.logger.log('Saga timeout scheduler disabled via OUTBOX_PUBLISHER_ENABLED');
      return;
    }

    const pending = await this.sagaLogRepo.countNonTerminal();
    if (pending > 0) {
      this.logger.log(`Resuming ${pending} non-terminal saga(s) on boot`);
    }

    await this.scan();
    this.timer = setInterval(() => void this.scan(), SAGA_TIMEOUT_SCAN_INTERVAL_MS);
  }

  async scan(): Promise<void> {
    const stale = await this.sagaLogRepo.findStale(new Date());
    for (const saga of stale) {
      await this.handleStale(saga).catch((err: Error) => {
        this.logger.error({ err, sagaId: saga.sagaId }, 'Error handling stale saga — will retry next scan');
      });
    }
  }

  private async handleStale(saga: SagaLogDocument): Promise<void> {
    if (saga.state === 'PAYMENT_SUCCEEDED') {
      await this.orchestrator.applyEvent(saga.sagaId, SAGA_EVENTS.CONFIRM, randomUUID(), {});
      return;
    }

    if (saga.retryCount < SAGA_STEP_MAX_RETRIES) {
      await this.orchestrator.retryCurrentStep(saga);
      return;
    }

    const failureEvent = STEP_TIMEOUT_EVENT[saga.currentStep as SagaStep];
    if (!failureEvent) {
      this.logger.error(
        { sagaId: saga.sagaId, currentStep: saga.currentStep },
        'No timeout-failure event mapped for step — cannot compensate automatically',
      );
      return;
    }
    await this.orchestrator.applyEvent(saga.sagaId, failureEvent, randomUUID(), {
      sagaId: saga.sagaId,
      orderId: saga.orderId,
      reason: `timeout: ${saga.currentStep} exceeded ${SAGA_STEP_MAX_RETRIES} retries`,
    });
  }

  onApplicationShutdown(): void {
    if (this.timer) clearInterval(this.timer);
  }
}
