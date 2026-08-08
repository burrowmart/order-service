import { Module } from '@nestjs/common';
import { OrchestratorService } from './orchestrator.service';

// TODO(order-orchestrator-session): this module is intentionally empty.
// The real saga state machine (RabbitMQ command dispatch + reply consumption,
// compensations, saga_log persistence — see schemas/saga-log.schema.ts) is
// built in the next session. For now it only exposes the OrchestratorService
// stub that OrdersModule calls as its explicit seam.
@Module({
  providers: [OrchestratorService],
  exports: [OrchestratorService],
})
export class OrderOrchestratorModule {}
