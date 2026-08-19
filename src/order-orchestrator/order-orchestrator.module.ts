import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { OutboxModule } from '../common/outbox/outbox.module';
// Read-only import — registers the orders collection a second time via the
// identical schema object (safe: Mongoose reuses the compiled model for a
// same-reference schema) so this module can write Order.status/sagaId
// atomically alongside its own saga_log/outbox writes, without modifying
// src/orders/ (out of scope for this session).
import { OrderEntity, OrderSchema } from '../orders/schemas/order.schema';
import { SagaLogEntity, SagaLogSchema } from './schemas/saga-log.schema';
import { SagaLogRepository } from './saga-log.repository';
import { OrchestratorService } from './orchestrator.service';
import { SagaRepliesConsumerService } from './saga-replies.consumer';
import { SagaTimeoutSchedulerService } from './saga-timeout-scheduler.service';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: SagaLogEntity.name, schema: SagaLogSchema },
      { name: OrderEntity.name, schema: OrderSchema },
    ]),
    OutboxModule,
  ],
  providers: [
    SagaLogRepository,
    OrchestratorService,
    SagaRepliesConsumerService,
    SagaTimeoutSchedulerService,
  ],
  exports: [OrchestratorService],
})
export class OrderOrchestratorModule {}
