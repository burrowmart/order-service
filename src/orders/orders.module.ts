import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { OrdersController } from './orders.controller';
import { OrdersRepository } from './orders.repository';
import { OrdersService } from './orders.service';
import { OrderEntity, OrderSchema } from './schemas/order.schema';
import { OutboxModule } from '../common/outbox/outbox.module';
import { OrderOrchestratorModule } from '../order-orchestrator/order-orchestrator.module';

@Module({
  imports: [
    MongooseModule.forFeature([{ name: OrderEntity.name, schema: OrderSchema }]),
    // Not used by OrdersService.create() yet — the orchestrator (next session)
    // is what will write saga commands/events through this module.
    OutboxModule,
    OrderOrchestratorModule,
  ],
  controllers: [OrdersController],
  providers: [OrdersService, OrdersRepository],
})
export class OrdersModule {}
