import { Injectable, Logger } from '@nestjs/common';
import type { Order } from '@demo/contracts';

/**
 * Explicit seam for the order-placement saga orchestrator.
 *
 * TODO(order-orchestrator-session): replace this stub with the real
 * implementation — inside the same Mongo transaction that persists the
 * order, outbox-write a ReserveInventoryPayload command (see
 * contracts/src/messages/commands.ts) to EXCHANGES.SAGA_COMMANDS, persist a
 * saga_log row, then consume the four reply queues
 * (INVENTORY_RESERVED/REJECTED, PAYMENT_SUCCEEDED/FAILED — see
 * contracts/src/messages/queue-names.ts) to advance the order's status via
 * OrdersService's transition guard, running compensations
 * (release-inventory / refund-payment) in reverse on any failure.
 *
 * OrdersService.create() calls this after persisting the order as PENDING;
 * it does not yet do anything beyond logging.
 */
@Injectable()
export class OrchestratorService {
  private readonly logger = new Logger(OrchestratorService.name);

  async start(order: Order): Promise<void> {
    this.logger.log(
      { orderId: order.id, userEmail: order.userEmail },
      'TODO(order-orchestrator-session): saga not yet implemented — order remains PENDING until the orchestrator is built',
    );
  }
}
