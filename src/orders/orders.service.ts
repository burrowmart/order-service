import { ConflictException, Injectable } from '@nestjs/common';
import { InjectConnection } from '@nestjs/mongoose';
import { Connection } from 'mongoose';
import type { Order, OrderStatus, Paginated } from '@demo/contracts';
import { OrdersRepository } from './orders.repository';
import { CreateOrderDto } from './dto/create-order.dto';
import { OrchestratorService } from '../order-orchestrator/orchestrator.service';

/**
 * Single source of truth for legal order status transitions.
 * PENDING is the only non-terminal state; CONFIRMED/CANCELLED are terminal —
 * nothing may transition out of them. Enforced exclusively in
 * OrdersService.transitionStatus(); no other code path may write `status`.
 */
const ALLOWED_TRANSITIONS: Record<OrderStatus, ReadonlyArray<OrderStatus>> = {
  PENDING: ['CONFIRMED', 'CANCELLED'],
  CONFIRMED: [],
  CANCELLED: [],
};

@Injectable()
export class OrdersService {
  constructor(
    private readonly repo: OrdersRepository,
    private readonly orchestrator: OrchestratorService,
    @InjectConnection() private readonly conn: Connection,
  ) {}

  async create(dto: CreateOrderDto): Promise<Order> {
    const total = dto.items.reduce((sum, item) => sum + item.qty * item.price, 0);

    const session = await this.conn.startSession();
    let order!: Order;
    try {
      await session.withTransaction(async () => {
        const doc = await this.repo.create(dto, total, session);
        order = this.toResponse(doc);
      });
    } finally {
      await session.endSession();
    }

    // Explicit seam for the order-placement saga orchestrator (next session).
    // See OrchestratorService for what this call is expected to grow into.
    await this.orchestrator.start(order);

    return order;
  }

  async findAllByUserEmail(userEmail: string, page = 1, limit = 20): Promise<Paginated<Order>> {
    const { data, total } = await this.repo.findAllByUserEmail(userEmail, page, limit);
    return { data: data.map((d) => this.toResponse(d)), total, page, limit };
  }

  async findById(id: string): Promise<Order> {
    const doc = await this.repo.findById(id);
    return this.toResponse(doc);
  }

  /**
   * The single choke point for order status changes. Only PENDING->CONFIRMED
   * and PENDING->CANCELLED are legal; every other transition (including
   * terminal->anything and same-state no-ops) is rejected with 409.
   *
   * Not yet wired to a REST endpoint — the order-orchestrator (next session)
   * is the intended caller once it consumes saga replies.
   */
  async transitionStatus(id: string, to: OrderStatus): Promise<Order> {
    const current = await this.repo.findById(id);
    const allowed = ALLOWED_TRANSITIONS[current.status];
    if (!allowed.includes(to)) {
      throw new ConflictException(
        `Cannot transition order ${id} from ${current.status} to ${to}`,
      );
    }
    const doc = await this.repo.updateStatus(id, to);
    return this.toResponse(doc);
  }

  // Strips internal Mongoose fields; handles both hydrated docs and lean objects
  private toResponse(doc: any): Order {
    const plain = typeof doc.toObject === 'function' ? doc.toObject({ versionKey: false }) : doc;
    return {
      id: String(plain._id),
      userEmail: plain.userEmail,
      items: (plain.items ?? []).map((i: any) => ({ sku: i.sku, qty: i.qty, price: i.price })),
      total: plain.total,
      status: plain.status,
      sagaId: plain.sagaId,
      createdAt: plain.createdAt instanceof Date ? plain.createdAt.toISOString() : String(plain.createdAt ?? ''),
      updatedAt: plain.updatedAt instanceof Date ? plain.updatedAt.toISOString() : String(plain.updatedAt ?? ''),
    };
  }
}
