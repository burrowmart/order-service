import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument } from 'mongoose';
import type { OrderStatus } from '@demo/contracts';
import { MONGO_COLLECTION } from '../../constants';

export type OrderDocument = HydratedDocument<OrderEntity>;

@Schema({ _id: false })
export class OrderItemEntity {
  @Prop({ required: true })
  sku!: string;

  @Prop({ required: true, min: 1 })
  qty!: number;

  @Prop({ required: true, min: 0 })
  price!: number;
}

export const OrderItemSchema = SchemaFactory.createForClass(OrderItemEntity);

@Schema({ timestamps: true, collection: MONGO_COLLECTION })
export class OrderEntity {
  // Not unique — one user places many orders; indexed for GET /orders?userEmail=
  @Prop({ required: true, index: true })
  userEmail!: string;

  @Prop({ type: [OrderItemSchema], required: true })
  items!: OrderItemEntity[];

  // Server-computed as sum(qty * price) at creation time — never trusted from the client
  @Prop({ required: true, min: 0 })
  total!: number;

  @Prop({
    required: true,
    enum: ['PENDING', 'CONFIRMED', 'CANCELLED'],
    default: 'PENDING',
  })
  status!: OrderStatus;

  // Set once the saga is started; the orchestrator (next session) owns clearing it
  // on terminal status. Indexed so the orchestrator can look up in-flight orders by saga.
  @Prop({ index: true })
  sagaId?: string;
}

export const OrderSchema = SchemaFactory.createForClass(OrderEntity);
