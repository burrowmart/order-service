import { Injectable, NotFoundException } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { isValidObjectId, Model } from 'mongoose';
import type { ClientSession } from 'mongoose';
import type { OrderStatus } from '@demo/contracts';
import { OrderEntity, OrderDocument } from './schemas/order.schema';
import { CreateOrderDto } from './dto/create-order.dto';

@Injectable()
export class OrdersRepository {
  constructor(
    @InjectModel(OrderEntity.name) private readonly model: Model<OrderDocument>,
  ) {}

  async create(dto: CreateOrderDto, total: number, session?: ClientSession): Promise<OrderDocument> {
    // Array form is required when passing a session option
    const docs = await this.model.create(
      [{ userEmail: dto.userEmail, items: dto.items, total, status: 'PENDING' }],
      session ? { session } : {},
    );
    return docs[0];
  }

  async findAllByUserEmail(
    userEmail: string,
    page: number,
    limit: number,
  ): Promise<{ data: any[]; total: number }> {
    const skip = (page - 1) * limit;
    const filter = { userEmail };
    const [data, total] = await Promise.all([
      this.model.find(filter).sort({ createdAt: -1 }).skip(skip).limit(limit).lean().exec(),
      this.model.countDocuments(filter).exec(),
    ]);
    return { data: data as any[], total };
  }

  async findById(id: string): Promise<OrderDocument> {
    // Invalid ObjectId strings would otherwise throw a Mongoose CastError (500)
    if (!isValidObjectId(id)) throw new NotFoundException(`Order ${id} not found`);
    const doc = await this.model.findById(id).exec();
    if (!doc) throw new NotFoundException(`Order ${id} not found`);
    return doc;
  }

  async updateStatus(
    id: string,
    status: OrderStatus,
    session?: ClientSession,
  ): Promise<OrderDocument> {
    const doc = await this.model
      .findByIdAndUpdate(id, { $set: { status } }, { new: true, session })
      .exec();
    if (!doc) throw new NotFoundException(`Order ${id} not found`);
    return doc;
  }
}
