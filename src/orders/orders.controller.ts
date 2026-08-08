import { Controller, Get, Param, Post, Query, Body, BadRequestException } from '@nestjs/common';
import {
  ApiCreatedResponse,
  ApiNotFoundResponse,
  ApiOkResponse,
  ApiTags,
} from '@nestjs/swagger';
import { OrdersService } from './orders.service';
import { CreateOrderDto } from './dto/create-order.dto';

@ApiTags('orders')
@Controller('orders')
export class OrdersController {
  constructor(private readonly service: OrdersService) {}

  @Post()
  @ApiCreatedResponse({ description: 'Order created as PENDING' })
  create(@Body() dto: CreateOrderDto) {
    return this.service.create(dto);
  }

  @Get()
  @ApiOkResponse({ description: 'Paginated orders for a user' })
  findAllByUserEmail(
    @Query('userEmail') userEmail: string,
    @Query('page') page = '1',
    @Query('limit') limit = '20',
  ) {
    if (!userEmail) throw new BadRequestException('userEmail query param is required');
    return this.service.findAllByUserEmail(userEmail, +page, +limit);
  }

  @Get(':id')
  @ApiOkResponse({ description: 'Order found' })
  @ApiNotFoundResponse({ description: 'Order not found' })
  findOne(@Param('id') id: string) {
    return this.service.findById(id);
  }
}
