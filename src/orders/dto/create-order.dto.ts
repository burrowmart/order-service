import type { CreateOrderRequest } from '@demo/contracts';
import { ApiProperty } from '@nestjs/swagger';
import { Type } from 'class-transformer';
import {
  ArrayMinSize,
  IsArray,
  IsEmail,
  IsInt,
  IsNumber,
  IsString,
  Min,
  ValidateNested,
} from 'class-validator';

export class CreateOrderItemDto {
  @IsString()
  @ApiProperty({ example: 'WIDGET-1' })
  sku!: string;

  @IsInt()
  @Min(1)
  @ApiProperty({ example: 2, minimum: 1 })
  qty!: number;

  @IsNumber()
  @Min(0)
  @ApiProperty({ example: 9.99, description: 'Unit price captured at order time' })
  price!: number;
}

// Implements the contract interface — the interface is the source of truth;
// this class only adds the NestJS ValidationPipe + Swagger decoration layer.
export class CreateOrderDto implements CreateOrderRequest {
  @IsEmail()
  @ApiProperty({ example: 'alice@example.com', description: 'User email — primary key from user-service' })
  userEmail!: string;

  @IsArray()
  @ArrayMinSize(1)
  @ValidateNested({ each: true })
  @Type(() => CreateOrderItemDto)
  @ApiProperty({ type: [CreateOrderItemDto] })
  items!: CreateOrderItemDto[];
}
