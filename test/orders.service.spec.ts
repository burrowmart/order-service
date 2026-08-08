import { ConflictException, NotFoundException } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { getConnectionToken } from '@nestjs/mongoose';
import { OrdersRepository } from '../src/orders/orders.repository';
import { OrdersService } from '../src/orders/orders.service';
import { OrchestratorService } from '../src/order-orchestrator/orchestrator.service';

const MOCK_ORDER = {
  _id: '507f1f77bcf86cd799439011',
  userEmail: 'alice@example.com',
  items: [{ sku: 'WIDGET-1', qty: 2, price: 9.99 }],
  total: 19.98,
  status: 'PENDING',
  createdAt: new Date('2024-01-01T00:00:00.000Z'),
  updatedAt: new Date('2024-01-01T00:00:00.000Z'),
};

const mockRepo: jest.Mocked<OrdersRepository> = {
  create: jest.fn(),
  findAllByUserEmail: jest.fn(),
  findById: jest.fn(),
  updateStatus: jest.fn(),
} as any;

const mockOrchestrator = { start: jest.fn().mockResolvedValue(undefined) };

// Minimal Mongoose session mock that just calls the transaction function
const mockSession = {
  withTransaction: jest.fn().mockImplementation((fn: () => Promise<void>) => fn()),
  endSession: jest.fn().mockResolvedValue(undefined),
};
const mockConn = {
  startSession: jest.fn().mockResolvedValue(mockSession),
};

describe('OrdersService', () => {
  let service: OrdersService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OrdersService,
        { provide: OrdersRepository, useValue: mockRepo },
        { provide: OrchestratorService, useValue: mockOrchestrator },
        { provide: getConnectionToken(), useValue: mockConn },
      ],
    }).compile();

    service = module.get<OrdersService>(OrdersService);
    jest.clearAllMocks();
  });

  describe('create', () => {
    it('computes total as sum(qty * price) and returns the created order', async () => {
      mockRepo.create.mockResolvedValue(MOCK_ORDER as any);
      const result = await service.create({
        userEmail: MOCK_ORDER.userEmail,
        items: [{ sku: 'WIDGET-1', qty: 2, price: 9.99 }],
      });
      expect(mockRepo.create).toHaveBeenCalledWith(
        expect.objectContaining({ userEmail: MOCK_ORDER.userEmail }),
        19.98,
        expect.anything(),
      );
      expect(result.userEmail).toBe(MOCK_ORDER.userEmail);
      expect(result.status).toBe('PENDING');
    });

    it('calls the orchestrator seam after persisting the order', async () => {
      mockRepo.create.mockResolvedValue(MOCK_ORDER as any);
      await service.create({
        userEmail: MOCK_ORDER.userEmail,
        items: [{ sku: 'WIDGET-1', qty: 2, price: 9.99 }],
      });
      expect(mockOrchestrator.start).toHaveBeenCalledTimes(1);
      expect(mockOrchestrator.start).toHaveBeenCalledWith(
        expect.objectContaining({ userEmail: MOCK_ORDER.userEmail, status: 'PENDING' }),
      );
    });
  });

  describe('findAllByUserEmail', () => {
    it('returns paginated results', async () => {
      mockRepo.findAllByUserEmail.mockResolvedValue({ data: [MOCK_ORDER], total: 1 });
      const result = await service.findAllByUserEmail(MOCK_ORDER.userEmail, 1, 20);
      expect(result.data).toHaveLength(1);
      expect(result.total).toBe(1);
      expect(result.page).toBe(1);
      expect(result.limit).toBe(20);
    });
  });

  describe('findById', () => {
    it('returns the order when found', async () => {
      mockRepo.findById.mockResolvedValue(MOCK_ORDER as any);
      const result = await service.findById(MOCK_ORDER._id);
      expect(result.id).toBe(MOCK_ORDER._id);
    });

    it('propagates NotFoundException from repository', async () => {
      mockRepo.findById.mockRejectedValue(new NotFoundException());
      await expect(service.findById('unknown')).rejects.toBeInstanceOf(NotFoundException);
    });
  });

  describe('transitionStatus', () => {
    it('allows PENDING -> CONFIRMED', async () => {
      mockRepo.findById.mockResolvedValue({ ...MOCK_ORDER, status: 'PENDING' } as any);
      mockRepo.updateStatus.mockResolvedValue({ ...MOCK_ORDER, status: 'CONFIRMED' } as any);
      const result = await service.transitionStatus(MOCK_ORDER._id, 'CONFIRMED');
      expect(result.status).toBe('CONFIRMED');
      expect(mockRepo.updateStatus).toHaveBeenCalledWith(MOCK_ORDER._id, 'CONFIRMED');
    });

    it('allows PENDING -> CANCELLED', async () => {
      mockRepo.findById.mockResolvedValue({ ...MOCK_ORDER, status: 'PENDING' } as any);
      mockRepo.updateStatus.mockResolvedValue({ ...MOCK_ORDER, status: 'CANCELLED' } as any);
      const result = await service.transitionStatus(MOCK_ORDER._id, 'CANCELLED');
      expect(result.status).toBe('CANCELLED');
    });

    it('rejects CONFIRMED -> CANCELLED (terminal state)', async () => {
      mockRepo.findById.mockResolvedValue({ ...MOCK_ORDER, status: 'CONFIRMED' } as any);
      await expect(
        service.transitionStatus(MOCK_ORDER._id, 'CANCELLED'),
      ).rejects.toBeInstanceOf(ConflictException);
      expect(mockRepo.updateStatus).not.toHaveBeenCalled();
    });

    it('rejects CANCELLED -> CONFIRMED (terminal state)', async () => {
      mockRepo.findById.mockResolvedValue({ ...MOCK_ORDER, status: 'CANCELLED' } as any);
      await expect(
        service.transitionStatus(MOCK_ORDER._id, 'CONFIRMED'),
      ).rejects.toBeInstanceOf(ConflictException);
    });

    it('rejects PENDING -> PENDING (no-op is not a legal transition)', async () => {
      mockRepo.findById.mockResolvedValue({ ...MOCK_ORDER, status: 'PENDING' } as any);
      await expect(
        service.transitionStatus(MOCK_ORDER._id, 'PENDING'),
      ).rejects.toBeInstanceOf(ConflictException);
    });
  });
});
