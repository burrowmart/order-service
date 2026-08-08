/**
 * MongoMemoryServer is started by test/global-setup.ts before any file is
 * imported, so MONGO_URI is already in process.env when ConfigModule.forRoot
 * validates the schema at app.module.ts parse time.
 */
import { ConflictException, INestApplication, ValidationPipe } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import request from 'supertest';
import { AppModule } from '../src/app.module';
import { OrdersService } from '../src/orders/orders.service';

describe('OrdersController (e2e)', () => {
  let app: INestApplication;
  let ordersService: OrdersService;

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleFixture.createNestApplication();
    app.useGlobalPipes(
      new ValidationPipe({ whitelist: true, transform: true, forbidNonWhitelisted: true }),
    );
    await app.init();

    ordersService = moduleFixture.get(OrdersService);
  });

  afterAll(async () => {
    await app.close();
  });

  const USER_EMAIL = 'e2e-user@example.com';
  let orderId: string;

  it('POST /orders — creates a PENDING order (201)', () =>
    request(app.getHttpServer())
      .post('/orders')
      .send({
        userEmail: USER_EMAIL,
        items: [
          { sku: 'WIDGET-1', qty: 2, price: 9.99 },
          { sku: 'GADGET-2', qty: 1, price: 5 },
        ],
      })
      .expect(201)
      .expect((res: request.Response) => {
        expect(res.body.id).toBeDefined();
        expect(res.body.userEmail).toBe(USER_EMAIL);
        expect(res.body.status).toBe('PENDING');
        expect(res.body.total).toBeCloseTo(24.98, 2);
        expect(res.body.items).toHaveLength(2);
        expect(res.body.createdAt).toBeDefined();
        orderId = res.body.id;
      }));

  it('POST /orders — 400 on missing userEmail', () =>
    request(app.getHttpServer())
      .post('/orders')
      .send({ items: [{ sku: 'WIDGET-1', qty: 1, price: 1 }] })
      .expect(400));

  it('POST /orders — 400 on empty items array', () =>
    request(app.getHttpServer())
      .post('/orders')
      .send({ userEmail: USER_EMAIL, items: [] })
      .expect(400));

  it('GET /orders/:id — round-trips the created order (200)', () =>
    request(app.getHttpServer())
      .get(`/orders/${orderId}`)
      .expect(200)
      .expect((res: request.Response) => {
        expect(res.body.id).toBe(orderId);
        expect(res.body.userEmail).toBe(USER_EMAIL);
        expect(res.body.total).toBeCloseTo(24.98, 2);
      }));

  it('GET /orders/:id — 404 for unknown id', () =>
    request(app.getHttpServer())
      .get('/orders/000000000000000000000000')
      .expect(404));

  it('GET /orders/:id — 404 for a malformed id', () =>
    request(app.getHttpServer())
      .get('/orders/not-a-valid-object-id')
      .expect(404));

  it('GET /orders?userEmail= — returns paginated orders for the user (200)', () =>
    request(app.getHttpServer())
      .get(`/orders?userEmail=${encodeURIComponent(USER_EMAIL)}`)
      .expect(200)
      .expect((res: request.Response) => {
        expect(res.body.total).toBe(1);
        expect(res.body.data).toHaveLength(1);
        expect(res.body.data[0].id).toBe(orderId);
        expect(res.body.page).toBe(1);
      }));

  it('GET /orders — 400 without userEmail query param', () =>
    request(app.getHttpServer())
      .get('/orders')
      .expect(400));

  it('GET /orders?userEmail= — empty list for a user with no orders', () =>
    request(app.getHttpServer())
      .get(`/orders?userEmail=${encodeURIComponent('nobody@example.com')}`)
      .expect(200)
      .expect((res: request.Response) => {
        expect(res.body.total).toBe(0);
        expect(res.body.data).toHaveLength(0);
      }));

  it('GET /health — returns ok', () =>
    request(app.getHttpServer())
      .get('/health')
      .expect(200)
      .expect({ status: 'ok' }));

  describe('status transitions (OrdersService — no REST endpoint yet, see order-orchestrator TODO)', () => {
    it('allows PENDING -> CONFIRMED', async () => {
      const confirmed = await ordersService.transitionStatus(orderId, 'CONFIRMED');
      expect(confirmed.status).toBe('CONFIRMED');
    });

    it('rejects a transition out of a terminal state (CONFIRMED -> CANCELLED)', async () => {
      await expect(
        ordersService.transitionStatus(orderId, 'CANCELLED'),
      ).rejects.toBeInstanceOf(ConflictException);
    });

    it('rejects transitioning a PENDING order straight to itself (no-op is not a legal transition)', async () => {
      const res = await request(app.getHttpServer())
        .post('/orders')
        .send({ userEmail: USER_EMAIL, items: [{ sku: 'X', qty: 1, price: 1 }] });
      const pendingId = res.body.id;

      await expect(
        ordersService.transitionStatus(pendingId, 'PENDING'),
      ).rejects.toBeInstanceOf(ConflictException);
    });
  });
});
