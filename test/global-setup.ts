/**
 * Jest globalSetup — runs once before any test file is loaded.
 * Starts a single-node MongoMemoryReplSet and exposes MONGO_URI via
 * process.env so that ConfigModule's Joi validation finds the variable when
 * app.module.ts is first imported (which happens at module-load time, before
 * beforeAll). Works because --runInBand keeps everything in the same process.
 *
 * A plain MongoMemoryServer starts a standalone mongod, which does NOT
 * support multi-document transactions ("Transaction numbers are only allowed
 * on a replica set member or mongos") — OrdersService.create() uses
 * conn.startSession()/withTransaction(), so a single-node replica set is
 * required even for this in-memory instance.
 */
import { MongoMemoryReplSet } from 'mongodb-memory-server';

export default async function globalSetup(): Promise<void> {
  const mongod = await MongoMemoryReplSet.create({ replSet: { count: 1 } });
  // getUri() already ends in "/?replicaSet=..." — the db name must be passed
  // as its argument (not string-concatenated) or it gets glued onto the query
  // string instead of the path, producing an unparseable replica set name.
  process.env.MONGO_URI = mongod.getUri('order-service-test');
  process.env.PORT = '3001';
  // Disable RabbitMQ/Redis publisher in unit & standard e2e tests
  process.env.OUTBOX_PUBLISHER_ENABLED = 'false';
  // Stash for globalTeardown (same process, same global object)
  (global as any).__MONGOD__ = mongod;
}
