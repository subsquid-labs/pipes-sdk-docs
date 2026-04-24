/**
 * Two stateful transformers writing to the same Postgres database — Approach B:
 * state is kept in memory, loaded from Postgres at startup.
 *
 * Pattern: WriteQueue
 * Same WriteQueue pattern as example 11 — each transformer pushes closures into
 * a shared queue, drizzleTarget's onData flushes them all inside one transaction.
 *
 * This file shows two transformers for the SQD token on Arbitrum:
 *   1. BalanceTransformer — tracks the running ERC-20 balance per address
 *   2. TransferCountTransformer — tracks the number of sends per address
 *
 * Approach B: in-memory state
 *   - start() loads the full state from Postgres into a Map once.
 *   - transform() reads/writes the Map — no DB round trip per batch.
 *   - fork() reloads the Map from Postgres after drizzleTarget has already
 *     committed the snapshot-based rollback. The Map then reflects pre-fork state.
 *   - The state Maps must fit in RAM. For very large address spaces use example 11.
 *
 * Crash safety: guaranteed — everything commits or rolls back together.
 * On restart, start() re-reads from Postgres, which is always consistent.
 *
 * Prerequisites: Postgres on localhost:5432 (see docker-compose.yml).
 *
 * Run:
 *   DB_URL=postgresql://postgres:postgres@localhost:5432/postgres \
 *   npx ts-node src/advanced/evm/12.stateful-transforms-postgres-in-memory.example.ts
 */

import { createTransformer } from '@subsquid/pipes'
import { commonAbis, evmDecoder, evmPortalStream } from '@subsquid/pipes/evm'
import { batchForInsert, drizzleTarget, type Transaction } from '@subsquid/pipes/targets/drizzle/node-postgres'
import { sql } from 'drizzle-orm'
import { drizzle } from 'drizzle-orm/node-postgres'
import { integer, numeric, pgTable, varchar } from 'drizzle-orm/pg-core'

// ── Schema ────────────────────────────────────────────────────────────────────

const tokenBalancesTable = pgTable('token_balances', {
  address: varchar('address', { length: 42 }).primaryKey(),
  balance: numeric('balance').notNull().default('0'),
})

const transferCountsTable = pgTable('transfer_counts', {
  address: varchar('address', { length: 42 }).primaryKey(),
  count: integer('count').notNull().default(0),
})

// ── WriteQueue ────────────────────────────────────────────────────────────────

/**
 * Collects database writes from multiple transformers. Each transformer pushes
 * closures; onData flushes them all inside the drizzleTarget transaction.
 *
 * A new instance is created per batch (inside the first transformer's transform())
 * and flows through the rest of the chain via the Piped wrapper.
 */
class WriteQueue {
  private readonly ops: Array<(tx: Transaction) => Promise<void>> = []

  push(op: (tx: Transaction) => Promise<void>): void {
    this.ops.push(op)
  }

  async flush(tx: Transaction): Promise<void> {
    for (const op of this.ops) {
      await op(tx)
    }
  }
}

// ── Pipeline types ────────────────────────────────────────────────────────────

type Transfer = {
  event: { from: string; to: string; value: bigint }
  block: { number: number }
  rawEvent: { logIndex: number; transactionIndex?: number }
}

type DecodedBatch = { transfers: Transfer[] }

/**
 * Wrapper that flows through the transformer chain after the first transformer
 * creates the WriteQueue. Subsequent transformers receive Piped<T> as input,
 * push their writes, and return Piped<T> unchanged.
 */
type Piped<T> = { payload: T; writes: WriteQueue }

// ── Transformer 1: running ERC-20 balances ────────────────────────────────────

/**
 * Keeps the full balance map in memory. start() loads it from Postgres. Each
 * transform() call updates the map in place and enqueues a deduplicated UPSERT
 * (one row per address touched in the batch). fork() reloads the map after
 * drizzleTarget has already restored the pre-fork Postgres state.
 *
 * This transformer is always first: it creates the WriteQueue for the batch.
 */
function createBalanceTransformer(db: ReturnType<typeof drizzle>) {
  const zero = '0x0000000000000000000000000000000000000000'
  let balances = new Map<string, bigint>()

  return createTransformer<DecodedBatch, Piped<DecodedBatch>>({
    start: async ({ logger }) => {
      logger.info('Loading balances from Postgres…')
      const rows = await db.select().from(tokenBalancesTable)
      balances = new Map(rows.map(r => [r.address, BigInt(r.balance ?? '0')]))
      logger.info(`Loaded ${balances.size} balances`)
    },

    transform: async (data, _ctx) => {
      const writes = new WriteQueue()  // fresh queue for this batch

      if (data.transfers.length === 0) {
        return { payload: data, writes }
      }

      // Track which addresses were modified so we can write only the delta.
      const modified = new Set<string>()

      for (const t of data.transfers) {
        const from  = t.event.from.toLowerCase()
        const to    = t.event.to.toLowerCase()
        const value = t.event.value

        if (from !== zero) {
          balances.set(from, (balances.get(from) ?? 0n) - value)
          modified.add(from)
        }
        if (to !== zero) {
          balances.set(to, (balances.get(to) ?? 0n) + value)
          modified.add(to)
        }
      }

      const rows = [...modified].map(address => ({
        address,
        balance: (balances.get(address) ?? 0n).toString(),
      }))

      writes.push(async (tx) => {
        for (const batch of batchForInsert(rows)) {
          await tx.insert(tokenBalancesTable).values(batch).onConflictDoUpdate({
            target: tokenBalancesTable.address,
            set: { balance: sql`excluded.balance` },
          })
        }
      })

      return { payload: data, writes }
    },

    // drizzleTarget commits the snapshot rollback BEFORE this callback fires,
    // so the DB already reflects pre-fork state. Reload the map from there.
    fork: async (_cursor, { logger }) => {
      logger.info('Fork detected — reloading balances from Postgres')
      const rows = await db.select().from(tokenBalancesTable)
      balances = new Map(rows.map(r => [r.address, BigInt(r.balance ?? '0')]))
    },
  })
}

// ── Transformer 2: per-address send count ─────────────────────────────────────

/**
 * Keeps the full send-count map in memory. Same lifecycle as the balance
 * transformer: start loads, transform updates, fork reloads.
 */
function createTransferCountTransformer(db: ReturnType<typeof drizzle>) {
  const zero = '0x0000000000000000000000000000000000000000'
  let counts = new Map<string, number>()

  return createTransformer<Piped<DecodedBatch>, Piped<DecodedBatch>>({
    start: async ({ logger }) => {
      logger.info('Loading transfer counts from Postgres…')
      const rows = await db.select().from(transferCountsTable)
      counts = new Map(rows.map(r => [r.address, r.count ?? 0]))
      logger.info(`Loaded ${counts.size} transfer counts`)
    },

    transform: async ({ payload, writes }, _ctx) => {
      if (payload.transfers.length === 0) {
        return { payload, writes }
      }

      const modified = new Set<string>()

      for (const t of payload.transfers) {
        const from = t.event.from.toLowerCase()
        if (from !== zero) {
          counts.set(from, (counts.get(from) ?? 0) + 1)
          modified.add(from)
        }
      }

      const rows = [...modified].map(address => ({
        address,
        count: counts.get(address) ?? 0,
      }))

      writes.push(async (tx) => {
        for (const batch of batchForInsert(rows)) {
          await tx.insert(transferCountsTable).values(batch).onConflictDoUpdate({
            target: transferCountsTable.address,
            set: { count: sql`excluded.count` },
          })
        }
      })

      return { payload, writes }
    },

    fork: async (_cursor, { logger }) => {
      logger.info('Fork detected — reloading transfer counts from Postgres')
      const rows = await db.select().from(transferCountsTable)
      counts = new Map(rows.map(r => [r.address, r.count ?? 0]))
    },
  })
}

// ── Main ──────────────────────────────────────────────────────────────────────

const DB_URL    = process.env.DB_URL ?? 'postgresql://postgres:postgres@localhost:5432/postgres'
const PORTAL    = 'https://portal.sqd.dev/datasets/arbitrum-one'
const SQD_TOKEN = '0x1337420dED5ADb9980CFc35f8f2B054ea86f8aB1'

async function main() {
  const db = drizzle(DB_URL)

  await evmPortalStream({
    id: 'sqd-balances-in-memory',
    portal: PORTAL,
    outputs: evmDecoder({
      contracts: [SQD_TOKEN],
      events: { transfers: commonAbis.erc20.events.Transfer },
      range: { from: 194_120_655 },
    }),
  })
    .pipe(createBalanceTransformer(db))       // loads balances map, creates WriteQueue
    .pipe(createTransferCountTransformer(db)) // loads counts map, adds to same queue
    .pipeTo(
      drizzleTarget({
        db,
        // Both state tables must be listed so drizzleTarget installs snapshot triggers
        // and rolls them back automatically on a blockchain reorg.
        tables: [tokenBalancesTable, transferCountsTable],
        onStart: async ({ db }) => {
          // For production use drizzle-kit migrations instead of raw DDL here.
          await db.execute(`
            CREATE TABLE IF NOT EXISTS token_balances (
              address varchar(42) PRIMARY KEY,
              balance numeric NOT NULL DEFAULT '0'
            )
          `)
          await db.execute(`
            CREATE TABLE IF NOT EXISTS transfer_counts (
              address varchar(42) PRIMARY KEY,
              count   integer NOT NULL DEFAULT 0
            )
          `)
        },
        onData: async ({ tx, data }) => {
          // Generic flush — works for any number of upstream transformers.
          await data.writes.flush(tx)
        },
      }),
    )
}

void main()
