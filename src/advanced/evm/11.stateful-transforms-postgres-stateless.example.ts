/**
 * Two stateful transformers writing to the same Postgres database — Approach A:
 * state is read from Postgres inside transform() and writes are deferred.
 *
 * Pattern: WriteQueue
 * Each transformer in the chain pushes closures into a shared WriteQueue instead
 * of returning mutations directly. drizzleTarget's onData flushes the queue inside
 * its own transaction, so every transformer's writes and the cursor save commit as
 * a single atomic unit. Adding a third (or fourth) transformer requires no changes
 * to onData — it stays a one-liner regardless of chain length.
 *
 * This file shows two transformers for the SQD token on Arbitrum:
 *   1. BalanceTransformer — tracks the running ERC-20 balance per address
 *   2. TransferCountTransformer — tracks the number of sends per address
 *
 * Approach A: stateless transform
 *   - transform() reads current state from the last committed Postgres rows.
 *   - No in-memory state survives between batches.
 *   - fork() is handled automatically: both tables are listed in `tables`, so
 *     drizzleTarget's snapshot mechanism rolls them back on a reorg. No fork()
 *     callback needed in createTransformer.
 *
 * Crash safety: guaranteed — everything commits or rolls back together.
 *
 * Prerequisites: Postgres on localhost:5432 (see docker-compose.yml).
 *
 * Run:
 *   DB_URL=postgresql://postgres:postgres@localhost:5432/postgres \
 *   npx ts-node src/advanced/evm/11.stateful-transforms-postgres-stateless.example.ts
 */

import { createTransformer } from '@subsquid/pipes'
import { commonAbis, evmDecoder, evmPortalStream } from '@subsquid/pipes/evm'
import { batchForInsert, drizzleTarget, type Transaction } from '@subsquid/pipes/targets/drizzle/node-postgres'
import { inArray, sql } from 'drizzle-orm'
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

/** Wraps any payload together with a WriteQueue for the transformer chain. */
type Piped<T> = { payload: T; writes: WriteQueue }

// ── Queue initializer ─────────────────────────────────────────────────────────

/**
 * Creates a fresh WriteQueue for each batch and wraps the payload in Piped<T>.
 * Place this as the first pipe() in any chain that uses WriteQueue so that all
 * downstream transformers receive a uniform Piped<T> input without needing to
 * know whether they are first or last in the chain.
 */
function initQueue<T>() {
  return createTransformer<T, Piped<T>>({
    transform: (data) => ({ payload: data, writes: new WriteQueue() }),
  })
}

// ── Transformer 1: running ERC-20 balances ────────────────────────────────────

/**
 * Reads the current balance for each touched address from Postgres, applies the
 * batch's transfers in order, and enqueues an UPSERT for each modified address.
 */
function createBalanceTransformer(db: ReturnType<typeof drizzle>) {
  return createTransformer<Piped<DecodedBatch>, Piped<DecodedBatch>>({
    transform: async ({ payload, writes }, _ctx) => {
      if (payload.transfers.length === 0) {
        return { payload, writes }
      }

      // Collect every address touched in this batch
      const zero = '0x0000000000000000000000000000000000000000'
      const addresses = new Set<string>()
      for (const t of payload.transfers) {
        const from = t.event.from.toLowerCase()
        const to   = t.event.to.toLowerCase()
        if (from !== zero) addresses.add(from)
        if (to   !== zero) addresses.add(to)
      }

      // Read current balances from the last committed snapshot.
      // Safe: transform() runs after the previous onData transaction committed.
      const existing = await db
        .select()
        .from(tokenBalancesTable)
        .where(inArray(tokenBalancesTable.address, [...addresses]))

      const current = new Map(existing.map(r => [r.address, BigInt(r.balance ?? '0')]))

      // Apply transfers in chronological order
      for (const t of payload.transfers) {
        const from  = t.event.from.toLowerCase()
        const to    = t.event.to.toLowerCase()
        const value = t.event.value
        if (from !== zero) current.set(from, (current.get(from) ?? 0n) - value)
        if (to   !== zero) current.set(to,   (current.get(to)   ?? 0n) + value)
      }

      const rows = [...current.entries()].map(([address, balance]) => ({
        address,
        balance: balance.toString(),
      }))

      writes.push(async (tx) => {
        for (const batch of batchForInsert(rows)) {
          await tx.insert(tokenBalancesTable).values(batch).onConflictDoUpdate({
            target: tokenBalancesTable.address,
            set: { balance: sql`excluded.balance` },
          })
        }
      })

      return { payload, writes }
    },
    // No fork() callback needed: tokenBalancesTable is in drizzleTarget's `tables`,
    // so the snapshot mechanism rolls it back automatically on a reorg.
  })
}

// ── Transformer 2: per-address send count ─────────────────────────────────────

/**
 * Reads the current send count for each sender from Postgres, increments it for
 * each outbound transfer in the batch, and enqueues an UPSERT.
 */
function createTransferCountTransformer(db: ReturnType<typeof drizzle>) {
  return createTransformer<Piped<DecodedBatch>, Piped<DecodedBatch>>({
    transform: async ({ payload, writes }, _ctx) => {
      if (payload.transfers.length === 0) {
        return { payload, writes }
      }

      const zero = '0x0000000000000000000000000000000000000000'
      const senders = new Set<string>()
      for (const t of payload.transfers) {
        const from = t.event.from.toLowerCase()
        if (from !== zero) senders.add(from)
      }

      const existing = await db
        .select()
        .from(transferCountsTable)
        .where(inArray(transferCountsTable.address, [...senders]))

      const current = new Map(existing.map(r => [r.address, r.count ?? 0]))

      for (const t of payload.transfers) {
        const from = t.event.from.toLowerCase()
        if (from !== zero) current.set(from, (current.get(from) ?? 0) + 1)
      }

      const rows = [...current.entries()].map(([address, count]) => ({ address, count }))

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
    // No fork() callback: transferCountsTable is also in `tables`.
  })
}

// ── Main ──────────────────────────────────────────────────────────────────────

const DB_URL  = process.env.DB_URL  ?? 'postgresql://postgres:postgres@localhost:5432/postgres'
const PORTAL  = 'https://portal.sqd.dev/datasets/arbitrum-one'
const SQD_TOKEN = '0x1337420dED5ADb9980CFc35f8f2B054ea86f8aB1'

async function main() {
  const db = drizzle(DB_URL)

  await evmPortalStream({
    id: 'sqd-balances-stateless',
    portal: PORTAL,
    outputs: evmDecoder({
      contracts: [SQD_TOKEN],
      events: { transfers: commonAbis.erc20.events.Transfer },
      range: { from: 194_120_655 },
    }),
  })
    .pipe(initQueue<DecodedBatch>())            // wraps each batch in Piped<T> with a fresh queue
    .pipe(createBalanceTransformer(db))         // adds balance writes to the queue
    .pipe(createTransferCountTransformer(db))   // adds count writes to the same queue
    .pipeTo(
      drizzleTarget({
        db,
        // Both state tables must be listed so drizzleTarget installs snapshot triggers
        // and can roll them back automatically on a blockchain reorg.
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
          // Each transformer's writes execute in the order they were pushed.
          await data.writes.flush(tx)
        },
      }),
    )
}

void main()
