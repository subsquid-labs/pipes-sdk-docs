/**
 * Stateful in-RAM transformer: rolling ~1-hour SQD transfer volume on Arbitrum.
 *
 * Pattern: bounded in-RAM state with portal warm-up
 * The transformer keeps a deque of recent Transfer events in memory bounded to
 * the last LOOKBACK_BLOCKS blocks (~1 hour on Arbitrum at 250 ms/block). On
 * startup, start() queries the portal directly for that window of history and
 * rebuilds the deque — so the rolling total is accurate from the first batch
 * after a restart, no matter how the process stopped.
 *
 * Warm-up uses portal.getStream() (the raw portal client API). Each batch,
 * entries older than LOOKBACK_BLOCKS are evicted and a single summary row is
 * written to ClickHouse.
 *
 * When to use this pattern:
 *   - State can be derived from a bounded window of recent blocks.
 *   - A warm-up replay of LOOKBACK_BLOCKS is cheap enough to run on restart.
 *   - State grows without bound → use the Postgres/Drizzle approach instead
 *     (examples 11 and 12).
 *
 * Composability: same initQueue/WriteQueue pattern as examples 11 and 12, adapted
 * for ClickhouseStore instead of a Postgres Transaction. Multiple in-RAM
 * transformers can be chained; each pushes to the shared WriteQueue and onData
 * flushes them all.
 *
 * Fork handling: fork() drops entries whose blockNumber exceeds the rollback cursor.
 * The next batch rebuilds the window from the re-fetched blocks.
 *
 * Prerequisites: ClickHouse on localhost:10123 (see docker-compose.yml).
 *
 * Run:
 *   npx ts-node src/advanced/evm/13.stateful-transform-in-ram.example.ts
 */

import { createClient } from '@clickhouse/client'
import { createTransformer } from '@subsquid/pipes'
import { api, commonAbis, evmDecoder, evmPortalStream } from '@subsquid/pipes/evm'
import { clickhouseTarget } from '@subsquid/pipes/targets/clickhouse'

// ── Schema ────────────────────────────────────────────────────────────────────

// Structural alias matching the surface of ClickhouseStore used here.
// Avoids importing the class directly while remaining fully type-safe.
type CHS = {
  insert(params: { table: string; values: unknown[]; format: string }): Promise<unknown>
}

// ── WriteQueue ────────────────────────────────────────────────────────────────

/**
 * Collects ClickHouse inserts from multiple transformers. onData flushes them all
 * inside a single onData call, so all writes for a batch go out together.
 * Each transformer pushes closures that receive the ClickhouseStore at flush time.
 */
class WriteQueue {
  private readonly ops: Array<(store: CHS) => Promise<void>> = []

  push(op: (store: CHS) => Promise<void>): void {
    this.ops.push(op)
  }

  async flush(store: CHS): Promise<void> {
    for (const op of this.ops) {
      await op(store)
    }
  }
}

// ── Pipeline types ────────────────────────────────────────────────────────────

type Transfer = {
  event: { from: string; to: string; value: bigint }
  block: { number: number }
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

// ── Constants ─────────────────────────────────────────────────────────────────

const SQD_TOKEN     = '0x1337420dED5ADb9980CFc35f8f2B054ea86f8aB1'
const PORTAL        = 'https://portal.sqd.dev/datasets/arbitrum-one'
const FIRST_BLOCK   = 194_120_655

// Arbitrum produces a block every ~250 ms → ~14 400 blocks per hour.
// The warm-up replay covers at most this many blocks on restart.
const LOOKBACK_BLOCKS = 14_400

// ERC-20 Transfer(address indexed from, address indexed to, uint256 value)
const TRANSFER_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' as const

// ── Warm-up query type ────────────────────────────────────────────────────────

// Fields requested from the portal during warm-up. Only block.number and log.data
// are needed to rebuild the in-RAM deque.
type WarmupFields = { block: { number: true }; log: { data: true } }
type WarmupQuery  = api.Query<WarmupFields>

// ── Transformer: rolling ~1-hour transfer volume ──────────────────────────────

type TransferEntry = { blockNumber: number; value: bigint }

/**
 * Maintains a sliding deque of Transfer events for the last LOOKBACK_BLOCKS.
 *
 * start()  — on restart, replays the warm-up window from the portal so the
 *             rolling total is accurate immediately, without waiting for the
 *             next LOOKBACK_BLOCKS of live data.
 *
 * transform() — adds new entries, evicts stale ones, writes a summary row.
 *
 * fork()   — drops entries for rolled-back blocks; the correct entries are
 *             re-added as the stream replays from the rollback cursor.
 */
function createVolumeTransformer() {
  let recentTransfers: TransferEntry[] = []

  return createTransformer<Piped<DecodedBatch>, Piped<DecodedBatch>>({
    start: async ({ portal, state, logger }) => {
      // No warm-up needed on the very first run (state.current is undefined).
      if (!state.current) return

      const warmupFrom = Math.max(state.initial, state.current.number - LOOKBACK_BLOCKS)
      if (warmupFrom >= state.current.number) return

      logger.info(
        { warmupFrom, current: state.current.number },
        'Warming up transfer window from portal…',
      )

      // Query the portal directly for the warm-up window using the raw client API.
      // This is independent of the main stream and completes before the first batch.
      const warmupQuery: WarmupQuery = {
        type: 'evm',
        fromBlock: warmupFrom,
        toBlock: state.current.number,
        fields: { block: { number: true }, log: { data: true } },
        logs: [{ address: [SQD_TOKEN], topic0: [TRANSFER_TOPIC] }],
      }

      for await (const { blocks } of portal.getStream(warmupQuery)) {
        for (const block of blocks) {
          for (const log of block.logs) {
            // Transfer data is ABI-encoded uint256: 32 bytes, no other fields.
            recentTransfers.push({ blockNumber: block.header.number, value: BigInt(log.data) })
          }
        }
      }

      // Evict entries outside the window (portal may return slightly more than needed).
      recentTransfers = recentTransfers.filter(
        (e) => e.blockNumber > state.current!.number - LOOKBACK_BLOCKS,
      )

      logger.info({ transfers: recentTransfers.length }, 'Warm-up complete')
    },

    transform: async ({ payload, writes }, ctx) => {
      const latestBlock = ctx.stream.state.current.number

      for (const t of payload.transfers) {
        recentTransfers.push({ blockNumber: t.block.number, value: t.event.value })
      }

      // Evict entries that have fallen outside the rolling window.
      recentTransfers = recentTransfers.filter(
        (e) => e.blockNumber > latestBlock - LOOKBACK_BLOCKS,
      )

      const rollingVolume = recentTransfers.reduce((sum, e) => sum + e.value, 0n)

      const row = { block_number: latestBlock, rolling_volume: rollingVolume.toString() }
      writes.push(async (store) => {
        await store.insert({ table: 'sqd_rolling_volume', values: [row], format: 'JSONEachRow' })
      })

      return { payload, writes }
    },

    // target.fork() (ClickHouse onRollback) fires BEFORE this callback,
    // so ClickHouse rows for rolled-back blocks are already removed when we get here.
    fork: async (cursor, { logger }) => {
      const before = recentTransfers.length
      recentTransfers = recentTransfers.filter((e) => e.blockNumber <= cursor.number)
      logger.info(
        { removed: before - recentTransfers.length, cursor: cursor.number },
        'Fork: pruned in-RAM transfer window',
      )
    },
  })
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  const client = createClient({
    username: 'default',
    password: 'default',
    url: 'http://localhost:10123',
  })

  await evmPortalStream({
    id: 'sqd-rolling-volume',
    portal: PORTAL,
    outputs: evmDecoder({
      contracts: [SQD_TOKEN],
      events: { transfers: commonAbis.erc20.events.Transfer },
      range: { from: FIRST_BLOCK },
    }),
  })
    .pipe(initQueue<DecodedBatch>())   // wraps each batch in Piped<T> with a fresh queue
    .pipe(createVolumeTransformer())   // maintains in-RAM window, pushes summary row
    .pipeTo(
      clickhouseTarget({
        client,
        onStart: async ({ store }) => {
          // For production use the SQL file approach (store.executeFiles) instead.
          await store.command({
            query: `
              CREATE TABLE IF NOT EXISTS sqd_rolling_volume (
                block_number  UInt32,
                rolling_volume String
              )
              ENGINE = ReplacingMergeTree()
              ORDER BY block_number
            `,
          })
        },
        onRollback: async ({ store, safeCursor }) => {
          await store.command({
            query: `
              ALTER TABLE sqd_rolling_volume
              DELETE WHERE block_number > ${safeCursor.number}
            `,
          })
        },
        onData: async ({ store, data }) => {
          // Generic flush — works for any number of upstream transformers.
          await data.writes.flush(store)
        },
      }),
    )
}

void main()
