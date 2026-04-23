import { createClient } from '@clickhouse/client'
import { createTransformer, type BatchContext, type BlockCursor } from '@subsquid/pipes'
import { commonAbis, evmDecoder, evmPortalStream } from '@subsquid/pipes/evm'
import { clickhouseTarget } from '@subsquid/pipes/targets/clickhouse'
import Database from 'better-sqlite3'
import { existsSync } from 'fs'
import assert from 'assert'

/**
 * This example demonstrates how to reconstruct token balances using SQLite
 * for storing the state of the transform. It tracks transfers of SQD
 * (0x1337420dED5ADb9980CFc35f8f2B054ea86f8aB1 on Arbitrum),
 * maintains balances in SQLite, and stores the results to ClickHouse
 * (`sqd_balances` table).
 *
 * The transformer is self-contained and uses SQLite to track intermediate balances,
 * not relying on ClickHouse for state management. The `fork` callback handles both
 * blockchain reorgs and crash recovery (via the `start` callback).
 */

const SQD_TOKEN_ADDRESS = '0x1337420dED5ADb9980CFc35f8f2B054ea86f8aB1'
const SQLITE_DB_PATH = './sqd-balances.sqlite'

interface LogLocation {
  block: number
  transactionIndex: number
  logIndex: number
}

interface BalanceRow {
  address: string
  block: number
  transactionIndex: number
  logIndex: number
  newBalance: string
}

type DecodedTransferData = {
  transfers: Array<{
    event: {
      from: string
      to: string
      value: bigint
    }
    block: {
      number: number
    }
    rawEvent: {
      transactionHash: string
      logIndex: number
      transactionIndex?: number
    }
  }>
}

function createBalanceTransformer() {
  let db: Database.Database | null = null

  // Rolls back SQLite state to `blockNumber` (inclusive).
  // Called from both `start` (crash recovery) and `fork` (blockchain reorg).
  function rollbackTo(blockNumber: number) {
    assert(db, 'rollbackTo: database not initialized')

    const getAllDeltas = db.prepare<[number], { address: string; delta: string }>(
      'SELECT address, delta FROM balance_deltas WHERE block_number > ?'
    )
    const getBalance = db.prepare<[string], { balance: string }>(
      'SELECT balance FROM balances WHERE address = ?'
    )
    const updateBalance = db.prepare(
      'INSERT INTO balances (address, balance) VALUES (?, ?) ON CONFLICT(address) DO UPDATE SET balance = excluded.balance'
    )
    const deleteDeltas = db.prepare('DELETE FROM balance_deltas WHERE block_number > ?')
    const deleteProcessedBlocks = db.prepare('DELETE FROM processed_blocks WHERE block_number > ?')

    db.transaction(() => {
      const deltas = getAllDeltas.all(blockNumber)

      // Net per-address delta across all blocks being undone.
      const net = new Map<string, bigint>()
      for (const { address, delta } of deltas) {
        net.set(address, (net.get(address) ?? 0n) + BigInt(delta))
      }

      for (const [address, delta] of net) {
        const row = getBalance.get(address)
        assert(row, `Balance for ${address} not found while rolling back`)
        const restored = BigInt(row.balance) - delta
        updateBalance.run(address, restored.toString())
      }

      deleteDeltas.run(blockNumber)
      deleteProcessedBlocks.run(blockNumber)
    })()
  }

  return createTransformer<DecodedTransferData, BalanceRow[]>({
    start: async ({ logger, state }) => {
      if (existsSync(SQLITE_DB_PATH)) {
        logger.info('Existing SQLite database found, will resume from last processed block')
      }

      db = new Database(SQLITE_DB_PATH)

      db.exec(`
        CREATE TABLE IF NOT EXISTS balances (
          address TEXT NOT NULL PRIMARY KEY,
          balance TEXT NOT NULL DEFAULT '0'
        )
      `)
      db.exec(`
        CREATE TABLE IF NOT EXISTS processed_blocks (
          block_number INTEGER NOT NULL PRIMARY KEY
        )
      `)
      // Cumulative net delta per (address, block_number), used to reverse state on fork/crash.
      db.exec(`
        CREATE TABLE IF NOT EXISTS balance_deltas (
          address      TEXT    NOT NULL,
          block_number INTEGER NOT NULL,
          delta        TEXT    NOT NULL,
          PRIMARY KEY (address, block_number)
        )
      `)

      const row = db.prepare('SELECT MAX(block_number) as max_block FROM processed_blocks').get() as
        | { max_block: number | null }
      const sqliteLastBlock = row.max_block ?? null
      const stateLastBlock = state.current?.number ?? null

      if (sqliteLastBlock !== null && (stateLastBlock === null || sqliteLastBlock > stateLastBlock)) {
        // SQLite is ahead of the pipeline state. This happens when the process crashed after
        // the SQLite transaction committed but before the ClickHouse target saved the updated
        // cursor. Roll back SQLite to match the pipeline state — this is the transformer-level
        // equivalent of ClickHouse's onRollback({ type: 'offset_check' }).
        logger.info(
          { sqliteLastBlock, stateLastBlock },
          'SQLite ahead of pipeline state (crash recovery) — rolling back'
        )
        rollbackTo(stateLastBlock ?? -1)
      } else if (sqliteLastBlock !== stateLastBlock) {
        throw new Error(
          `State mismatch: SQLite max block ${sqliteLastBlock} < pipeline cursor ${stateLastBlock}. ` +
          'Cannot recover automatically — manual intervention required.'
        )
      } else {
        logger.info({ stateLastBlock }, 'Balance transformer state consistent, resuming')
      }
    },

    transform: async (data: DecodedTransferData, ctx: BatchContext): Promise<BalanceRow[]> => {
      assert(db, 'BalanceTransformer::transform: database not initialized')

      const balanceRows: BalanceRow[] = []
      const transfers = data.transfers

      const lastProcessedBlock = ctx.stream.state.current.number

      const getBalance = db.prepare<[string], { balance: string }>(
        'SELECT balance FROM balances WHERE address = ?'
      )
      const updateBalance = db.prepare(
        'INSERT INTO balances (address, balance) VALUES (?, ?) ON CONFLICT(address) DO UPDATE SET balance = excluded.balance'
      )
      // Accumulate the net delta per (address, block_number) in the table so that
      // rollbackTo() can correctly reverse every block's net effect even when the
      // same address appears in multiple transfers within the same block.
      const upsertDelta = db.prepare(`
        INSERT INTO balance_deltas (address, block_number, delta)
        VALUES (?, ?, ?)
        ON CONFLICT(address, block_number)
        DO UPDATE SET delta = CAST(CAST(delta AS INTEGER) + CAST(excluded.delta AS INTEGER) AS TEXT)
      `)
      const insertProcessedBlock = db.prepare(
        'INSERT OR IGNORE INTO processed_blocks (block_number) VALUES (?)'
      )

      db.transaction(() => {
        const currentBalances = new Map<string, bigint>()

        for (const transfer of transfers) {
          const from = transfer.event.from.toLowerCase()
          const to = transfer.event.to.toLowerCase()
          const value = transfer.event.value
          assert(
            transfer.rawEvent.transactionIndex !== undefined,
            `Received transfer with no transactionIndex on block ${transfer.block.number}`
          )
          const logLocation: LogLocation = {
            block: transfer.block.number,
            transactionIndex: transfer.rawEvent.transactionIndex,
            logIndex: transfer.rawEvent.logIndex,
          }

          if (from !== '0x0000000000000000000000000000000000000000') {
            let balance = currentBalances.get(from) ?? BigInt(getBalance.get(from)?.balance ?? '0')
            balance -= value
            currentBalances.set(from, balance)
            updateBalance.run(from, balance.toString())
            upsertDelta.run(from, logLocation.block, (-value).toString())
            balanceRows.push({ address: from, ...logLocation, newBalance: balance.toString() })
          }
          if (to !== '0x0000000000000000000000000000000000000000') {
            let balance = currentBalances.get(to) ?? BigInt(getBalance.get(to)?.balance ?? '0')
            balance += value
            currentBalances.set(to, balance)
            updateBalance.run(to, balance.toString())
            upsertDelta.run(to, logLocation.block, value.toString())
            balanceRows.push({ address: to, ...logLocation, newBalance: balance.toString() })
          }
        }

        insertProcessedBlock.run(lastProcessedBlock)
      })()

      return balanceRows
    },

    fork: async (cursor: BlockCursor, { logger }) => {
      logger.info({ forkBlock: cursor.number }, 'Handling fork, rolling back SQLite balances')
      rollbackTo(cursor.number)
      logger.info('SQLite balances rolled back to fork point')
    },

    stop: async ({ logger }) => {
      if (db) {
        db.close()
        db = null
        logger.info('Balance transformer stopped, database closed')
      }
    },
  })
}

async function main() {
  const client = createClient({
    username: 'default',
    password: 'default',
    url: 'http://localhost:10123',
  })

  await evmPortalStream({
    id: 'stateful-sqlite-balances',
    portal: 'https://portal.sqd.dev/datasets/arbitrum-one',
    outputs: evmDecoder({
      contracts: [SQD_TOKEN_ADDRESS],
      events: {
        transfers: commonAbis.erc20.events.Transfer,
      },
      range: { from: 194120655 },
    }),
  })
    .pipe(createBalanceTransformer())
    .pipeTo(
      clickhouseTarget({
        client,
        onStart: async ({ store }) => {
          store.command({
            query: `
              CREATE TABLE IF NOT EXISTS sqd_balances (
                address          LowCardinality(FixedString(42)),
                block            UInt32 CODEC (DoubleDelta, ZSTD),
                transactionIndex UInt32,
                logIndex         UInt32,
                newBalance       String,
                sign             Int8 DEFAULT 1
              )
              ENGINE = CollapsingMergeTree(sign)
              ORDER BY (block, transactionIndex, logIndex, address)
            `,
          })
        },
        onRollback: async ({ store, safeCursor }) => {
          // Remove ClickHouse rows written after the safe cursor.
          // SQLite rollback is handled by the transformer's fork/start callbacks.
          const result = await store.removeAllRows({
            tables: ['sqd_balances'],
            where: `block > {latest:UInt32}`,
            params: { latest: safeCursor.number },
          })
          console.log(`Removed ${result[0]?.count ?? 0} rows from sqd_balances`)
        },
        onData: async ({ store, data }) => {
          if (data.length === 0) return
          console.log(`Inserting ${data.length} balance updates`)
          store.insert({
            table: 'sqd_balances',
            values: data.map((row) => ({
              address: row.address,
              block: row.block,
              transactionIndex: row.transactionIndex,
              logIndex: row.logIndex,
              newBalance: row.newBalance,
              sign: 1,
            })),
            format: 'JSONEachRow',
          })
        },
      })
    )
}

void main()
