import { createClient } from '@clickhouse/client'
import { createTransformer, type BatchContext } from '@subsquid/pipes'
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
 * This variant does not handle blockchain forks. It is suitable for historical
 * streams (range.from set to a past block) where the data is already finalized
 * and no forks will occur. For real-time streams that may produce forks, see
 * 09.stateful-transform-on-sqlite.example.ts.
 *
 * Crash recovery: if the process crashes mid-batch, SQLite and ClickHouse may
 * diverge. The `start` callback detects this and fails fast. To recover, wipe
 * the SQLite database and let the pipeline rebuild from scratch (ClickHouse
 * rows will be cleaned up by onRollback before streaming restarts).
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

      const row = db.prepare('SELECT MAX(block_number) as max_block FROM processed_blocks').get() as
        | { max_block: number | null }
      const sqliteLastBlock = row.max_block ?? null
      const stateLastBlock = state.current?.number ?? null

      if (sqliteLastBlock !== stateLastBlock) {
        // SQLite and pipeline state diverged, most likely due to a crash between the SQLite
        // write and the ClickHouse cursor save. There is no delta table in this variant, so
        // automatic recovery is not possible. Wipe the SQLite database and restart.
        throw new Error(
          `State mismatch: SQLite max block ${sqliteLastBlock}, pipeline cursor ${stateLastBlock}. ` +
          `Delete ${SQLITE_DB_PATH} to rebuild from scratch.`
        )
      }

      logger.info({ stateLastBlock }, 'Balance transformer initialized')
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
            balanceRows.push({ address: from, ...logLocation, newBalance: balance.toString() })
          }
          if (to !== '0x0000000000000000000000000000000000000000') {
            let balance = currentBalances.get(to) ?? BigInt(getBalance.get(to)?.balance ?? '0')
            balance += value
            currentBalances.set(to, balance)
            updateBalance.run(to, balance.toString())
            balanceRows.push({ address: to, ...logLocation, newBalance: balance.toString() })
          }
        }

        insertProcessedBlock.run(lastProcessedBlock)
      })()

      return balanceRows
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
    id: 'stateful-sqlite-balances-no-forks',
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
          // onRollback is required even without fork handling: ClickHouse is non-transactional,
          // so a crash between the data insert and the cursor save may leave rows newer than the
          // saved cursor. This fires on every startup to purge any such rows.
          //
          // Note: ClickHouse rollback is handled here, but SQLite state cannot be automatically
          // recovered (no delta table). If SQLite is ahead of the pipeline cursor, `start` will
          // throw and the user must delete the SQLite database to rebuild from scratch.
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
