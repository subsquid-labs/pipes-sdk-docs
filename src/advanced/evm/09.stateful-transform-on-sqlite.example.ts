import { createClient } from '@clickhouse/client'
import { createTransformer, type BatchCtx, type BlockCursor } from '@subsquid/pipes'
import { commonAbis, evmDecoder, evmPortalSource } from '@subsquid/pipes/evm'
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
 * not relying on ClickHouse for state management. State consistency is ensured with
 * an assert.
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
      // Initialize SQLite database
      if (existsSync(SQLITE_DB_PATH)) {
        logger.info('Existing SQLite database found, will resume from last processed block')
      }
      
      db = new Database(SQLITE_DB_PATH)
      
      // Create balances table if it doesn't exist
      db.exec(`
        CREATE TABLE IF NOT EXISTS balances (
          address TEXT NOT NULL PRIMARY KEY,
          balance TEXT NOT NULL DEFAULT '0'
        )
      `)
      
      // Create processed blocks table to track progress
      db.exec(`
        CREATE TABLE IF NOT EXISTS processed_blocks (
          block_number INTEGER NOT NULL PRIMARY KEY
        )
      `)
      
      // Create balance deltas table to track changes per block (for rollback)
      db.exec(`
        CREATE TABLE IF NOT EXISTS balance_deltas (
          address TEXT NOT NULL,
          block_number INTEGER NOT NULL,
          delta TEXT NOT NULL,
          PRIMARY KEY (address, block_number)
        )
      `)
      
      // Get last processed block from SQLite
      const lastBlock = db.prepare('SELECT MAX(block_number) as max_block FROM processed_blocks').get() as { max_block: number | null }
      const sqliteLastBlock = lastBlock.max_block ?? null
      
      // Assert that the initial state matches what's in SQLite
      const stateLastBlock = state.current?.number ?? null
      assert(
        sqliteLastBlock === stateLastBlock,
        `State mismatch: SQLite has last processed block ${sqliteLastBlock}, but transformer state indicates ${stateLastBlock}. ` +
        `This indicates inconsistent state between SQLite and the pipeline state.`
      )
      
      if (sqliteLastBlock !== null) {
        logger.info({ lastProcessedBlock: sqliteLastBlock }, `BalanceTransformer: Resuming from last processed block ${sqliteLastBlock}`)
      }
      
      logger.info('Balance transformer initialized')
    },
    
    transform: async (data: DecodedTransferData, ctx: BatchCtx): Promise<BalanceRow[]> => {
      assert(db, 'BalanceTransformer::transform: Database not initialized')

      const balanceRows: BalanceRow[] = []
      const transfers = data.transfers

      // The block that will become the last processed one from from ctx.state
      const lastProcessedBlock = ctx.state.current?.number ?? null
      
      // If we have data to process but no ctx.state.current, that's an error condition
      assert(
        lastProcessedBlock !== null || transfers.length === 0,
        `State error: Have data to process but ctx.state.current is null. This indicates a pipes SDK bug.`
      )
 
      const getBalance = db.prepare('SELECT balance FROM balances WHERE address = ?')
      const updateBalance = db.prepare('INSERT INTO balances (address, balance) VALUES (?, ?) ON CONFLICT(address) DO UPDATE SET balance = ?')
      const insertDelta = db.prepare('INSERT OR REPLACE INTO balance_deltas (address, block_number, delta) VALUES (?, ?, ?)')
      const insertProcessedBlock = db.prepare('INSERT OR IGNORE INTO processed_blocks (block_number) VALUES (?)')
       
      // All SQLite operations are wrapped in a single transaction for atomicity
      const transaction = db.transaction(() => {
        const currentBalances = new Map<string, bigint>()

        for (const transfer of transfers) {
          const from = transfer.event.from.toLowerCase()
          const to = transfer.event.to.toLowerCase()
          const value = transfer.event.value
          assert(transfer.rawEvent.transactionIndex, `Received transfer with no transactionIndex on block ${transfer.block.number}`)
          const logLocation: LogLocation = {
            block: transfer.block.number,
            transactionIndex: transfer.rawEvent.transactionIndex,
            logIndex: transfer.rawEvent.logIndex
          }
          
          if (from !== '0x0000000000000000000000000000000000000000') {
            let currentBalance = currentBalances.get(from) ||
              BigInt(getBalance.get(from)?.balance || '0')
            currentBalance -= value
            currentBalances.set(from, currentBalance)
            updateBalance.run(from, currentBalance.toString(), currentBalance.toString())
            insertDelta.run(from, logLocation.block, (-value).toString())
            balanceRows.push({
              address: from,
              ...logLocation,
              newBalance: currentBalance.toString()
            })
          }
          if (to !== '0x0000000000000000000000000000000000000000') {
            let currentBalance = currentBalances.get(to) ||
              BigInt(getBalance.get(to)?.balance || '0')
            currentBalance += value
            currentBalances.set(to, currentBalance)
            updateBalance.run(to, currentBalance.toString(), currentBalance.toString())
            insertDelta.run(to, logLocation.block, value.toString())
            balanceRows.push({
              address: to,
              ...logLocation,
              newBalance: currentBalance.toString()
            })
          }
        }
        
        // Mark the last processed block from the batch
        if (lastProcessedBlock !== null) {
          insertProcessedBlock.run(lastProcessedBlock)
        }
      })
      
      // Execute the transaction atomically
      transaction()
      
      return balanceRows
    },
    
    fork: async (cursor: BlockCursor, { logger }) => {
      assert(db, 'BalanceTransformer::fork: Database not initialized')
      
      logger.info({ forkBlock: cursor.number }, 'Handling fork, rolling back balances')
      
      const getCurrentBalance = db.prepare('SELECT balance FROM balances WHERE address = ?')
      const getAllDeltasAfterFork = db.prepare(`
        SELECT address, delta FROM balance_deltas
        WHERE block_number > ?
      `)
      const updateBalance = db.prepare('INSERT INTO balances (address, balance) VALUES (?, ?) ON CONFLICT(address) DO UPDATE SET balance = ?')
      const deleteDeltas = db.prepare('DELETE FROM balance_deltas WHERE block_number > ?')
      const deleteProcessedBlocks = db.prepare('DELETE FROM processed_blocks WHERE block_number > ?')

      // All SQLite operations are wrapped in a single transaction for atomicity
      const transaction = db.transaction(() => {
        const allDeltasAfterFork = getAllDeltasAfterFork.all(cursor.number) as Array<{ address: string; delta: string }>

        const updatedBalances = new Map<string, bigint>()
        for (const { address, delta } of allDeltasAfterFork) {
          let currentBalance = updatedBalances.get(address)
          if (!currentBalance) {
            const dbBalance = getCurrentBalance.get(address)?.balance
            assert(dbBalance, `Balance for address ${address} with a recorded update not found in database while rolling back due to a fork`)
            currentBalance = BigInt(dbBalance)
          }
          currentBalance -= BigInt(delta)
          updatedBalances.set(address, currentBalance)
        }

        for (const [address, balance] of updatedBalances) {
          updateBalance.run(address, balance.toString(), balance.toString())
        }
        
        // Delete deltas and processed blocks after the fork point
        deleteDeltas.run(cursor.number)
        deleteProcessedBlocks.run(cursor.number)
      })
      
      // Execute the transaction atomically
      transaction()
      
      logger.info('Balances rolled back to fork point')
    },
    
    stop: async ({ logger }) => {
      if (db) {
        db.close()
        db = null
        logger.info('Balance transformer stopped, database closed')
      }
    }
  })
}

async function main() {

  const client = createClient({
    username: 'default',
    password: 'default',
    url: 'http://localhost:10123',
  })

  await evmPortalSource({
    portal: 'https://portal.sqd.dev/datasets/arbitrum-one',
  })
    .pipe(
      evmDecoder({
        contracts: [SQD_TOKEN_ADDRESS],
        events: {
          transfers: commonAbis.erc20.events.Transfer,
        },
        range: { from: 194120655 },
      }),
    )
    .pipe(createBalanceTransformer())
    .pipeTo(
      clickhouseTarget({
        client,
        onStart: async ({ store }) => {
          store.command({ query: `
            CREATE TABLE IF NOT EXISTS sqd_balances (
              address          LowCardinality(FixedString(42)),
              block            UInt32 CODEC (DoubleDelta, ZSTD),
              transactionIndex UInt32,
              logIndex         UInt32,
              newBalance       String,
              sign             Int8 DEFAULT 1
            )
            ENGINE = CollapsingMergeTree(sign)
            ORDER BY (block, transactionIndex, logIndex, address);
            `
          })
        },
        onRollback: async ({ type, store, safeCursor }) => {
          try {
            // Query rows that need to be cancelled (blocks after rollback point)
            const rowsToCancel = await client.query({
              query: `
                SELECT address, block, transactionIndex, logIndex, newBalance
                FROM sqd_balances
                WHERE block > ${safeCursor.number} AND sign = 1
              `,
              format: 'JSONEachRow',
            })
            
            type RowType = {
              address: string
              block: number
              transactionIndex: number
              logIndex: number
              newBalance: string
            }
            
            const result: any = await rowsToCancel.json()
            // Flatten if nested, otherwise use as-is
            const data: RowType[] = Array.isArray(result) 
              ? (Array.isArray(result[0]) ? result.flat() : result)
              : []

            if (data.length > 0) {
              // Insert rows with sign=-1 to cancel the deprecated records
              await store.insert({
                table: 'sqd_balances',
                values: data.map((row: RowType) => ({
                  address: row.address,
                  block: row.block,
                  transactionIndex: row.transactionIndex,
                  logIndex: row.logIndex,
                  newBalance: row.newBalance,
                  sign: -1,
                })),
                format: 'JSONEachRow',
              })
              console.log(`ClickhouseTarget onRollback: Cancelled ${data.length} balance records with sign=-1`)
            }
          } catch (err) {
            console.error('onRollback err:', err)
            throw err
          }
        },
        onData: async ({ store, data, ctx }) => {
          if (data.length === 0) return
          
          console.log(`Inserting ${data.length} balance updates`)
          store.insert({
            table: 'sqd_balances',
            values: data.map(row => ({
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
      }),
    )
}

void main()

