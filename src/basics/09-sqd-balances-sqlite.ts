import { createClient } from '@clickhouse/client'
import { createTransformer, type BatchCtx, type BlockCursor } from '@subsquid/pipes'
import { commonAbis, evmDecoder, evmPortalSource } from '@subsquid/pipes/evm'
import { clickhouseTarget } from '@subsquid/pipes/targets/clickhouse'
import Database from 'better-sqlite3'
import { existsSync } from 'fs'

/**
 * This example demonstrates how to reconstruct token balances using SQLite for state management.
 * It tracks SQD token (0x1337420dED5ADb9980CFc35f8f2B054ea86f8aB1 on Arbitrum) transfers,
 * maintains balances in SQLite, and stores the results (address, block, newBalance) to ClickHouse.
 * 
 * The transformer is self-contained and uses SQLite to track intermediate balances,
 * not relying on ClickHouse for state management.
 */

const SQD_TOKEN_ADDRESS = '0x1337420dED5ADb9980CFc35f8f2B054ea86f8aB1'
const SQLITE_DB_PATH = './sqd-balances.sqlite'

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
      if (sqliteLastBlock !== stateLastBlock) {
        throw new Error(
          `State mismatch: SQLite has last processed block ${sqliteLastBlock}, but transformer state indicates ${stateLastBlock}. ` +
          `This indicates inconsistent state between SQLite and the pipeline state.`
        )
      }
      
      if (sqliteLastBlock !== null) {
        logger.info({ lastProcessedBlock: sqliteLastBlock }, `BalanceTransformer: Resuming from last processed block ${sqliteLastBlock}`)
      }
      
      logger.info('Balance transformer initialized')
    },
    
    transform: async (data: DecodedTransferData, ctx: BatchCtx): Promise<BalanceRow[]> => {
      if (!db) {
        throw new Error('Database not initialized')
      }

      const balanceRows: BalanceRow[] = []
      
      // Processing transfers in block order
      const transfers = data.transfers
      const transfersByBlock = new Map<number, typeof transfers>()
      for (const transfer of transfers) {
        const blockNum = transfer.block.number
        if (!transfersByBlock.has(blockNum)) {
          transfersByBlock.set(blockNum, [])
        }
        transfersByBlock.get(blockNum)!.push(transfer)
      }
      const sortedBlocks = Array.from(transfersByBlock.keys()).sort((a, b) => a - b)

      // Getting the block that will become the last processed one from from ctx.state
      const lastProcessedBlock = ctx.state.current?.number ?? null
      
      // If we have data to process but no state, that's an error condition
      if (sortedBlocks.length > 0 && lastProcessedBlock === null) {
        throw new Error(
          `State error: Have data to process (blocks ${Math.min(...sortedBlocks)}-${Math.max(...sortedBlocks)}) ` +
          `but ctx.state.current is null. This indicates a state management issue.`
        )
      }
 
      const getBalance = db.prepare('SELECT balance FROM balances WHERE address = ?')
      const updateBalance = db.prepare('INSERT INTO balances (address, balance) VALUES (?, ?) ON CONFLICT(address) DO UPDATE SET balance = ?')
      const insertDelta = db.prepare('INSERT OR REPLACE INTO balance_deltas (address, block_number, delta) VALUES (?, ?, ?)')
      const insertProcessedBlock = db.prepare('INSERT OR IGNORE INTO processed_blocks (block_number) VALUES (?)')
       
      // All SQLite operations are wrapped in a single transaction for atomicity
      const transaction = db.transaction(() => {
        for (const blockNum of sortedBlocks) {
          const blockTransfers = transfersByBlock.get(blockNum)!
          
          // Track addresses that changed in this block and aggregate deltas
          // Also track the maximum transactionIndex and logIndex for each address
          const changedAddresses = new Set<string>()
          const addressDeltas = new Map<string, bigint>()
          const addressTransactionIndex = new Map<string, number>()
          const addressLogIndex = new Map<string, number>()
          
          // First pass: calculate all deltas for this block and track transaction indices and log indices
          for (const transfer of blockTransfers) {
            const from = transfer.event.from.toLowerCase()
            const to = transfer.event.to.toLowerCase()
            const value = transfer.event.value
            // Use transactionIndex if available, otherwise use logIndex as fallback
            const txIndex = transfer.rawEvent.transactionIndex ?? transfer.rawEvent.logIndex
            const logIdx = transfer.rawEvent.logIndex
            
            if (from !== '0x0000000000000000000000000000000000000000') {
              const currentDelta = addressDeltas.get(from) || 0n
              addressDeltas.set(from, currentDelta - value)
              changedAddresses.add(from)
              // Track maximum transactionIndex and logIndex for this address
              const currentMaxTx = addressTransactionIndex.get(from) ?? -1
              addressTransactionIndex.set(from, Math.max(currentMaxTx, txIndex))
              const currentMaxLog = addressLogIndex.get(from) ?? -1
              addressLogIndex.set(from, Math.max(currentMaxLog, logIdx))
            }
            
            if (to !== '0x0000000000000000000000000000000000000000') {
              const currentDelta = addressDeltas.get(to) || 0n
              addressDeltas.set(to, currentDelta + value)
              changedAddresses.add(to)
              // Track maximum transactionIndex and logIndex for this address
              const currentMaxTx = addressTransactionIndex.get(to) ?? -1
              addressTransactionIndex.set(to, Math.max(currentMaxTx, txIndex))
              const currentMaxLog = addressLogIndex.get(to) ?? -1
              addressLogIndex.set(to, Math.max(currentMaxLog, logIdx))
            }
          }
          
          // Second pass: apply deltas and update balances
          for (const [address, delta] of addressDeltas) {
            const row = getBalance.get(address) as { balance: string } | undefined
            const currentBalance = BigInt(row?.balance || '0')
            const newBalance = currentBalance + delta
            updateBalance.run(address, newBalance.toString(), newBalance.toString())
            insertDelta.run(address, blockNum, delta.toString())
          }
          
          // Record balance changes for this block with transactionIndex and logIndex
          for (const address of changedAddresses) {
            const row = getBalance.get(address) as { balance: string } | undefined
            const txIndex = addressTransactionIndex.get(address) ?? 0
            const logIdx = addressLogIndex.get(address) ?? 0
            if (row) {
              balanceRows.push({
                address,
                block: blockNum,
                transactionIndex: txIndex,
                logIndex: logIdx,
                newBalance: row.balance
              })
            }
          }
        }
        
        // Mark the last processed block from the batch
        // This ensures we track the actual last block processed, not just blocks that had transfers
        if (lastProcessedBlock !== null) {
          insertProcessedBlock.run(lastProcessedBlock)
        }
      })
      
      // Execute the transaction atomically
      transaction()
      
      return balanceRows
    },
    
    fork: async (cursor: BlockCursor, { logger }) => {
      if (!db) {
        throw new Error('Database not initialized')
      }
      
      // On fork, rollback balances to the fork point
      logger.info({ forkBlock: cursor.number }, 'Handling fork, rolling back balances')
      
      // Get all addresses that had changes after the fork block
      const addressesToRollback = db.prepare(`
        SELECT DISTINCT address FROM balance_deltas WHERE block_number > ?
      `).all(cursor.number) as Array<{ address: string }>
      
      // For each address, rollback by subtracting deltas from blocks after the fork
      const getCurrentBalance = db.prepare('SELECT balance FROM balances WHERE address = ?')
      const getDeltasAfterFork = db.prepare(`
        SELECT delta FROM balance_deltas
        WHERE address = ? AND block_number > ?
      `)
      const updateBalance = db.prepare('INSERT INTO balances (address, balance) VALUES (?, ?) ON CONFLICT(address) DO UPDATE SET balance = ?')
      
      for (const { address } of addressesToRollback) {
        const currentRow = getCurrentBalance.get(address) as { balance: string } | undefined
        const currentBalance = BigInt(currentRow?.balance || '0')
        
        // Sum all deltas after fork (using BigInt to handle large numbers)
        const deltaRows = getDeltasAfterFork.all(address, cursor.number) as Array<{ delta: string }>
        let deltaToSubtract = 0n
        for (const row of deltaRows) {
          deltaToSubtract += BigInt(row.delta)
        }
        
        const balanceAtFork = currentBalance - deltaToSubtract
        updateBalance.run(address, balanceAtFork.toString(), balanceAtFork.toString())
      }
      
      // Delete deltas and processed blocks after the fork point
      db.prepare('DELETE FROM balance_deltas WHERE block_number > ?').run(cursor.number)
      db.prepare('DELETE FROM processed_blocks WHERE block_number > ?').run(cursor.number)
      
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

  // Create ClickHouse table with CollapsingMergeTree
  await client.command({ query: `
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
  `})

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

