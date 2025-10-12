import { createClient } from '@clickhouse/client'
import { commonAbis, createEvmDecoder, createEvmPortalSource } from '@sqd-pipes/pipes/evm'
import { createClickhouseTarget } from '@sqd-pipes/pipes/targets/clickhouse'

/**
 * This example demonstrates how to use ClickHouse as a target for storing processed blockchain data.
 * It creates a connection to a local ClickHouse instance, sets up an EVM Portal Source to stream
 * ERC20 transfer events from Base Mainnet, and pipes the decoded data to ClickHouse while
 * measuring performance with a profiler.
 */
async function main() {
  const client = createClient({
    username: 'default',
    password: 'default',
    url: 'http://localhost:10123',
  })

  client.command({ query: `
    CREATE TABLE IF NOT EXISTS usdc_transfers (
      block_number          UInt32 CODEC (DoubleDelta, ZSTD),
      timestamp             DateTime CODEC (DoubleDelta, ZSTD),
      transaction_hash      String,
      log_index             UInt16,
      from                  LowCardinality(FixedString(42)), -- address
      to                    LowCardinality(FixedString(42)), -- address
      value                 UInt256,
      sign                  Int8 DEFAULT 1
    )
    ENGINE = CollapsingMergeTree(sign)
    ORDER BY (block_number, transaction_hash, log_index);
    `
  })

  await createEvmPortalSource({
    portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
  })
  .pipe(
    createEvmDecoder({
      range: { from: 'latest' },
      contracts: [ '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' ], // USDC
      events: {
        transfers: commonAbis.erc20.events.Transfer,
      },
    }),
  )
  .pipeTo(
    createClickhouseTarget({
      client,
      onRollback: async ({type, store, cursor}) => {
        try {
          await store.removeAllRows({
            tables: ['usdc_transfers'],
            where: `block_number > ${cursor.number}`,
          })
        }
        catch (err) {
          console.error('onRollback err:', err)
          throw err
        }
      },
      onData: async ({ store, data, ctx }) => {
        console.log(`inserting ${data.transfers.length} transfers`)
        store.insert({
          table: 'usdc_transfers',
          values: data.transfers.map(t => ({
            block_number: t.blockNumber,
            timestamp: t.timestamp.valueOf() / 1000,
            transaction_hash: t.rawEvent.transactionHash,
            log_index: t.rawEvent.logIndex,
            from: t.event.from,
            to: t.event.to,
            value: t.event.value.toString()
          })),
          format: 'JSONEachRow'
        })
      },
    }),
  )
}

void main()