import { createTarget } from '@subsquid/pipes'
import { evmPortalSource, evmDecoder } from '@subsquid/pipes/evm'
import { portalSqliteCache } from '@subsquid/pipes/portal-cache/node'

import { commonAbis } from '@subsquid/pipes/evm'

/**
 * In our measurements this took about 55s for the first run and
 * 35s once the cache was populated.
 */

async function main() {
  await evmPortalSource({
    portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
    cache: portalSqliteCache({
      path: './evm-source.cache.sqlite'
    })
  })
  .pipe(evmDecoder({
    profiler: {id: 'Decoding'},
    contracts: ['0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'], // USDC
    events: {
      transfer: commonAbis.erc20.events.Transfer
    },
    range: { from: 20_000_000, to: 20_100_000 }
  }))
  .pipeTo(createTarget({
    write: async ({logger, read}) => {
      for await (const {data} of read()) {
        logger.info(`Got ${data.transfer.length} transfers`)
      }
    },
  }))
}

void main()