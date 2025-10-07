import { createTarget } from '@sqd-pipes/pipes'
import { createEvmPortalSource, createEvmDecoder } from '@sqd-pipes/pipes/evm'
import { sqliteCacheAdapter } from '@sqd-pipes/pipes/portal-cache'

import { commonAbis } from '@sqd-pipes/pipes/evm'

/**
 * In our measurements this took about 55s for the first run and
 * 35s once the cache was populated.
 */

async function main() {
  await createEvmPortalSource({
    portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
    cache: {
      adapter: await sqliteCacheAdapter({
        path: './evm-source.cache.sqlite'
      })
    }
  })
  .pipe(createEvmDecoder({
    profiler: {id: 'Decoding'},
    contracts: ['0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'], // USDC
    events: {
      transfer: commonAbis.erc20.events.Transfer
    },
    range: { from: 20_000_000, to: 20_100_000 }
  }))
  .pipeTo(createTarget({
    write: async ({ctx: {logger, profiler}, read}) => {
      for await (const {data} of read()) {
        logger.info(`Got ${data.transfer.length} transfers`)
      }
    },
  }))
}

void main()