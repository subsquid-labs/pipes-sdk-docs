import { createTarget } from '@subsquid/pipes'
import { evmPortalSource, evmDecoder } from '@subsquid/pipes/evm'

import { commonAbis } from '@subsquid/pipes/evm'

async function main() {
  const source = evmPortalSource({
    portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet'
    // we can omit the query builder, the source with add a blank one
  })

  const transformer = evmDecoder({
    contracts: ['0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'], // USDC
    events: {
      transfer: commonAbis.erc20.events.Transfer
    },
    range: { from: 20_000_000, to: 20_000_000 }
  })

  const target = createTarget({
    write: async ({logger, read}) => {
      for await (const {data} of read()) {
        logger.info({data}, 'data')
      }
    },
  })

  await source.pipe(transformer).pipeTo(target)
}

void main()
