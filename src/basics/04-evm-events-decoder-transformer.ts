import { createTarget } from '@sqd-pipes/pipes'
import { createEvmPortalSource, createEvmDecoder } from '@sqd-pipes/pipes/evm'

import { commonAbis } from '@sqd-pipes/pipes/evm'

async function main() {
  const source = createEvmPortalSource({
    portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet'
    // we can omit the query builder, the source with add a blank one
  })

  const transformer = createEvmDecoder({
    contracts: ['0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'], // USDC
    events: {
      transfer: commonAbis.erc20.events.Transfer
    },
    range: { from: 20_000_000, to: 20_000_000 }
  })

  const target = createTarget({
    write: async ({ctx: {logger, profiler}, read}) => {
      for await (const {data} of read()) {
        logger.info({data}, 'data')
      }
    },
  })

  await source.pipe(transformer).pipeTo(target)
}

void main()
