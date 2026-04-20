import { createTarget } from '@subsquid/pipes'
import {
  evmPortalStream,
  EvmQueryBuilder
} from '@subsquid/pipes/evm'

// Exact same query as in 01-trivial-pipe.ts
const queryBuilderWithUsdcTransfers = new EvmQueryBuilder()
  .addFields({
    block: {
      number: true, hash: true,
    },
    log: {
      address: true,
      topics: true,
      data: true,
      transactionHash: true,
    },
  })
  .addLog({
    request: {
      address: ['0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'], // USDC
      topic0: ['0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'], // Transfer
    },
    range: {
      from: 20000000,
      to: 20000000,
    },
  })

async function main() {
  const source = evmPortalStream({
    id: 'transformer',
    portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
    outputs: queryBuilderWithUsdcTransfers,
  })

  const target = createTarget({
    write: async ({logger, read}) => {
      for await (const {data} of read()) {
        logger.info({data}, 'data')
      }
    },
  })

  // Custom transforms are chained with .pipe() on the stream
  await source.pipe({
    transform: (data) => data.map(b => b.logs.map(l => l.transactionHash))
  }).pipeTo(target)
}

void main()
