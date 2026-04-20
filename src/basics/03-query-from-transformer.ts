import { createTarget } from '@subsquid/pipes'
import { evmPortalStream, evmQuery } from '@subsquid/pipes/evm'

async function main() {
  // In the new API, query requirements are specified directly in the query builder
  // passed as `outputs`. Transforms are then chained with .build().pipe()
  const source = evmPortalStream({
    id: 'query-from-transformer',
    portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
    outputs: evmQuery()
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
      .build()
      .pipe({
        transform: (data) => data.map(b => b.logs.map(l => l.transactionHash))
      }),
  })

  const target = createTarget({
    write: async ({logger, read}) => {
      for await (const {data} of read()) {
        logger.info({data}, 'data')
      }
    },
  })

  await source.pipeTo(target)
}

void main()
