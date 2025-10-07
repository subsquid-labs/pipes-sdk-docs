import { createTarget } from '@sqd-pipes/pipes'
import { createEvmPortalSource, EvmQueryBuilder } from '@sqd-pipes/pipes/evm'

// The query builder shapes the query that gets sent to the Portal API.
// All methods only add to the data request, none shrink it.
// Here we'll use a query builder that requests Transfer event
// logs from the USDC token contract.
const queryBuilderWithUsdcTransfers = new EvmQueryBuilder()
  .addFields({
    block: {
      // These two fields are required.
      // For now please manually add them to the query builder.
      // Once is enough, anywhere is fine.
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
  const source = createEvmPortalSource({
    portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
    query: queryBuilderWithUsdcTransfers,
  })

  const target = createTarget({
    write: async ({ctx: {logger, profiler}, read}) => {
      for await (const {data} of read()) {
        logger.info(data, 'data')
      }
    },
  })

  await source.pipeTo(target)

  // You can also iterate over the source directly
  for await (let {data} of source) {
    console.log(data)
  }
}

void main()