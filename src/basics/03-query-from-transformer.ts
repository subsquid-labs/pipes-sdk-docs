import { createTarget, createTransformer } from '@sqd-pipes/pipes'
import {
  createEvmPortalSource,
  type EvmPortalData,
  EvmQueryBuilder
} from '@sqd-pipes/pipes/evm'

async function main() {
  const blankQueryBuilder = new EvmQueryBuilder()

  const source = createEvmPortalSource({
    portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
    query: blankQueryBuilder,
  })

  const transformer = createTransformer({
    transform: async (data: EvmPortalData<any>) => {
      return data.blocks.map(b => b.logs.map(l => l.transactionHash))
    },
    query: ({queryBuilder, portal, logger}) => {
      queryBuilder.addFields({
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
    }
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
