import { createTarget, createTransformer } from '@sqd-pipes/pipes'
import {
  createEvmPortalSource,
  createEvmDecoder,
  commonAbis
} from '@sqd-pipes/pipes/evm'
import * as uniswapV3Pool from '../abi/uniswapV3Pool'

const atBlock = 20000099
const oneBlockRange = { from: atBlock, to: atBlock }

async function main() {
  const source = createEvmPortalSource({
    portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
  })

  const target = createTarget({
    write: async ({ctx: {logger, profiler}, read}) => {
      for await (const {data} of read()) {
        logger.info({data}, 'data')
      }
    },
  })

  await source
    .pipeComposite({
      usdcTransfers: createEvmDecoder({
        contracts: ['0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'], // USDC
        events: {
          transfer: commonAbis.erc20.events.Transfer
        },
        range: oneBlockRange
      }),
      wethUsdcSwaps: createEvmDecoder({
        contracts: ['0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'], // Uniswap v3 WETH-USDC pool
        events: {
          swap: uniswapV3Pool.events.Swap,
        },
        range: oneBlockRange
      })
    })
    .pipeTo(target)
}

void main()
