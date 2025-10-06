import { BlockCursor, createTarget } from '@sqd-pipes/pipes'
import { createEvmPortalSource, createEvmDecoder} from '@sqd-pipes/pipes/evm'

import { commonAbis } from '@sqd-pipes/pipes/evm'

function printBlockCursor(b: BlockCursor): string {
  return `{number: ${b.number}, hash: ${b.hash}`
}

function printBlockCursorArray(bcs: BlockCursor[]): string {
  return '[' + bcs.map(bc => printBlockCursor(bc)).join(',\n ') + ']'
}

async function main() {
  const source = createEvmPortalSource({
    portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet'
  })

  const transformer = createEvmDecoder({
    contracts: ['0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'], // USDC
    events: {
      transfer: commonAbis.erc20.events.Transfer
    },
    range: { from: 23515071 }
  })

  const recentBlocks: BlockCursor[] = []

  const pushRecentBlock = (b: BlockCursor) => {
    recentBlocks.push(b)
    if (recentBlocks.length > 64) {
      recentBlocks.shift()
    }
  }

  await source
    .pipe(transformer)
    .pipeTo(createTarget({
      write: async ({ctx: {logger, profiler}, read}) => {
        for await (const {data, ctx} of read(recentBlocks[recentBlocks.length-1])) {
          logger.info(`Got ${data.transfer.length} transfers`)
          for (const bc of ctx.state.rollbackChain) {
            pushRecentBlock(bc)
          }
          logger.info(`Recent blocks list length is ${recentBlocks.length} after processing the batch`)
          console.log(printBlockCursorArray(recentBlocks))
        }
      },
      fork: async (previousBlocks) => {
        console.log(`Got a fork with ${previousBlocks.length} previous blocks`)
        console.log(`Here are the saved recent blocks:\n`, printBlockCursorArray(recentBlocks))
        console.log(`Here are the previous blocks sent by the portal:\n`, printBlockCursorArray(previousBlocks))
        const lastCommonIndex = findRollbackIndex(recentBlocks, previousBlocks)
        if (lastCommonIndex >= 0) {
          console.log(`Rolling back / removing blocks after ${printBlockCursor(recentBlocks[lastCommonIndex])}`)
          recentBlocks.length = lastCommonIndex + 1
          console.log(`Updated recent blocks:\n`, printBlockCursorArray(recentBlocks))
          return recentBlocks[lastCommonIndex]
        }
        else {
          console.log(`Failed to process the fork - no common ancestor block found`)
          recentBlocks.length = 0
          return null
        }
      }
    }))
}

main().then(() => { console.log('\n\ndone') })

function findRollbackIndex(chainA: BlockCursor[], chainB: BlockCursor[]): number {
    let aIndex = 0
    let bIndex = 0
    let lastCommonIndex = -1

    while (aIndex < chainA.length && bIndex < chainB.length) {
        const blockA = chainA[aIndex]
        const blockB = chainB[bIndex]

        if (blockA.number < blockB.number) {
            aIndex++
            continue
        }

        if (blockA.number > blockB.number) {
            bIndex++
            continue
        }

        if (blockA.number === blockB.number && blockA.hash !== blockB.hash) {
            return lastCommonIndex
        }

        lastCommonIndex = aIndex
        aIndex++
        bIndex++
    }

    return lastCommonIndex
}
