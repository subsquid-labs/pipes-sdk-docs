import { BlockCursor, createTarget } from '@sqd-pipes/pipes'
import { createEvmPortalSource, createEvmDecoder} from '@sqd-pipes/pipes/evm'

import { commonAbis } from '@sqd-pipes/pipes/evm'

async function main() {
  const source = createEvmPortalSource({
    portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet'
  })

  const transformer = createEvmDecoder({
    contracts: ['0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'], // USDC
    events: {
      transfer: commonAbis.erc20.events.Transfer
    },
    range: { from: 'latest' }
  })

  // To handle forks we'll need to keep track of recently
  // processed unfinalized blocks. Here we'll use an in-memory queue.
  let recentUnfinalizedBlocks: BlockCursor[] = []

  await source
    .pipe(transformer)
    .pipeTo(createTarget({
      // When the source detects a fork it throws a
      // ForkException out of the read() function.
      // As a result write() is restarted.
      // For that reason it's critical that the we resume
      // from the last known block:
      //   ...
      //   for await (const {data, ctx} of read(recentUnfinalizedBlocks[recentUnfinalizedBlocks.length-1])) {
      //   ...
      write: async ({ctx: {logger, profiler}, read}) => {
        for await (const {data, ctx} of read(recentUnfinalizedBlocks[recentUnfinalizedBlocks.length-1])) {
          console.log(`Got ${data.transfer.length} transfers`)
          // Not all data streams contain information on recent blocks.
          // So instead of looking at the data we're using
          // ctx.state.rollbackChain: it contains cursor values for
          // all unfinalized blocks of the batch.
          ctx.state.rollbackChain.forEach((bc) => {
            recentUnfinalizedBlocks.push(bc)
          })
          // If the source has supplied a cursor of the last known final block
          // we can use it to prune the queue. Also, capping the queue length at 1000
          // (sufficient for all networks we know of).
          if (ctx.head.finalized) {
            recentUnfinalizedBlocks = recentUnfinalizedBlocks.filter(b => b.number >= ctx.head.finalized!.number)
          }
          recentUnfinalizedBlocks = recentUnfinalizedBlocks.slice(recentUnfinalizedBlocks.length - 1000, recentUnfinalizedBlocks.length)

          console.log(`Recent blocks list length is ${recentUnfinalizedBlocks.length} after processing the batch`)
        }
      },
      // When the source detects a fork it'll throw a ForkException
      // from the read() function. The target will catch it and run
      // the fork() function with the blocks sampled from
      // the new consensus (passed with the exception), then
      // run the write() function again.
      //
      // This might happen several times: we won't always find
      // the common ancestor among the new known consensus blocks
      // immediately.
      fork: async (newConsensusBlocks) => {
        console.log(`Got a fork!`)
        console.log(`Here are the saved recent blocks:\n`, printBlockCursorArray(recentUnfinalizedBlocks))
        console.log(`Here are the updated consensus blocks sent by the portal:\n`, printBlockCursorArray(newConsensusBlocks))
        const rollbackIndex = findRollbackIndex(recentUnfinalizedBlocks, newConsensusBlocks)
        if (rollbackIndex >= 0) {
          console.log(`Rolling back: removing blocks after ${printBlockCursor(recentUnfinalizedBlocks[rollbackIndex])}`)
          recentUnfinalizedBlocks.length = rollbackIndex + 1
          console.log(`Updated recent blocks:\n`, printBlockCursorArray(recentUnfinalizedBlocks))
          return recentUnfinalizedBlocks[rollbackIndex]
        }
        else {
          // We can't recover if the fork is deeper than
          // our log of recently processed blocks.
          console.log(`Failed to process the fork - no common ancestor found in recent blocks`)
          recentUnfinalizedBlocks.length = 0
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

function printBlockCursor(b: BlockCursor): string {
  return `{number: ${b.number}, hash: ${b.hash}`
}

function printBlockCursorArray(bcs: BlockCursor[]): string {
  return '[' + bcs.map(bc => printBlockCursor(bc)).join(',\n ') + ']'
}
