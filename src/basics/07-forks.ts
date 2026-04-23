import { BlockCursor, createTarget } from '@subsquid/pipes'
import { evmPortalStream, evmDecoder, commonAbis } from '@subsquid/pipes/evm'

async function main() {
  // To handle forks we'll need to keep track of recently
  // processed unfinalized blocks. Here we'll use an in-memory queue.
  let recentUnfinalizedBlocks: BlockCursor[] = []
  // Portal instances can be load-balanced: a reconnected stream may land on a
  // lagging instance whose X-Sqd-Finalized-Head-Number is lower than what
  // we previously saw. Treat the finalized head as a monotonically increasing
  // high-water mark so the pruning threshold never moves backwards. The full
  // cursor (number + hash) is kept so it can serve as a fallback rollback
  // point when a lagging instance's 409 sample doesn't overlap our history.
  let finalizedHighWatermark: BlockCursor | undefined

  await evmPortalStream({
    id: 'forks',
    portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
    outputs: evmDecoder({
      contracts: ['0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'], // USDC
      events: {
        transfer: commonAbis.erc20.events.Transfer
      },
      range: { from: 'latest' }
    }),
  })
    .pipeTo(createTarget({
      // The cursor passed to read() is the startup cursor. For this in-memory
      // example recentUnfinalizedBlocks is always empty at process start, so
      // the stream begins from range.from. If the state were persisted across
      // restarts (e.g. in a database), restoring recentUnfinalizedBlocks and
      // passing its last entry here would resume from the correct position.
      //
      // Fork handling is transparent to write(): when the portal returns a 409
      // the SDK catches the ForkException inside the read() iterator, calls
      // fork() to determine the rollback cursor, then resumes the stream from
      // that cursor — write() keeps iterating batches without interruption.
      write: async ({logger, read}) => {
        for await (const {data, ctx} of read(recentUnfinalizedBlocks[recentUnfinalizedBlocks.length-1])) {
          console.log(`Got ${data.transfer.length} transfers`)
          // Not all data streams contain information on recent blocks.
          // So instead of looking at the data we're using
          // ctx.state.rollbackChain: it contains cursor values for
          // all unfinalized blocks of the batch.
          ctx.stream.state.rollbackChain.forEach((bc) => {
            recentUnfinalizedBlocks.push(bc)
          })
          // If the source has supplied a cursor of the last known final block
          // we can use it to prune the queue. Also, capping the queue length at 1000
          // (sufficient for all networks we know of).
          if (ctx.stream.head.finalized) {
            if (!finalizedHighWatermark || ctx.stream.head.finalized.number > finalizedHighWatermark.number) {
              finalizedHighWatermark = ctx.stream.head.finalized
            }
            recentUnfinalizedBlocks = recentUnfinalizedBlocks.filter(b => b.number >= finalizedHighWatermark!.number)
          }
          recentUnfinalizedBlocks = recentUnfinalizedBlocks.slice(recentUnfinalizedBlocks.length - 1000, recentUnfinalizedBlocks.length)

          console.log(`Recent blocks list length is ${recentUnfinalizedBlocks.length} after processing the batch`)
        }
      },
      // When the portal detects a fork it returns HTTP 409. The SDK translates
      // this into a ForkException, catches it inside the read() iterator, and
      // calls fork() with the portal's current-chain block sample. fork() must
      // find the latest block that both our local history and that sample agree
      // on (by hash and number) and return it as the rollback cursor. The
      // iterator then resumes the stream from cursor.number + 1.
      //
      // This may happen more than once if the portal's sample doesn't reach
      // the common ancestor on the first attempt.
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
        else if (
          finalizedHighWatermark &&
          newConsensusBlocks.every(b => b.number < finalizedHighWatermark!.number)
        ) {
          // All of previousBlocks are below the finalized high-water mark.
          // This happens when the load balancer switches to a lagging portal
          // instance whose fork sample doesn't reach up to our history. Since
          // the high-water mark is truly final, all correct instances agree on
          // it: the fork must be somewhere above it. Roll back to the high-water
          // mark so the stream can recover from there.
          console.log(`All previousBlocks below high-water mark; rolling back to ${printBlockCursor(finalizedHighWatermark)}`)
          // Discard any blocks above the high-water mark — they may be on
          // the old fork chain and must be re-fetched from the new chain.
          recentUnfinalizedBlocks = recentUnfinalizedBlocks.filter(b => b.number <= finalizedHighWatermark!.number)
          return finalizedHighWatermark
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
