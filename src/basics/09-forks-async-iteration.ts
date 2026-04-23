import { BlockCursor, createTarget } from '@subsquid/pipes'
import { evmPortalStream, evmDecoder, commonAbis } from '@subsquid/pipes/evm'

// ── WORKAROUND: pipeToIterator ────────────────────────────────────────────────
//
// The native [Symbol.asyncIterator]() on a PortalSource cannot handle forks
// that require multiple 409 rounds. When the stream is re-created after a fork
// the first request lacks parentBlockHash, so the portal cannot detect whether
// we are still on the wrong chain and will never send the second 409.
//
// pipeTo's internal read() generator maintains a cursor variable across fork
// rounds: after target.fork() returns a rollback cursor, it sets
// cursor = forkedCursor before re-entering self.read(cursor), so
// parentBlockHash is always present and multi-round fork detection works.
//
// pipeToIterator wraps pipeTo to expose that behaviour as a for-await-of
// iterator. The bridge between pipeTo's push-based write() callback and the
// pull-based iterator is a single-item queue with producer acknowledgement:
//   write() pushes one batch, then blocks until the consumer acks it.
//   next() shifts the batch, acks the producer, and yields it to the caller.
// This preserves pull semantics: the portal is never asked for the next batch
// until the consumer has processed the current one.
//
// Limitation: if the caller's loop body contains await points, onFork may
// interleave with batch processing. For the synchronous example below this
// does not arise; in production code guard shared state if needed.
// ─────────────────────────────────────────────────────────────────────────────
function pipeToIterator<T>(
  source: { pipeTo(t: ReturnType<typeof createTarget<T>>): Promise<void> },
  initialCursor: BlockCursor | undefined,
  onFork: (previousBlocks: BlockCursor[]) => Promise<BlockCursor | null>
): AsyncIterableIterator<{ data: T; ctx: any }> {

  type Slot =
    | { k: 'batch'; v: { data: T; ctx: any } }
    | { k: 'end' }
    | { k: 'error'; err: unknown }

  const queue: Slot[] = []
  let consumerWake: (() => void) | null = null
  let producerAck:  (() => void) | null = null

  const wake = () => { consumerWake?.(); consumerWake = null }

  // Drive the stream via pipeTo in the background.
  ;(source.pipeTo as any)(createTarget({
    write: async ({ read }: any) => {
      for await (const batch of read(initialCursor)) {
        queue.push({ k: 'batch', v: batch })
        wake()
        await new Promise<void>(r => { producerAck = r })
      }
      queue.push({ k: 'end' })
      wake()
    },
    fork: onFork,
  })).catch((err: unknown) => { queue.push({ k: 'error', err }); wake() })

  return {
    async next(): Promise<IteratorResult<{ data: T; ctx: any }>> {
      if (!queue.length) await new Promise<void>(r => { consumerWake = r })
      const slot = queue.shift()!
      if (slot.k === 'end')   return { done: true,  value: undefined as any }
      if (slot.k === 'error') throw slot.err
      producerAck?.(); producerAck = null
      return { done: false, value: slot.v }
    },
    [Symbol.asyncIterator]() { return this },
  }
}

async function main() {
  let recentUnfinalizedBlocks: BlockCursor[] = []
  let finalizedHighWatermark: BlockCursor | undefined

  const stream = pipeToIterator(
    evmPortalStream({
      id: 'forks-async',
      portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
      outputs: evmDecoder({
        contracts: ['0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'], // USDC
        events: {
          transfer: commonAbis.erc20.events.Transfer
        },
        range: { from: 'latest' }
      }),
    }),
    recentUnfinalizedBlocks.at(-1),
    // Fork callback — identical logic to fork() in 07-forks.ts.
    // Runs synchronously relative to the stream (the SDK awaits it before
    // resuming), so recentUnfinalizedBlocks is safe to mutate here.
    async (newConsensusBlocks) => {
      console.log(`Got a fork!`)
      console.log(`Here are the saved recent blocks:\n`, printBlockCursorArray(recentUnfinalizedBlocks))
      console.log(`Here are the updated consensus blocks sent by the portal:\n`, printBlockCursorArray(newConsensusBlocks))
      const rollbackIndex = findRollbackIndex(recentUnfinalizedBlocks, newConsensusBlocks)
      if (rollbackIndex >= 0) {
        console.log(`Rolling back: removing blocks after ${printBlockCursor(recentUnfinalizedBlocks[rollbackIndex])}`)
        recentUnfinalizedBlocks.length = rollbackIndex + 1
        return recentUnfinalizedBlocks[rollbackIndex]
      }
      else if (
        finalizedHighWatermark &&
        newConsensusBlocks.every(b => b.number < finalizedHighWatermark!.number)
      ) {
        console.log(`All previousBlocks below high-water mark; rolling back to ${printBlockCursor(finalizedHighWatermark)}`)
        recentUnfinalizedBlocks = recentUnfinalizedBlocks.filter(b => b.number <= finalizedHighWatermark!.number)
        return finalizedHighWatermark
      }
      else {
        console.log(`Failed to process the fork - no common ancestor found in recent blocks`)
        recentUnfinalizedBlocks.length = 0
        return null
      }
    }
  )

  for await (const {data, ctx} of stream) {
    console.log(`Got ${data.transfer.length} transfers`)
    ctx.stream.state.rollbackChain.forEach((bc: BlockCursor) => {
      recentUnfinalizedBlocks.push(bc)
    })
    if (ctx.stream.head.finalized) {
      if (!finalizedHighWatermark || ctx.stream.head.finalized.number > finalizedHighWatermark.number) {
        finalizedHighWatermark = ctx.stream.head.finalized
      }
      recentUnfinalizedBlocks = recentUnfinalizedBlocks.filter(b => b.number >= finalizedHighWatermark!.number)
    }
    recentUnfinalizedBlocks = recentUnfinalizedBlocks.slice(recentUnfinalizedBlocks.length - 1000)

    console.log(`Recent blocks list length is ${recentUnfinalizedBlocks.length} after processing the batch`)
  }
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
