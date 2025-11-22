import { commonAbis, evmDecoder, evmPortalSource } from '@subsquid/pipes/evm'

/**
 * Basic example demonstrating how to use pipes for processing EVM data.
 * This example shows how to:
 * - Create a data stream from Base Mainnet using Portal API
 * - Decode ERC20 transfer events
 * - Transform the decoded events by adding a custom type field
 * - Process the transformed events in a streaming fashion
 */

async function cli() {
  const stream = evmPortalSource({
    portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
  }).pipe(
    evmDecoder({
      profiler: { id: 'ERC20 transfers' },
      range: { from: 'latest' },
      events: {
        transfers: commonAbis.erc20.events.Transfer,
      },
    }).pipe({
      profiler: { id: 'add type field' },
      transform: ({ transfers }) => {
        return {
          transfers: transfers.map((e) => ({
            ...e,
            type: 'transfer',
          })),
        }
      },
    }),
  )

  for await (const { data } of stream) {
    // console.log(data.transfers.length)
  }
}

void cli()
