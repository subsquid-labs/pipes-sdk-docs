import { solanaInstructionDecoder, solanaPortalStream } from '@subsquid/pipes/solana'

import * as orcaWhirlpool from './abi/orca_whirlpool'

async function cli() {
  const stream = solanaPortalStream({
    id: 'solana-swaps',
    portal: 'https://portal.sqd.dev/datasets/solana-mainnet',
    outputs: {
      orcaWhirlpool: solanaInstructionDecoder({
        range: { from: 'latest' },
        programId: orcaWhirlpool.programId,
        instructions: {
          swaps: orcaWhirlpool.instructions.swap,
          swapsV2: orcaWhirlpool.instructions.swapV2,
        },
      }),
    },
  })

  for await (const { data } of stream) {
    console.log(`parsed orca ${data.orcaWhirlpool.swaps.length} swaps`)
  }
}

void cli()
