import { createTarget } from '@sqd-pipes/pipes'
import { createEvmPortalSource, createEvmDecoder } from '@sqd-pipes/pipes/evm'

import { commonAbis } from '@sqd-pipes/pipes/evm'

const source = createEvmPortalSource({
  portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet'
})

const transformer = createEvmDecoder({
  contracts: ['0x27702a26126e0b3702af63ee09ac4d1a084ef628'], // Aleph token
  events: {
    transfer: commonAbis.erc20.events.Transfer
  },
  range: { from: 20_000_000, to: 20_000_500 }
})

async function firstRun() {
  console.log(`\n\nStarting from the default block 20_000_000...`)
  await source
    .pipe(transformer)
    .pipeTo(createTarget({
      write: async ({ctx: {logger, profiler}, read}) => {
        for await (const {data} of read()) {
          console.log('data:', data)
        }
      },
    }))
}

async function secondRun() {
  console.log(`\n\nStarting from blocks following 20_000_300...`)
  await source
    .pipe(transformer)
    .pipeTo(createTarget({
      write: async ({ctx: {logger, profiler}, read}) => {
        for await (const {data} of read({ number: 20_000_300 })) {
          console.log('data:', data)
        }
      },
    }))
}

firstRun().then(() => { secondRun().then(() => { console.log('\n\ndone') }) })