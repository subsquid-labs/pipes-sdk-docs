# Pipes SDK examples directory

Pipes SDK is a highly customizable blockchain indexing library made by [Subsquid](https://www.sqd.ai). This repo is a collection of various examples of its usage.

## Basics

At [src/basics](src/basics) folder of this repo you'll find a sequence of examples highlighting core concepts and basic functionality of the Pipes SDK. Follow [BASICS.md](BASICS.md) to learn more.

## Advanced

[src/advanced](src/advanced) contains examples showcasing the less common features. It significantly overlaps with the [examples folder of the Pipes SDK package repo](https://github.com/subsquid-labs/pipes-sdk/tree/main/docs/examples) which doubles as a collection of ad-hoc tests. Here are some topics covered:

 * [user side raw data caching](src/advanced/evm/08.portal-cache.example.ts)
 * profiling (throughout the examples)
 * [indexing factory contracts](src/advanced/evm/03.factory.example.ts)
 * [customizing the metrics](src/advanced/evm/06.custom-metrics.example.ts)

These should run in the same manner as basic examples do: start with [BASICS.md/Quickstart](https://github.com/subsquid-labs/pipes-sdk-docs/?tab=readme-ov-file#quickstart) then run the example files with `bun` or `ts-node`.

## Templates

A collection of Pipes SDK project intended as starting points:
 * [LayerZero](https://github.com/subsquid-labs/eth-global-layerzero-pipe-template)
 * [ENS](https://github.com/subsquid-labs/eth-global-ens-pipe-template)
 * [UniswapV4](https://github.com/subsquid-labs/eth-global-uniswapV4-pipe-template)

## Miscellaneous

These are related to the indexers that SQD uses internally:
 * [The stablecoins indexer](https://github.com/iankressin/stablecoin-dashboard/blob/main/src/lib/indexer/stables.indexer.ts) that sends the data via Server Side Events for further processing
 * [Simple indexer of ERC20 transfers](https://github.com/iankressin/erc20-transfer-pipe).