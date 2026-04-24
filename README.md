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
 * [stateful transforms writing to Postgres — stateless approach](src/advanced/evm/11.stateful-transforms-postgres-stateless.example.ts)
 * [stateful transforms writing to Postgres — in-memory approach](src/advanced/evm/12.stateful-transforms-postgres-in-memory.example.ts)

These should run in the same manner as basic examples do: start with [BASICS.md/Quickstart](https://github.com/subsquid-labs/pipes-sdk-docs/?tab=readme-ov-file#quickstart) then run the example files with `bun` or `ts-node`.

### Postgres examples (11 and 12)

Examples 11 and 12 demonstrate two approaches to storing both transformer state and final data in the same PostgreSQL database using `drizzleTarget`.

**Start Postgres:**

```bash
docker compose up -d test_postgres
```

This starts a Postgres 17 instance on `localhost:5432` (user: `postgres`, password: `postgres`, database: `postgres`). Wait for the healthcheck to pass before running the examples.

**Schema / migrations:**

Both examples call `onStart` to run `CREATE TABLE IF NOT EXISTS` statements when the indexer first connects. This is sufficient for running the examples as-is.

For production use, replace the `onStart` DDL with [drizzle-kit](https://orm.drizzle.team/docs/migrations) migrations:

```bash
npx drizzle-kit generate   # generate SQL migration files from your schema
npx drizzle-kit migrate    # apply pending migrations
```

**Run example 11** (stateless — reads state from Postgres each batch):

```bash
DB_URL=postgresql://postgres:postgres@localhost:5432/postgres \
npx ts-node src/advanced/evm/11.stateful-transforms-postgres-stateless.example.ts
```

**Run example 12** (in-memory — loads state once at startup, updates in RAM):

```bash
DB_URL=postgresql://postgres:postgres@localhost:5432/postgres \
npx ts-node src/advanced/evm/12.stateful-transforms-postgres-in-memory.example.ts
```

Both examples index the SQD token on Arbitrum One and maintain a `token_balances` table (running ERC-20 balance per address) and a `transfer_counts` table (number of outbound transfers per address). They are interchangeable — the on-disk schema is identical; only the in-process state management differs.

## Templates

A collection of Pipes SDK project intended as starting points:
 * [LayerZero](https://github.com/subsquid-labs/eth-global-layerzero-pipe-template)
 * [ENS](https://github.com/subsquid-labs/eth-global-ens-pipe-template)
 * [UniswapV4](https://github.com/subsquid-labs/eth-global-uniswapV4-pipe-template)

## Miscellaneous

These are related to the indexers that SQD uses internally:
 * [The stablecoins indexer](https://github.com/iankressin/stablecoin-dashboard/blob/main/src/lib/indexer/stables.indexer.ts) that sends the data via Server Side Events for further processing
 * [Simple indexer of ERC20 transfers](https://github.com/iankressin/erc20-transfer-pipe).