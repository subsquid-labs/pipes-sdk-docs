# Pipes SDK examples

Pipes SDK is a highly customizable blockchain indexing library made by [Subsquid](https://www.sqd.ai).

## Quickstart

```bash
git clone https://github.com/subsquid-labs/pipes-sdk-docs
cd pipes-sdk-docs
npm i
```
The examples are at
 * `src/basics/*.ts`
 * `src/advanced/evm/*.ts`
 * `src/advanced/solana/*.ts`
You can run any of them with `ts-node`:
```bash
npx ts-node <path_to_example>
```
Clickhouse-related examples require that a local Clickhouse container is running. Start it with
```bash
docker compose up -d
```

## Basic examples

1. [trivial-pipe](src/basics/01-trivial-pipe.ts): an elementary Portal data pipeline with a source that fetches USDC transfers on a single block (20M) + a target that just prints the incoming data.

2. [transformer](src/basics/02-transformer.ts): same pipeline, but with a transformer added in the middle. The transformer takes a `transactionHash` for every Transfer.

3. [query-from-transformer](src/basics/03-query-from-transformer.ts): transformers can send queries to the source! This example's source starts with a blank query. A transformer then adds the USDC Transfers query to the query builder using the `query` callback.

   This enables transformers to combine data selection and processing, creating self-contained modules.

4. [evm-events-decoder-transformer](src/basics/04-evm-events-decoder-transformer.ts): `createEvmDecoder` makes self-contained transforms that request and decode event logs of EVM smart contracts. Again I'm using USDC Transfers at block 20M, but now logs come out decoded:
   ```
   data: {
     "transfer": [
       {
         ...
         "event": {
           "from": "0xcb83ca9633ad057bd88a48a5b6e8108d97ad4472",
           "to": "0xa1db2fc9b2ceaf3cdf41fd11ffcb38404eae3906",
           "value": 615568393
         },
         ...
         "rawEvent": {
            ...
   ```

   Event selection and decoding is done using event objects. For common contract protocols these can be retrieved from the `commonAbis` object exported from `@sqd-pipes/pipes/evm`. Equivalently, you can generate them from JSON ABIs with the `squid-evm-typegen` utility:
   ```bash
   npx squid-evm-typegen src/abi abi/contract0abi.json ...
   ```

5. [composite-transformer](src/basics/05-composite-transformer.ts): now there are two transformers simultaneously adding data to the source output. A call
   ```ts
   source.extend({
     field0: transformer0,
     field1: transformer1
   })
   ```
   makes data in the shape of
   ```ts
   {
     ...data_as_it_arrived_from_the_source,
     field0: output_of_transformer0,
     field1: output_of_transformer1
   }
   ```
   I'm using two transformers made using `createEvmDecoder`: one for USDC data and another for data on Swap events emitted by the [Uniswap V3 WETH-USDC pool](https://etherscan.io/address/0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640). Each transformer only request its own relevant events from the source; the queries are merged.

6. [cursor-from-target](src/basics/06-cursor-from-target.ts): aside from queries, it's also possible to pass *cursors* to the source. A cursor specifies a position within the original query, such as a block number. When a cursor is passed to the `read()` function by either a target or a transformer, the source will only fetch the data starting from the block that follows the cursor position.

   The example fetches Transfer events of the Aleph token in the range [20_000_000, 20_000_500]. The pipeline runs twice: first with a target that doesn't supply a cursor to `read()`, then with a target that supplies `{number: 20_000_300}`. In the first run the pipeline outputs two events at 20_000_267 and 20_000_459. The second run outputs only the latter event and informs the user:
   ```
   INFO: Resuming indexing from 20,000,300 block
   ```

   This function allows the pipelines to be restarted from a previously saved position.

7. [forks](src/basics/07-forks.ts): data source can also raise a `ForkException` that indicates that the data at the source has changed (e.g. due to a [blockchain reorg](https://cointelegraph.com/explained/what-is-chain-reorganization-in-blockchain-technology)) and all downstream components must adjust to the new state. Targets made with a call like `createTarget({ write, fork })` will run the `fork` function whenever this happens.

8. [clickhouse](src/basics/08-clickhouse-target.ts): `createClickhouseTarget` creates targets that write their incoming data to Clickhouse, automatically keep track of the progress, resume from the last processed block on restarts and process forks.

   Start a local Clickhouse container before running this example:
   ```bash
   docker compose up -d
   ```

## Advanced examples

Advanced examples at [src/advanced](src/advanced) are sorted by virtual machine from which the data is sourced ([EVM](src/advanced/evm), [Solana](src/advanced/solana)) and cover a variety of topics:
 * profiling
 * indexing factory contracts
 * customizing the metrics
 * the basics covered in previous section

<!-- user-side caching -->
