# crypto-zilla

Streaming lakehouse on live market data. Trades from public exchange feeds go
through Kafka into Flink and Spark, and land in Iceberg tables. Runs locally on
Docker and is laid out so the same images and code can be deployed to GCP.

## Status

- [x] Part 1: Binance trades into Kafka
- [x] Part 2: object storage, Iceberg catalog, Spark streaming to bronze
- [ ] Part 3: Coinbase source
- [ ] Part 4+: Flink, batch, maintenance, cloud

## Architecture (so far)

```
Binance WS -> ingest-binance -> Kafka trades.raw -> spark-bronze (Structured Streaming)
                                                        |
                                    Lakekeeper (Iceberg REST catalog, Postgres)
                                                        |
                                    MinIO s3://warehouse  (Parquet + Iceberg metadata)
```

The Spark job runs on a standalone cluster: `spark-master` hands out executors on
`spark-worker`, and `spark-bronze` is the driver (client mode). Spark gets storage
access from the catalog as short-lived vended credentials; no storage keys are
configured in Spark.

## Layout

```
docker-compose.yml        local stack
docker/                   image definitions (ingest, spark)
infra/
  kafka/                  topic definitions and bootstrap
  minio/                  bucket, catalog user and its policy
  lakekeeper/             catalog bootstrap and warehouse
  spark/                  spark-defaults, extra jars, submit wrapper
src/cryptozilla/
  config.py               settings from env (one class per concern)
  log.py                  text or json logs
  models/                 shared data contracts (Trade, Instrument)
  schemas/                Spark schemas for the same contracts
  messaging/              sink interface + Kafka implementation
  ingest/                 exchange adapters, runner, CLI
  spark/                  session factory (catalog config lives only here)
  lake/                   table definitions
  jobs/                   Spark jobs, run with cz-submit <job>
  tools/                  sql runner
tests/
```

## Running locally

Requires Docker and [uv](https://docs.astral.sh/uv/). Give Docker at least 6 GB of RAM.

```
make up          # whole stack; the first build of the Spark image takes a few minutes
make ps          # *-init and lakekeeper-migrate should show Exited (0)
make spark-logs
```

| UI | URL |
|---|---|
| Kafka UI | http://localhost:8085 |
| MinIO console | http://localhost:9101 (user/password from `.env`) |
| Lakekeeper | http://localhost:8181/ui |
| Spark master | http://localhost:8080 |
| Spark worker | http://localhost:8082 |
| Bronze job | http://localhost:4040 |

Queries (each starts a small local Spark session inside the driver container):

```
make sql q="SELECT count(*) FROM lake.bronze.trades"
make sql q="SELECT instrument, count(*), max(event_time) FROM lake.bronze.trades GROUP BY 1"
make sql q="SELECT committed_at, operation, summary['added-records'] FROM lake.bronze.trades.snapshots ORDER BY 1 DESC"
make sql q="SELECT reason, count(*) FROM lake.bronze.trades_rejected GROUP BY 1"
make shell       # interactive pyspark
```

Development on the host:

```
make install
make test        # Spark tests need Java 17+ on the host, otherwise they are skipped
make lint
```

## Checks

1. `count(*)` on `lake.bronze.trades` grows about once a minute.
2. `lake.bronze.trades.snapshots` shows one `append` per trigger.
3. No duplicates after a restart:

   ```
   docker compose restart spark-bronze
   make sql q="SELECT count(*) AS total, count(DISTINCT kafka_partition, kafka_offset) AS distinct_offsets FROM lake.bronze.trades"
   ```

4. The Spark master UI shows `cz-bronze_trades` running with executors on the worker.

## Data contracts

Topic `trades.raw`, key `<source>:<instrument>`, JSON value:

```json
{
  "schema_version": 1,
  "source": "binance",
  "instrument": "BTC-USDT",
  "symbol": "BTCUSDT",
  "base": "BTC",
  "quote": "USDT",
  "trade_id": "123456789",
  "price": "64250.12",
  "quantity": "0.0015",
  "side": "sell",
  "event_ts": 1700000000050,
  "ingest_ts": 1700000000999
}
```

`side` is the aggressor side. Prices and quantities are strings to keep full
decimal precision. Timestamps are epoch milliseconds UTC.

`lake.bronze.trades` holds the same fields typed (`decimal(38,18)`, UTC timestamps),
plus Kafka lineage (`kafka_partition`, `kafka_offset`, `kafka_time`) and the original
message in `raw`. Partitioned by `days(event_time), source`. Messages that fail
validation go to `lake.bronze.trades_rejected` with a `reason`.

## Adding a source

1. Create `src/cryptozilla/ingest/sources/<name>.py` with a settings class and a
   `Source` subclass (usually `WebSocketSource`) that maps raw messages to `Trade`.
2. Register it in `sources/__init__.py`.
3. Add a service to `docker-compose.yml` with `command: ["<name>"]`.

## Adding a Spark job

1. Create `src/cryptozilla/jobs/<name>.py` with a `main(args)` function.
2. Register it in `jobs/__main__.py`.
3. Add a service using the `x-spark` block with `command: ["cz-submit", "<name>"]`.