# crypto-zilla

Streaming lakehouse on live market data. Trades from public exchange feeds go
through Kafka into Flink and Spark, and land in Iceberg tables. Runs locally on
Docker and is laid out so the same images and code can be deployed to GCP.

## Status

- [x] Part 1: Binance trades into Kafka
- [ ] Part 2: object storage, Iceberg catalog, Spark streaming to bronze
- [ ] Part 3: Coinbase source
- [ ] Part 4+: Flink, batch, maintenance, cloud

## Layout

```
docker-compose.yml        local stack
infra/kafka/              topic definitions and bootstrap script
docker/                   image definitions
src/cryptozilla/
  config.py               settings from env (one class per concern)
  log.py                  text or json logs
  models/                 shared data contracts (Trade, Instrument)
  messaging/              sink interface + Kafka implementation
  ingest/
    sources/              one adapter per exchange, registered in REGISTRY
    runner.py             reconnect, backoff, stats
    __main__.py           CLI entrypoint
tests/
```

## Running locally

Requires Docker and [uv](https://docs.astral.sh/uv/).

```
make up          # kafka, topic bootstrap, kafka-ui, binance producer
make ps
make logs s=ingest-binance
make consume     # tail trades.raw
```

Kafka UI: http://localhost:8085

Development on the host:

```
make install
make test
make lint
docker compose stop ingest-binance && make ingest-binance
```

## Message format

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

## Adding a source

1. Create `src/cryptozilla/ingest/sources/<name>.py` with a settings class and a
   `Source` subclass (usually `WebSocketSource`) that maps raw messages to `Trade`.
2. Register it in `sources/__init__.py`.
3. Add a service to `docker-compose.yml` with `command: ["<name>"]`.
