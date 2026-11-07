import argparse
import asyncio
import contextlib
import logging
import signal

from cryptozilla.config import AppSettings, IngestSettings, KafkaSettings
from cryptozilla.ingest.runner import Ingestor
from cryptozilla.ingest.sources import REGISTRY
from cryptozilla.log import setup_logging
from cryptozilla.messaging import KafkaSink

log = logging.getLogger("cryptozilla.ingest")


async def main(source_name: str) -> None:
    source = REGISTRY[source_name].from_env()
    sink = KafkaSink(KafkaSettings())
    ingestor = Ingestor(source, sink, IngestSettings())

    task = asyncio.current_task()
    assert task is not None
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, task.cancel)

    log.info("starting", extra={"source": source_name})
    with contextlib.suppress(asyncio.CancelledError):
        await ingestor.run()


def cli() -> None:
    parser = argparse.ArgumentParser(prog="cryptozilla.ingest")
    parser.add_argument("source", choices=sorted(REGISTRY))
    args = parser.parse_args()

    app = AppSettings()
    setup_logging(app.log_level, app.log_format)
    asyncio.run(main(args.source))


if __name__ == "__main__":
    cli()
