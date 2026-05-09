"""
python -m mqtt_broker  [--config config.yaml]
"""

import argparse
import asyncio
import logging
import logging.handlers
import signal
import sys

import yaml

from .broker import MQTTBroker

__version__ = "1.0.0"


def setup_logging(cfg: dict) -> None:
    lc      = cfg.get("logging", {})
    level   = getattr(logging, lc.get("level", "INFO").upper(), logging.INFO)
    fmt     = "%(asctime)s [%(levelname)-8s] %(name)s – %(message)s"
    handlers = [logging.StreamHandler(sys.stdout)]

    logfile = lc.get("file")
    if logfile:
        handlers.append(
            logging.handlers.RotatingFileHandler(
                logfile,
                maxBytes    = lc.get("max_bytes",    10 * 1024 * 1024),
                backupCount = lc.get("backup_count", 5),
                encoding    = "utf-8",
            )
        )
    logging.basicConfig(level=level, format=fmt, handlers=handlers)


async def _run(broker: MQTTBroker, log: logging.Logger) -> None:
    """Run the broker and handle shutdown signals."""
    stop = asyncio.Event()

    def _request_stop(signum, frame):
        log.info("Signal %d received — shutting down gracefully…", signum)
        # Thread-safe: schedule stop from the signal handler thread
        broker._loop.call_soon_threadsafe(stop.set)

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT,  _request_stop)

    serve_task = asyncio.create_task(broker.serve())
    stop_task  = asyncio.create_task(stop.wait())

    done, pending = await asyncio.wait(
        {serve_task, stop_task},
        return_when=asyncio.FIRST_COMPLETED,
    )

    for task in pending:
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    broker._executor.shutdown(wait=False)
    log.info("Broker stopped.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Pure-Python MQTT Broker (3.1/3.1.1/5.0)")
    parser.add_argument("--config", "-c", default="config.yaml",
                        help="Path to YAML config (default: config.yaml)")
    parser.add_argument("--version", action="version", version=f"mqtt_broker {__version__}")
    args = parser.parse_args()

    try:
        with open(args.config, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"ERROR: Config file '{args.config}' not found.", file=sys.stderr)
        sys.exit(1)
    except yaml.YAMLError as exc:
        print(f"ERROR: Invalid YAML — {exc}", file=sys.stderr)
        sys.exit(1)

    setup_logging(cfg)
    log = logging.getLogger("mqtt_broker")
    log.info("=== MQTT Broker v%s starting (config: %s) ===", __version__, args.config)

    broker = MQTTBroker(cfg)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    broker._loop = loop

    try:
        loop.run_until_complete(_run(broker, log))
    except KeyboardInterrupt:
        log.info("Interrupted — shutting down.")
    finally:
        loop.close()


if __name__ == "__main__":
    main()
