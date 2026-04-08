"""
Entry point for the CodingLemons worker service.

Usage:
    python -m worker.main --env dev     # reads from .env.dev
    python -m worker.main --env prod    # reads from .env.prod
    python -m worker.main               # reads from .env (default)

Environment variables (all optional with defaults):
    SQS_QUEUE_URL          — SQS queue URL (required for production)
    AWS_REGION             — AWS region (default: ap-south-1)
    NSJAIL_PATH            — Path to nsjail binary (default: /usr/local/bin/nsjail)
    NSJAIL_CONFIG_PATH     — Path to sandbox config (required)
    MAX_CONCURRENT         — Max parallel sandboxes (default: 4)
    DEFAULT_TIME_LIMIT     — Seconds per test case (default: 5)
    DEFAULT_MEMORY_LIMIT   — MB per sandbox (default: 50)
    TEMP_DIR               — Base temp directory (default: /tmp/codinglemons)
    OUTPUT_DIR             — Where reports are saved (default: inside exec dir)
"""
import argparse
import asyncio
import logging
import logging.handlers
import os
import sys

from config import WorkerConfig, load_config
from dispatcher import Dispatcher


def setup_logging() -> None:
    """
    Configure logging for the worker.

    Logs are written to a daily rotating file and also to stdout.
    Log format: [timestamp] LEVEL module — message
    Level is controlled by LOG_LEVEL env var (default: INFO).
    Log directory is controlled by LOG_DIR env var (default: /var/log/codinglemons).
    """
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    log_dir = os.environ.get("LOG_DIR", "/var/log/codinglemons")
    os.makedirs(log_dir, exist_ok=True)

    fmt = logging.Formatter(
        fmt="[%(asctime)s] %(levelname)-7s %(name)-20s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Daily rotating file — rolls over at midnight, keeps 30 days of logs
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=os.path.join(log_dir, "worker.log"),
        when="midnight",
        backupCount=10,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)

    # Keep stdout as well — useful when running manually or checking systemd status
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(fmt)

    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        handlers=[file_handler, stdout_handler],
    )

    # Quiet down noisy libraries
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def validate_config(config: WorkerConfig) -> None:
    """
    Check that required settings are present before starting.

    Fails fast with a clear message rather than crashing later
    with a cryptic boto3 error.
    """
    errors = []

    if not config.sqs.queue_url:
        errors.append("SQS_QUEUE_URL is required")

    if not config.sandbox.nsjail_path:
        errors.append("NSJAIL_PATH is required")

    if not config.sandbox.config_path:
        errors.append("NSJAIL_CONFIG_PATH is required")
    elif not os.path.isfile(config.sandbox.config_path):
        errors.append(f"NSJAIL_CONFIG_PATH does not exist: {config.sandbox.config_path}")

    if not config.redis.host:
        errors.append("REDIS HOST is required")

    if errors:
        for err in errors:
            logging.getLogger(__name__).error("Config error: %s", err)
        sys.exit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CodingLemons Worker Service")
    parser.add_argument(
        "--env",
        default=None,
        help="Environment name to load config from (e.g. dev → .env.dev, prod → .env.prod). "
             "Defaults to .env if not specified.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging()
    logger = logging.getLogger(__name__)

    env_file = f".env.{args.env}" if args.env else ".env.example"
    logger.info("Loading configuration from '%s'...", env_file)
    config = load_config(env_file=env_file)

    validate_config(config)

    # Ensure temp and output directories exist
    os.makedirs(config.execution.temp_dir, exist_ok=True)
    if config.execution.output_dir:
        os.makedirs(config.execution.output_dir, exist_ok=True)

    logger.info("=== CodingLemons Worker Service ===")
    logger.info("  SQS Queue:       %s", config.sqs.queue_url)
    logger.info("  Redis host:       %s", config.redis.host)
    logger.info("  nsjail config:   %s", config.sandbox.config_path)
    logger.info("  Max concurrent:  %d", config.execution.max_concurrent)
    logger.info("  Time limit:      %ds", config.execution.default_time_limit)
    logger.info("  Memory limit:    %dMB", config.execution.default_memory_limit)
    logger.info("  Temp dir:        %s", config.execution.temp_dir)
    logger.info("  Output dir:      %s", config.execution.output_dir or "(inside exec dir)")
    # logger.info("  Languages:       %s", ", ".join(config.execution.allowed_languages))
    logger.info("===================================")

    dispatcher = Dispatcher(config)

    # asyncio.run() creates the event loop, runs the dispatcher,
    # and cleans up when done
    asyncio.run(dispatcher.start())


if __name__ == "__main__":
    main()
