import os
from dataclasses import dataclass, field
from typing import List

from dotenv import load_dotenv


@dataclass
class SQSConfig:
    """Settings for the AWS SQS queue we poll from."""
    queue_url: str = ""
    region: str = ""
    max_messages_per_poll: int = 4        # Fetch up to 4 messages at once
    poll_wait_time: int = 20              # Long-polling wait (seconds)
    visibility_timeout: int = 60          # How long SQS hides a message after we receive it


@dataclass
class SandboxConfig:
    """Settings for nsjail sandbox execution."""
    nsjail_path: str = ""
    config_path: str = ""                 # Path to sandbox_minimal.cfg
    
@dataclass
class ExecutionConfig:
    """Settings that control how code execution works."""
    max_concurrent: int = 4  
    compilation_timeout: int = 30         # Seconds for compilation step
    default_time_limit: int = 5               # max time limit for test case execution = 5 sec
    default_memory_limit: int = 128
             # Max sandboxes running in parallel
    max_output_bytes: int = 262144        # 256 KB output cap
    max_output_lines: int = 5000          # Max lines of output
    temp_dir: str = "/tmp/codinglemon"   # Base directory for temp execution dirs
    output_dir: str = "/tmp/codinglemon/reports"                  # Where execution reports are saved


@dataclass
class RedisConfig:
    """Settings for ElastiCache Redis — used to publish execution reports."""
    host: str = ""
    port: int = 6379
    report_ttl: int = 300    # Seconds to keep report in Redis (5 min is plenty for polling)


@dataclass
class WorkerConfig:
    """Top-level config that groups all sub-configs together."""
    sqs: SQSConfig = field(default_factory=SQSConfig)
    sandbox: SandboxConfig = field(default_factory=SandboxConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    redis: RedisConfig = field(default_factory=RedisConfig)
    max_consecutive_errors: int = 5       # Shutdown after this many back-to-back failures


_DEFAULT_ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")

def load_config(env_file: str) -> WorkerConfig:
    """
    Build a WorkerConfig from environment variables.

    Loads variables from `env_file` first (defaults to .env in the working
    directory), then reads os.environ. Real env vars take precedence over
    the file — dotenv never overwrites an already-set variable.

    Each setting can be overridden by an env var like:
        SQS_QUEUE_URL, AWS_REGION, MAX_CONCURRENT, NSJAIL_PATH, etc.

    Falls back to the defaults defined in the dataclasses above.
    """
    # Ensure the .env file exists before proceeding
    if not os.path.isfile(env_file):
        raise SystemExit(
            f"[config] Required env file '{env_file}' not found. "
            f"Copy .env.example to {env_file} and fill in your values."
        )

    # Load .env file into os.environ. override=False means real env vars
    # (e.g. set in the shell or CI) always win over the file.
    load_dotenv(env_file, override=False)

    config = WorkerConfig()

    # --- SQS ---
    config.sqs.queue_url = os.environ.get("SQS_QUEUE_URL", config.sqs.queue_url)
    config.sqs.region = os.environ.get("AWS_REGION", config.sqs.region)
    config.sqs.max_messages_per_poll = int(
        os.environ.get("SQS_MAX_MESSAGES_PER_POLL", config.sqs.max_messages_per_poll)
    )
    config.sqs.poll_wait_time = int(
        os.environ.get("SQS_POLL_WAIT_TIME", config.sqs.poll_wait_time)
    )
    config.sqs.visibility_timeout = int(
        os.environ.get("SQS_VISIBILITY_TIMEOUT", config.sqs.visibility_timeout)
    )

    # --- Sandbox ---
    config.sandbox.nsjail_path = os.environ.get(
        "NSJAIL_PATH", config.sandbox.nsjail_path
    )
    config.sandbox.config_path = os.environ.get(
        "NSJAIL_CONFIG_PATH", config.sandbox.config_path
    )

    # --- Execution ---
    config.execution.default_time_limit = int(
        os.environ.get("DEFAULT_TIME_LIMIT", config.execution.default_time_limit)
    )
    config.execution.default_memory_limit = int(
        os.environ.get("DEFAULT_MEMORY_LIMIT", config.execution.default_memory_limit)
    )
    config.execution.compilation_timeout = int(
        os.environ.get("COMPILATION_TIMEOUT", config.execution.compilation_timeout)
    )
    config.execution.max_concurrent = int(
        os.environ.get("MAX_CONCURRENT", config.execution.max_concurrent)
    )
    config.execution.max_output_bytes = int(
        os.environ.get("MAX_OUTPUT_BYTES", config.execution.max_output_bytes)
    )
    config.execution.max_output_lines = int(
        os.environ.get("MAX_OUTPUT_LINES", config.execution.max_output_lines)
    )
    config.execution.temp_dir = os.environ.get(
        "TEMP_DIR", config.execution.temp_dir
    )
    config.execution.output_dir = os.environ.get(
        "OUTPUT_DIR", config.execution.output_dir
    )
    # allowed = os.environ.get("ALLOWED_LANGUAGES", "")
    # if allowed:
    #     config.execution.allowed_languages = [
    #         lang.strip().lower() for lang in allowed.split(",")
    #     ]

    # --- Redis ---
    config.redis.host = os.environ.get("REDIS_HOST", config.redis.host)
    config.redis.port = int(os.environ.get("REDIS_PORT", config.redis.port))
    config.redis.report_ttl = int(os.environ.get("REDIS_REPORT_TTL", config.redis.report_ttl))

    # --- Worker ---
    config.max_consecutive_errors = int(
        os.environ.get("MAX_CONSECUTIVE_ERRORS", config.max_consecutive_errors)
    )

    return config
