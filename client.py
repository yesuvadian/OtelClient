"""
OTel Log Client — sends simulated logs in VARIED formats to test
the server's log normalization.

JSON formats (sent as application/json):
  - Standard:  {timestamp, service, level, message}
  - OTel:      {timeUnixNano, serviceName, severityText, body, traceId, spanId}
  - Syslog:    {datetime, host, priority, msg, facility}
  - CloudApp:  {ts, app, severity, text, environment, region}
  - Minimal:   {message}  (barely anything — server must fill defaults)

Text formats (sent as text/plain):
  - Python:    DEBUG:botocore.hooks:Changing event name...
  - Bracket:   [ERROR] auth-service - Token expired
  - Stamped:   2026-03-07 09:28:44 ERROR auth-service - Token expired
"""

import argparse
import random
import time
import uuid
from datetime import datetime, timezone
import requests


SERVICES = [
    "auth-service",
    "payment-service",
    "order-service",
    "user-service",
    "notification-service",
]

PYTHON_LOGGERS = [
    "botocore.hooks",
    "botocore.credentials",
    "urllib3.connectionpool",
    "bedrock_agentcore.app",
    "uvicorn.access",
    "sqlalchemy.engine",
    "celery.worker",
    "django.request",
]

LEVELS = ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"]

MESSAGES = {
    "DEBUG": [
        "Entering request handler",
        "Cache lookup for session token",
        "Database query executed in 12ms",
        "Serializing response payload",
        "Attempting retry for transient failure",
        "Changing event name from before-parameter-build.route53 to before-parameter-build.route-53",
        "Changing event name from request-created.cloudsearchdomain.Search to request-created.cloudsearch-domain.Search",
    ],
    "INFO": [
        "User login successful",
        "Order placed successfully",
        "Payment processed for $49.99",
        "Email notification sent",
        "Health check passed",
        "Returning streaming response (generator) (0.000s)",
        "Application startup complete",
    ],
    "WARN": [
        "Response time exceeded 2s threshold",
        "Deprecated API version called",
        "Disk usage at 85%",
        "Rate limit approaching for client",
        "Connection pool nearing capacity",
    ],
    "ERROR": [
        "Failed to validate authentication token",
        "Database connection timeout after 30s",
        "Payment gateway returned 502",
        "Unable to resolve downstream service",
        "Message queue consumer lag detected",
    ],
    "CRITICAL": [
        "Out of memory - service restarting",
        "Data corruption detected in primary store",
        "All database replicas unreachable",
        "Certificate expired - TLS handshake failing",
        "Unrecoverable state - manual intervention required",
    ],
}

# ── Level variations per format ──────────────────────────────────────
OTEL_LEVELS = {"DEBUG": "debug", "INFO": "info", "WARN": "warn", "ERROR": "error", "CRITICAL": "fatal"}
SYSLOG_LEVELS = {"DEBUG": "debug", "INFO": "informational", "WARN": "warning", "ERROR": "err", "CRITICAL": "emergency"}
CLOUD_LEVELS = {"DEBUG": "verbose", "INFO": "notice", "WARN": "warning", "ERROR": "failure", "CRITICAL": "panic"}

ENVIRONMENTS = ["production", "staging", "development"]
REGIONS = ["us-east-1", "eu-west-1", "ap-southeast-1"]
FACILITIES = ["kern", "user", "daemon", "auth", "local0"]


# ── JSON Log Generators ─────────────────────────────────────────────

def generate_standard_log():
    """Standard format: {timestamp, service, level, message}"""
    level = random.choice(LEVELS)
    return {
        "timestamp": int(time.time()),
        "service": random.choice(SERVICES),
        "level": level,
        "message": random.choice(MESSAGES[level]),
    }


def generate_otel_log():
    """OTel-style: {timeUnixNano, serviceName, severityText, body, traceId, spanId}"""
    level = random.choice(LEVELS)
    return {
        "timeUnixNano": int(time.time() * 1_000_000_000),
        "serviceName": random.choice(SERVICES),
        "severityText": OTEL_LEVELS[level],
        "body": random.choice(MESSAGES[level]),
        "traceId": uuid.uuid4().hex,
        "spanId": uuid.uuid4().hex[:16],
        "resource": {"sdk.name": "opentelemetry", "sdk.version": "1.27.0"},
    }


def generate_syslog_log():
    """Syslog-style: {datetime, host, priority, msg, facility}"""
    level = random.choice(LEVELS)
    return {
        "datetime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "host": random.choice(SERVICES),
        "priority": SYSLOG_LEVELS[level],
        "msg": random.choice(MESSAGES[level]),
        "facility": random.choice(FACILITIES),
        "pid": random.randint(1000, 9999),
    }


def generate_cloud_log():
    """Cloud app style: {ts, app, severity, text, environment, region}"""
    level = random.choice(LEVELS)
    return {
        "ts": int(time.time() * 1000),
        "app": random.choice(SERVICES),
        "severity": CLOUD_LEVELS[level],
        "text": random.choice(MESSAGES[level]),
        "environment": random.choice(ENVIRONMENTS),
        "region": random.choice(REGIONS),
        "requestId": str(uuid.uuid4()),
    }


def generate_minimal_log():
    """Minimal format: just a message — server fills all defaults."""
    level = random.choice(LEVELS)
    return {
        "message": random.choice(MESSAGES[level]),
    }


# ── Text Log Generators ─────────────────────────────────────────────

def generate_python_log():
    """Python logging style: LEVEL:logger.name:message"""
    level = random.choice(LEVELS)
    logger = random.choice(PYTHON_LOGGERS)
    msg = random.choice(MESSAGES[level])
    return f"{level}:{logger}:{msg}"


def generate_bracket_log():
    """Bracket style: [LEVEL] service - message"""
    level = random.choice(LEVELS)
    svc = random.choice(SERVICES)
    msg = random.choice(MESSAGES[level])
    return f"[{level}] {svc} - {msg}"


def generate_stamped_log():
    """Timestamped style: 2026-03-07 09:28:44 LEVEL service - message"""
    level = random.choice(LEVELS)
    svc = random.choice(SERVICES)
    msg = random.choice(MESSAGES[level])
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return f"{ts} {level} {svc} - {msg}"


# ── Format registry ─────────────────────────────────────────────────

JSON_FORMATS = {
    "standard": generate_standard_log,
    "otel": generate_otel_log,
    "syslog": generate_syslog_log,
    "cloud": generate_cloud_log,
    "minimal": generate_minimal_log,
}

TEXT_FORMATS = {
    "python": generate_python_log,
    "bracket": generate_bracket_log,
    "stamped": generate_stamped_log,
}

ALL_FORMATS = {**JSON_FORMATS, **TEXT_FORMATS}
ALL_FORMAT_NAMES = list(ALL_FORMATS.keys())
JSON_FORMAT_NAMES = list(JSON_FORMATS.keys())
TEXT_FORMAT_NAMES = list(TEXT_FORMATS.keys())


def is_text_format(fmt):
    return fmt in TEXT_FORMATS


def generate_log(fmt="mixed"):
    """Generate a single log in the specified format. 'mixed' picks randomly."""
    if fmt == "mixed":
        fmt = random.choice(ALL_FORMAT_NAMES)
    elif fmt == "mixed-json":
        fmt = random.choice(JSON_FORMAT_NAMES)
    elif fmt == "mixed-text":
        fmt = random.choice(TEXT_FORMAT_NAMES)
    gen = ALL_FORMATS.get(fmt, generate_standard_log)
    return gen(), fmt


def generate_batch(count, fmt="mixed"):
    """Generate a batch of logs, returning (logs_list, format_counts)."""
    logs = []
    format_counts = {}
    for _ in range(count):
        log, used_fmt = generate_log(fmt)
        logs.append(log)
        format_counts[used_fmt] = format_counts.get(used_fmt, 0) + 1
    return logs, format_counts


def send_batch(url, logs, format_counts):
    """
    Send batch to server. Auto-picks content type:
      - All text logs      → text/plain (newline-separated)
      - All JSON/dict logs → application/json
      - Mixed              → application/json with strings in the logs array
    """
    text_count = sum(format_counts.get(f, 0) for f in TEXT_FORMATS)
    json_count = sum(format_counts.get(f, 0) for f in JSON_FORMATS)

    if json_count == 0 and text_count > 0:
        # All text — send as text/plain
        body = "\n".join(logs)
        response = requests.post(url, data=body, headers={"Content-Type": "text/plain"}, timeout=10)
    else:
        # JSON or mixed — send as JSON (text logs become strings in the array)
        payload = {"logs": logs}
        response = requests.post(url, json=payload, timeout=10)

    response.raise_for_status()
    return response.json()


def print_results(response_data, batch_number=None, format_counts=None):
    total = response_data.get("total", 0)
    accepted = response_data.get("accepted", 0)
    rejected = response_data.get("rejected", 0)

    header = f"Batch #{batch_number}" if batch_number else "Results"
    print(f"\n--- {header} ---")
    print(f"  Sent: {total}  |  Accepted: {accepted}  |  Rejected: {rejected}")

    if format_counts:
        fmt_str = ", ".join(f"{k}: {v}" for k, v in sorted(format_counts.items()))
        print(f"  Formats: {fmt_str}")

    # Show details
    for r in response_data.get("results", []):
        if r.get("status") == "rejected":
            print(f"  [REJECTED] Log #{r['index']}: {r.get('errors', 'unknown')}")
        elif r.get("status") == "accepted" and r.get("normalized"):
            n = r["normalized"]
            storage = r.get("storage", "?")
            print(f"  [OK] Log #{r['index']}: [{n['level']:<8}] {n['service']:<24} {n['message'][:50]}  ({storage})")


def run_one_shot(url, count, fmt):
    logs, format_counts = generate_batch(count, fmt)

    print(f"Sending {count} logs to {url} (format: {fmt}) ...")
    for i, log in enumerate(logs):
        if isinstance(log, str):
            print(f"  #{i}: [text] {log[:80]}")
        else:
            keys = ", ".join(log.keys())
            print(f"  #{i}: [json] fields=[{keys}]")

    try:
        data = send_batch(url, logs, format_counts)
        print_results(data, format_counts=format_counts)
    except requests.RequestException as e:
        print(f"\nError: Could not reach server - {e}")


def run_continuous(url, count, interval, fmt):
    batch_number = 0

    print(f"Continuous mode: sending {count} logs every {interval}s to {url} (format: {fmt})")
    print("Press Ctrl+C to stop\n")

    try:
        while True:
            batch_number += 1
            logs, format_counts = generate_batch(count, fmt)

            try:
                data = send_batch(url, logs, format_counts)
                print_results(data, batch_number, format_counts)
            except requests.RequestException as e:
                print(f"\nBatch #{batch_number} failed: {e}")

            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\n\nStopped after {batch_number} batches.")


def main():
    all_choices = [
        "mixed", "mixed-json", "mixed-text",
        "standard", "otel", "syslog", "cloud", "minimal",
        "python", "bracket", "stamped",
    ]

    parser = argparse.ArgumentParser(
        description="OTel Log Client - send simulated logs in varied formats",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Format groups:
  mixed         All formats randomly (JSON + text)
  mixed-json    JSON formats only (standard, otel, syslog, cloud, minimal)
  mixed-text    Text formats only (python, bracket, stamped)

JSON formats:
  standard      {timestamp, service, level, message}
  otel          {timeUnixNano, serviceName, severityText, body, ...}
  syslog        {datetime, host, priority, msg, facility, ...}
  cloud         {ts, app, severity, text, environment, region, ...}
  minimal       {message}

Text formats (sent as text/plain):
  python        DEBUG:botocore.hooks:Changing event name...
  bracket       [ERROR] auth-service - Token expired
  stamped       2026-03-07 09:28:44 ERROR auth-service - Token expired
""",
    )

    parser.add_argument(
        "--url", default="http://localhost:8000/otel/logs",
        help="OTel endpoint URL (default: http://localhost:8000/otel/logs)",
    )
    parser.add_argument(
        "--count", type=int, default=10,
        help="Number of logs per batch (default: 10)",
    )
    parser.add_argument(
        "--continuous", action="store_true",
        help="Run in continuous mode, sending batches at a regular interval",
    )
    parser.add_argument(
        "--interval", type=int, default=5,
        help="Seconds between batches in continuous mode (default: 5)",
    )
    parser.add_argument(
        "--format", dest="fmt", default="mixed",
        choices=all_choices,
        help="Log format to send (default: mixed)",
    )

    args = parser.parse_args()

    if args.continuous:
        run_continuous(args.url, args.count, args.interval, args.fmt)
    else:
        run_one_shot(args.url, args.count, args.fmt)


if __name__ == "__main__":
    main()
