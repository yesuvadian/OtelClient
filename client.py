import argparse
import random
import time
import requests


SERVICES = [
    "auth-service",
    "payment-service",
    "order-service",
    "user-service",
    "notification-service",
]

LEVELS = ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"]

MESSAGES = {
    "DEBUG": [
        "Entering request handler",
        "Cache lookup for session token",
        "Database query executed in 12ms",
        "Serializing response payload",
        "Attempting retry for transient failure",
    ],
    "INFO": [
        "User login successful",
        "Order placed successfully",
        "Payment processed for $49.99",
        "Email notification sent",
        "Health check passed",
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


def generate_log():
    level = random.choice(LEVELS)
    return {
        "timestamp": int(time.time()),
        "service": random.choice(SERVICES),
        "level": level,
        "message": random.choice(MESSAGES[level]),
    }


def generate_batch(count):
    return [generate_log() for _ in range(count)]


def send_batch(url, logs):
    payload = {"logs": logs}
    response = requests.post(url, json=payload, timeout=10)
    response.raise_for_status()
    return response.json()


def print_results(response_data, batch_number=None):
    results = response_data.get("results", [])

    stored = sum(1 for r in results if r["status"] == "stored")
    ignored = sum(1 for r in results if r["status"] == "ignored")

    header = f"Batch #{batch_number}" if batch_number else "Results"
    print(f"\n--- {header} ---")
    print(f"  Sent: {len(results)}  |  Stored: {stored}  |  Ignored: {ignored}")


def run_one_shot(url, count):
    logs = generate_batch(count)

    print(f"Sending {count} logs to {url} ...")
    for log in logs:
        print(f"  [{log['level']:<8}] {log['service']:<24} {log['message']}")

    try:
        data = send_batch(url, logs)
        print_results(data)
    except requests.RequestException as e:
        print(f"\nError: Could not reach server - {e}")


def run_continuous(url, count, interval):
    batch_number = 0

    print(f"Continuous mode: sending {count} logs every {interval}s to {url}")
    print("Press Ctrl+C to stop\n")

    try:
        while True:
            batch_number += 1
            logs = generate_batch(count)

            try:
                data = send_batch(url, logs)
                print_results(data, batch_number)
            except requests.RequestException as e:
                print(f"\nBatch #{batch_number} failed: {e}")

            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\n\nStopped after {batch_number} batches.")


def main():
    parser = argparse.ArgumentParser(description="OTel Log Client - send simulated logs to the OTel endpoint")

    parser.add_argument("--url", default="http://localhost:8000/otel/logs", help="OTel endpoint URL (default: http://localhost:8000/otel/logs)")
    parser.add_argument("--count", type=int, default=10, help="Number of logs per batch (default: 10)")
    parser.add_argument("--continuous", action="store_true", help="Run in continuous mode, sending batches at a regular interval")
    parser.add_argument("--interval", type=int, default=5, help="Seconds between batches in continuous mode (default: 5)")

    args = parser.parse_args()

    if args.continuous:
        run_continuous(args.url, args.count, args.interval)
    else:
        run_one_shot(args.url, args.count)


if __name__ == "__main__":
    main()
