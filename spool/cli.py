"""spool CLI: enqueue, drain and inspect the local telemetry queue."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .mqtt import MqttError, MqttPublisher
from .spool import Spool


def cmd_add(args: argparse.Namespace) -> int:
    with Spool(args.db) as spool:
        payload = args.payload
        if payload == "-":
            payload = sys.stdin.read().strip()
        message_id = spool.enqueue(args.topic, payload, args.qos)
        print(f"queued id={message_id}", file=sys.stderr)
    return 0


def cmd_feed(args: argparse.Namespace) -> int:
    """Read JSONL from stdin: {"tag": "...", "value": ...} or {"topic","payload"}."""
    count = 0
    with Spool(args.db) as spool:
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                print(f"skipped invalid JSON: {line[:80]}", file=sys.stderr)
                continue
            if "topic" in record:
                topic = record["topic"]
                payload = record.get("payload", record.get("value"))
            else:
                topic = f"{args.topic_prefix}/{record['tag']}"
                payload = record.get("value")
            spool.enqueue(topic, {"value": payload, "ts": record.get("ts")}, args.qos)
            count += 1
    print(f"queued {count} message(s)", file=sys.stderr)
    return 0


def cmd_drain(args: argparse.Namespace) -> int:
    publisher: MqttPublisher | None = None
    sent = failed = 0
    with Spool(args.db) as spool:
        while True:
            batch = spool.claim(args.limit)
            if not batch:
                break
            for message in batch:
                if args.dry_run:
                    print(f"{message.topic} {message.payload}")
                    spool.delivered(message.id)
                    sent += 1
                    continue
                if publisher is None:
                    try:
                        publisher = MqttPublisher(args.mqtt_host, args.mqtt_port)
                        publisher.connect()
                    except (OSError, MqttError) as exc:
                        print(f"broker unavailable: {exc}", file=sys.stderr)
                        return 2
                try:
                    publisher.publish(message.topic, message.payload, message.qos)
                    spool.delivered(message.id)
                    sent += 1
                except (OSError, MqttError) as exc:
                    backoff = min(3600.0, 5.0 * (2 ** message.attempts))
                    spool.failed(message.id, backoff)
                    print(
                        f"delivery failed id={message.id} retry_in={backoff:.0f}s: {exc}",
                        file=sys.stderr,
                    )
                    failed += 1
            if args.once:
                break
    if publisher:
        publisher.close()
    print(f"drained sent={sent} deferred={failed}", file=sys.stderr)
    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    with Spool(args.db) as spool:
        stats = spool.stats()
    if args.json:
        print(json.dumps(stats, indent=2))
    else:
        print(f"spool    : {stats['path']}")
        print(f"pending  : {stats['pending']}")
        print(f"delivered: {stats['delivered']}")
        print(f"oldest   : {stats['oldest_pending_s']:.0f}s")
        print(f"attempts : {stats['max_attempts']}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="telemetry-spool",
        description="Durable store-and-forward queue for MQTT telemetry.",
    )
    parser.add_argument("--db", default="spool.sqlite3")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("add", help="enqueue one message")
    p.add_argument("--topic", required=True)
    p.add_argument("--payload", required=True, help="JSON string or '-' for stdin")
    p.add_argument("--qos", type=int, default=1, choices=[0, 1])
    p.set_defaults(func=cmd_add)

    p = sub.add_parser("feed", help="enqueue JSONL from stdin")
    p.add_argument("--topic-prefix", default="telemetry")
    p.add_argument("--qos", type=int, default=1, choices=[0, 1])
    p.set_defaults(func=cmd_feed)

    p = sub.add_parser("drain", help="publish queued messages")
    p.add_argument("--mqtt-host", default="127.0.0.1")
    p.add_argument("--mqtt-port", type=int, default=1883)
    p.add_argument("--limit", type=int, default=50)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--once", action="store_true", help="one claim batch only")
    p.set_defaults(func=cmd_drain)

    p = sub.add_parser("stats", help="queue depth and age")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_stats)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except (OSError, RuntimeError, KeyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
