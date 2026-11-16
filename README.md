# telemetry-spool

Store-and-forward queue for MQTT telemetry on links that are not always up.

Edge sites drop connectivity. A collector that publishes straight to the broker
loses whatever it read during the outage. This spools rows into SQLite first and
only marks them delivered after the broker acknowledges the PUBLISH.

## Usage

```bash
# enqueue one reading
python3 -m spool add --db spool.sqlite3 --topic telemetry/LT-101.PV \
    --payload '{"value":3.42,"ts":"2026-11-16T08:00:00Z"}'

# enqueue a JSONL stream from a collector
python3 -m spool feed --db spool.sqlite3 --topic-prefix telemetry < readings.jsonl

# send what is queued (dry-run prints to stdout)
python3 -m spool drain --db spool.sqlite3 --mqtt-host 10.42.7.20 --dry-run

# queue depth and age
python3 -m spool stats --db spool.sqlite3 --json
```

## Behaviour worth knowing

- WAL journal, `synchronous=FULL`: the spool survives power loss, at the cost
  of an fsync per enqueue.
- Retry backoff is per message (`5s * 2^attempts`, capped at 1 h), so one bad
  payload does not block the rest of the queue.
- Delivered rows are pruned separately (`prune()`), which keeps the audit trail
  without growing the file forever.
- The spool raises when it is full rather than deleting undelivered telemetry.

## Limits

- QoS 0/1 only, no TLS, no authentication — put it on a private APN/VPN.
- One drainer per spool file; SQLite is the lock, not a scheduler.
- Payloads are stored as text, so keep them small (a few hundred bytes).

## License

MIT.

---

Maintained by Glen Marsh · Helix Corp Ltd
