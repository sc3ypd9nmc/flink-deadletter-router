# flink-deadletter-router

A small Flink DataStream job that validates order events from Kafka and routes
every rejected record to a dead-letter side output instead of failing the job.

## What and why

One malformed record should not stall a partition, and it should not vanish
either. This job parses each `orders.raw` message, checks the required fields and
shapes, and emits:

* **valid orders** to `orders.valid`, normalised JSON;
* **rejected orders** to `orders.quarantine` as a `DeadLetterEnvelope` carrying
  the original bytes, the parse error, and the source topic/partition/offset.

The dead-letter path is a Flink side output, so the quarantine stream comes from
the same operator invocation that rejected the record. Nothing is re-read from
the broker and no second parser can disagree with the first.

Quarantine keys are `topic:partition:offset`, which makes replay idempotent at
`orders.valid` when the consumer dedupes on that key.

## Delivery guarantees

* Checkpointing every 30 s, exactly-once checkpoint mode; Kafka source commits
  offsets only through completed checkpoints.
* Sinks run at-least-once with idempotent producers. Failure between the sink
  write and the checkpoint restore can repeat a record, never drop one.
* A record is either at `orders.valid` or at `orders.quarantine` after recovery
  from a checkpoint, not at both: the side output is part of the same snapshot.
* Downstream `orders.valid` consumers must dedupe on the envelope key. That is a
  required part of the contract, not an optimisation.

## Build and run

Requires JDK 17 and Maven.

```bash
mvn -q package
flink run -c dev.mikkel.flinkdeadletter.DeadLetterJob \
  target/flink-deadletter-router-0.1.0.jar \
  --bootstrap localhost:9092
```

Optional flags: `--source-topic`, `--sink-topic`, `--quarantine-topic`,
`--group` (all have defaults, see `DeadLetterJob`). The job is meant to run
against a cluster; `mvn -q exec:java` works for a local smoke check but gives
up checkpointing durability.

## Replay procedure

1. Read the quarantine topic from the checkpointed group offset of the failed
   run; if that offset is unknown, start at the run's start timestamp minus the
   checkpoint interval.
2. Re-key each message to its original `orders.raw` topic and produce it back.
3. Consume `orders.valid` with dedupe on `topic:partition:offset` for at least
   one full retention window after replay.
4. Only then delete replayed quarantine records. Records that fail again stay in
   quarantine with an incremented `attempt` count.
5. Never replay by resetting the source group to the earliest offset of the
   whole topic; the valid sink gets a full history rewrite.

## Layout

```
pom.xml
src/main/java/dev/mikkel/flinkdeadletter/
  DeadLetterJob.java      pipeline wiring, Kafka source/sink, checkpointing
  QuarantineRouter.java   validation operator and dead-letter side output
  OrderParser.java        field and shape checks, error classification
  DeadLetterEnvelope.java quarantine record and dedupe key
```

## License

MIT, see `LICENSE`.
