package dev.mikkel.flinkdeadletter;

import java.time.Instant;

/**
 * Quarantined record. Identity is the source coordinates, so replaying the same
 * offset twice produces the same dedupe key at the valid sink.
 */
public record DeadLetterEnvelope(
        String sourceTopic,
        int partition,
        long offset,
        String raw,
        String error,
        int attempt,
        long failedAt) {

    public static DeadLetterEnvelope rejected(
            String sourceTopic, int partition, long offset, String raw, String error) {
        return new DeadLetterEnvelope(
                sourceTopic, partition, offset, raw, error, 1, Instant.now().toEpochMilli());
    }

    public DeadLetterEnvelope retried() {
        return new DeadLetterEnvelope(
                sourceTopic, partition, offset, raw, error, attempt + 1, failedAt);
    }

    public String dedupeKey() {
        return sourceTopic + ":" + partition + ":" + offset;
    }

    public String errorClass() {
        int colon = error.indexOf(':');
        return colon < 0 ? error : error.substring(0, colon);
    }
}
