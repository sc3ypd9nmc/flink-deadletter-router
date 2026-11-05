package dev.mikkel.flinkdeadletter.replay;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;

/**
 * One contiguous replay range for a single source partition, derived from the
 * quarantine log. Offsets are inclusive; the range never crosses partitions,
 * so a replay can be produced back with the original keys and ordering.
 */
public record ReplayRequest(
        String sourceTopic,
        int partition,
        long fromOffset,
        long toOffset,
        int recordCount,
        List<String> errorClasses,
        long generatedAt) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static ReplayRequest of(String sourceTopic, int partition, long fromOffset,
                                   long toOffset, int recordCount, List<String> errorClasses) {
        return new ReplayRequest(sourceTopic, partition, fromOffset, toOffset, recordCount,
                errorClasses, Instant.now().toEpochMilli());
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (Exception e) {
            throw new IllegalStateException("replay request serialisation failed", e);
        }
    }
}
