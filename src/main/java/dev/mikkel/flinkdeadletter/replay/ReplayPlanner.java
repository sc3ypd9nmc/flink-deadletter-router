package dev.mikkel.flinkdeadletter.replay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Turns a quarantine JSONL dump into per-partition replay requests.
 *
 * <p>Usage: {@code ReplayPlanner <quarantine.jsonl> [--max-attempt N]}.
 * Records above the attempt limit are counted, not emitted: they failed enough
 * times that a plain replay is likely to fail again.
 *
 * <p>Output is one JSON request per line, sorted by topic and partition, suitable
 * for piping into the replay producer.
 */
public final class ReplayPlanner {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ReplayPlanner() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("usage: ReplayPlanner <quarantine.jsonl> [--max-attempt N]");
            System.exit(2);
        }
        int maxAttempt = 3;
        for (int i = 1; i < args.length - 1; i++) {
            if ("--max-attempt".equals(args[i])) {
                maxAttempt = Integer.parseInt(args[i + 1]);
            }
        }
        try (PrintStream out = System.out) {
            plan(Path.of(args[0]), maxAttempt, out);
        }
    }

    static void plan(Path quarantine, int maxAttempt, PrintStream out) throws IOException {
        Map<String, ReplayRequest> ranges = new TreeMap<>();
        int skipped = 0;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                Files.newInputStream(quarantine), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                JsonNode node = MAPPER.readTree(line);
                if (node.path("attempt").asInt(1) > maxAttempt) {
                    skipped++;
                    continue;
                }
                String topic = required(node, "sourceTopic");
                if (node.path("partition").asInt(-1) < 0) {
                    throw new IOException("missing partition in quarantine record");
                }
                int partition = node.get("partition").asInt();
                long offset = node.get("offset").asLong();
                String key = topic + "/" + partition;

                ReplayRequest current = ranges.get(key);
                if (current == null) {
                    ranges.put(key, ReplayRequest.of(topic, partition, offset, offset, 1,
                            List.of(errorClass(node))));
                } else {
                    Set<String> classes = new LinkedHashSet<>(current.errorClasses());
                    classes.add(errorClass(node));
                    ranges.put(key, ReplayRequest.of(topic, partition,
                            Math.min(current.fromOffset(), offset),
                            Math.max(current.toOffset(), offset),
                            current.recordCount() + 1, new ArrayList<>(classes)));
                }
            }
        }

        for (ReplayRequest request : ranges.values()) {
            out.println(request.toJson());
        }
        System.err.println("planned " + ranges.size() + " ranges, skipped " + skipped
                + " records above attempt " + maxAttempt);
    }

    private static String required(JsonNode node, String field) throws IOException {
        String value = node.path(field).asText("");
        if (value.isBlank()) {
            throw new IOException("missing " + field + " in quarantine record");
        }
        return value;
    }

    private static String errorClass(JsonNode node) {
        String error = node.path("error").asText("PARSE: unknown");
        int colon = error.indexOf(':');
        return colon < 0 ? error : error.substring(0, colon);
    }
}
