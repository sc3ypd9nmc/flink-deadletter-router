package dev.mikkel.flinkdeadletter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.util.Iterator;

/**
 * Validates one raw order. Errors are classified by prefix so the quarantine
 * consumer can split permanent shape problems from fixable data problems:
 * {@code PARSE}, {@code MISSING}, {@code TYPE}, {@code AMOUNT}, {@code CURRENCY}.
 */
public final class OrderParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private OrderParser() {
    }

    public static JsonNode parse(String raw) throws Rejected {
        if (raw == null || raw.isBlank()) {
            throw new Rejected("PARSE: empty message");
        }

        JsonNode node;
        try {
            node = MAPPER.readTree(raw);
        } catch (Exception e) {
            throw new Rejected("PARSE: " + e.getMessage());
        }

        if (!node.isObject()) {
            throw new Rejected("PARSE: expected JSON object, got " + node.getNodeType());
        }
        if (node.path("order_id").asText("").isBlank()) {
            throw new Rejected("MISSING: order_id");
        }
        if (node.path("customer_id").asText("").isBlank()) {
            throw new Rejected("MISSING: customer_id");
        }

        JsonNode amount = node.get("amount_cents");
        if (amount == null || !amount.isNumber()) {
            throw new Rejected("TYPE: amount_cents must be numeric");
        }
        if (new BigDecimal(amount.asText()).signum() <= 0) {
            throw new Rejected("AMOUNT: amount_cents must be positive");
        }

        String currency = node.path("currency").asText("");
        if (!currency.matches("[A-Z]{3}")) {
            throw new Rejected("CURRENCY: expected ISO-4217 code, got '" + currency + "'");
        }

        rejectBlankStrings(node);
        return node;
    }

    private static void rejectBlankStrings(JsonNode node) throws Rejected {
        Iterator<String> names = node.fieldNames();
        while (names.hasNext()) {
            String name = names.next();
            JsonNode value = node.get(name);
            if (value.isTextual() && value.asText().isBlank() && !"note".equals(name)) {
                throw new Rejected("MISSING: empty " + name);
            }
        }
    }

    /** Permanent or retriable data problem for one record. */
    public static final class Rejected extends Exception {
        public Rejected(String message) {
            super(message, null, false, false);
        }
    }
}
