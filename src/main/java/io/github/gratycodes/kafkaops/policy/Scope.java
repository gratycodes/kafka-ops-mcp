package io.github.gratycodes.kafkaops.policy;

import java.util.Arrays;

/**
 * Permission a tool needs. Scopes are granted per server instance in configuration;
 * anything not listed is denied. Only {@link #DLQ_REPLAY} allows a write.
 */
public enum Scope {
    TOPICS_READ("topics:read"),
    GROUPS_READ("groups:read"),
    DLQ_READ("dlq:read"),
    DLQ_REPLAY("dlq:replay");

    private final String id;

    Scope(String id) {
        this.id = id;
    }

    public String id() {
        return id;
    }

    public boolean isWrite() {
        return this == DLQ_REPLAY;
    }

    public static Scope fromId(String id) {
        return Arrays.stream(values())
                .filter(s -> s.id.equalsIgnoreCase(id.trim()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Unknown scope '" + id + "'. Known scopes: " + Arrays.toString(
                                Arrays.stream(values()).map(Scope::id).toArray())));
    }
}
