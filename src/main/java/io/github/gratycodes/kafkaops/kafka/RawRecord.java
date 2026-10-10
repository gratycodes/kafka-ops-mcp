package io.github.gratycodes.kafkaops.kafka;

import java.util.List;

/** A record as read from Kafka, before redaction. Never returned to a client as-is. */
public record RawRecord(String topic, int partition, long offset, long timestamp, byte[] key, byte[] value,
                        List<RawHeader> headers) {

    public record RawHeader(String name, byte[] value) {
    }

    public String headerAsString(String name) {
        for (int i = headers.size() - 1; i >= 0; i--) {
            RawHeader h = headers.get(i);
            if (h.name().equals(name) && h.value() != null) {
                return new String(h.value(), java.nio.charset.StandardCharsets.UTF_8);
            }
        }
        return null;
    }
}
