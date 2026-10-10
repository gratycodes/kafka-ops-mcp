package io.github.gratycodes.kafkaops.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Thin wrapper over the Kafka admin, consumer and producer clients.
 *
 * <p>Reads never join a consumer group and never commit offsets, so browsing a DLQ cannot move anyone's
 * position. Policy checks are not done here; callers check scopes and the allowlist first.
 */
public class KafkaGateway implements AutoCloseable {

    private final Map<String, Object> base;
    private final Duration timeout;
    private volatile Admin admin;

    public KafkaGateway(String bootstrapServers, Map<String, String> clientProperties, Duration timeout) {
        this.base = new HashMap<>(clientProperties);
        this.base.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.base.put(AdminClientConfig.CLIENT_ID_CONFIG, "kafka-ops-mcp");
        this.timeout = timeout;
    }

    // ---------------------------------------------------------------- admin

    public Set<String> topicNames() {
        return await(admin().listTopics(new ListTopicsOptions().listInternal(false)).names());
    }

    public Map<String, TopicDescription> describe(Collection<String> topics) {
        return await(admin().describeTopics(topics).allTopicNames());
    }

    public Map<String, String> topicConfig(String topic, Set<String> keys) {
        ConfigResource res = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Config config = await(admin().describeConfigs(List.of(res)).all()).get(res);
        Map<String, String> out = new java.util.TreeMap<>();
        config.entries().stream()
                .filter(e -> keys.contains(e.name()))
                .forEach(e -> out.put(e.name(), e.value()));
        return out;
    }

    public Map<TopicPartition, Long> offsets(Collection<TopicPartition> partitions, OffsetSpec spec) {
        if (partitions.isEmpty()) {
            return Map.of();
        }
        Map<TopicPartition, OffsetSpec> req = partitions.stream().collect(Collectors.toMap(Function.identity(), p -> spec));
        return await(admin().listOffsets(req).all()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
    }

    public ConsumerGroupDescription describeGroup(String groupId) {
        try {
            return await(admin().describeConsumerGroups(List.of(groupId)).describedGroups().get(groupId));
        } catch (KafkaUnavailableException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.GroupIdNotFoundException) {
                throw new IllegalArgumentException("Consumer group '" + groupId + "' not found");
            }
            throw e;
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> committedOffsets(String groupId) {
        return await(admin().listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata());
    }

    // ---------------------------------------------------------------- reads

    /**
     * Reads up to {@code limit} records. With {@code fromOffset == null} it returns the most recent records;
     * otherwise records at or after {@code fromOffset}. {@code partition == null} means all partitions.
     */
    public List<RawRecord> read(String topic, Integer partition, Long fromOffset, int limit) {
        try (KafkaConsumer<byte[], byte[]> consumer = consumer()) {
            List<TopicPartition> tps = partition != null
                    ? List.of(new TopicPartition(topic, partition))
                    : consumer.partitionsFor(topic, timeout).stream()
                            .map(p -> new TopicPartition(topic, p.partition())).toList();
            Map<TopicPartition, Long> begin = consumer.beginningOffsets(tps, timeout);
            Map<TopicPartition, Long> end = consumer.endOffsets(tps, timeout);
            Map<TopicPartition, Long> start = new HashMap<>();
            for (TopicPartition tp : tps) {
                long s = fromOffset == null ? end.get(tp) - limit : fromOffset;
                start.put(tp, Math.max(begin.get(tp), s));
            }
            List<RawRecord> out = poll(consumer, start, end, fromOffset == null ? limit * tps.size() : limit);
            if (fromOffset == null) {
                out.sort(Comparator.comparingLong(RawRecord::timestamp).reversed());
                out = new ArrayList<>(out.subList(0, Math.min(limit, out.size())));
            }
            out.sort(Comparator.comparingInt(RawRecord::partition).thenComparingLong(RawRecord::offset));
            return out;
        }
    }

    /** Reads exactly the offsets [from, toInclusive] of one partition (fewer if the log has been truncated). */
    public List<RawRecord> readRange(String topic, int partition, long from, long toInclusive) {
        try (KafkaConsumer<byte[], byte[]> consumer = consumer()) {
            TopicPartition tp = new TopicPartition(topic, partition);
            long end = consumer.endOffsets(List.of(tp), timeout).get(tp);
            long stop = Math.min(end, toInclusive + 1);
            return poll(consumer, Map.of(tp, from), Map.of(tp, stop), (int) Math.max(0, stop - from));
        }
    }

    private List<RawRecord> poll(KafkaConsumer<byte[], byte[]> consumer, Map<TopicPartition, Long> start,
                                 Map<TopicPartition, Long> stop, int max) {
        List<TopicPartition> active = start.keySet().stream()
                .filter(tp -> start.get(tp) < stop.get(tp)).toList();
        List<RawRecord> out = new ArrayList<>();
        if (active.isEmpty() || max <= 0) {
            return out;
        }
        consumer.assign(active);
        active.forEach(tp -> consumer.seek(tp, start.get(tp)));
        long deadline = System.nanoTime() + timeout.toNanos();
        while (out.size() < max && System.nanoTime() < deadline) {
            for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofMillis(200))) {
                TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                if (r.offset() < stop.get(tp) && out.size() < max) {
                    out.add(toRaw(r));
                }
            }
            boolean done = active.stream().allMatch(tp -> consumer.position(tp, timeout) >= stop.get(tp));
            if (done) {
                break;
            }
        }
        return out;
    }

    private static RawRecord toRaw(ConsumerRecord<byte[], byte[]> r) {
        List<RawRecord.RawHeader> headers = new ArrayList<>();
        for (Header h : r.headers()) {
            headers.add(new RawRecord.RawHeader(h.key(), h.value()));
        }
        return new RawRecord(r.topic(), r.partition(), r.offset(), r.timestamp(), r.key(), r.value(), List.copyOf(headers));
    }

    // ---------------------------------------------------------------- writes

    /** Sends records in order with an idempotent, acks=all producer. Stops at the first failure. */
    public List<RecordMetadata> send(List<ProducerRecord<byte[], byte[]>> records) {
        Map<String, Object> cfg = new HashMap<>(base);
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        cfg.put(ProducerConfig.ACKS_CONFIG, "all");
        cfg.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        cfg.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        cfg.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) Math.max(timeout.toMillis(), 1000) + 5000);
        cfg.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) Math.max(timeout.toMillis(), 1000));
        List<RecordMetadata> sent = new ArrayList<>();
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(cfg)) {
            for (ProducerRecord<byte[], byte[]> r : records) {
                try {
                    sent.add(producer.send(r).get(timeout.toMillis() + 5000, TimeUnit.MILLISECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new PartialSendException(sent, e);
                } catch (ExecutionException | TimeoutException e) {
                    throw new PartialSendException(sent, e);
                }
            }
            producer.flush();
        }
        return sent;
    }

    /** Some records were written before a send failed. */
    public static class PartialSendException extends RuntimeException {
        private final List<RecordMetadata> sent;

        PartialSendException(List<RecordMetadata> sent, Throwable cause) {
            super("Send failed after " + sent.size() + " record(s): " + cause.getMessage(), cause);
            this.sent = List.copyOf(sent);
        }

        public List<RecordMetadata> sent() {
            return sent;
        }
    }

    // ---------------------------------------------------------------- plumbing

    private KafkaConsumer<byte[], byte[]> consumer() {
        Map<String, Object> cfg = new HashMap<>(base);
        cfg.remove(ConsumerConfig.GROUP_ID_CONFIG);
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cfg.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        cfg.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return new KafkaConsumer<>(cfg);
    }

    private Admin admin() {
        Admin a = admin;
        if (a == null) {
            synchronized (this) {
                if (admin == null) {
                    Map<String, Object> cfg = new HashMap<>(base);
                    cfg.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) timeout.toMillis());
                    cfg.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) timeout.toMillis());
                    admin = Admin.create(cfg);
                }
                a = admin;
            }
        }
        return a;
    }

    private <T> T await(org.apache.kafka.common.KafkaFuture<T> f) {
        try {
            return f.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaUnavailableException("Interrupted while talking to Kafka", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new KafkaUnavailableException(cause.getClass().getSimpleName() + ": " + cause.getMessage(), cause);
        } catch (TimeoutException e) {
            throw new KafkaUnavailableException("Kafka did not answer within " + timeout, e);
        }
    }

    @Override
    public void close() {
        if (admin != null) {
            admin.close(Duration.ofSeconds(5));
        }
    }

    public static class KafkaUnavailableException extends RuntimeException {
        public KafkaUnavailableException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
