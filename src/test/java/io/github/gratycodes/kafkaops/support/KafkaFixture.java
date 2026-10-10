package io.github.gratycodes.kafkaops.support;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Test helper that talks to a real broker (embedded or Testcontainers). All data is synthetic. */
public class KafkaFixture implements AutoCloseable {

    private static final AtomicInteger SEQ = new AtomicInteger();

    private final String bootstrap;
    private final Admin admin;
    private final KafkaProducer<byte[], byte[]> producer;

    public KafkaFixture(String bootstrap) {
        this.bootstrap = bootstrap;
        this.admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
        this.producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
    }

    public String bootstrap() {
        return bootstrap;
    }

    /** Unique name so tests sharing a broker do not see each other's data. */
    public static String unique(String prefix) {
        return prefix + SEQ.incrementAndGet() + "-" + Long.toHexString(System.nanoTime() & 0xffffff);
    }

    public void createTopics(String... names) {
        try {
            admin.createTopics(Arrays.stream(names).map(n -> new NewTopic(n, 1, (short) 1)).toList()).all().get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Sends one record. headers alternate name, value. */
    public long send(String topic, String key, String value, String... headers) {
        ProducerRecord<byte[], byte[]> r = new ProducerRecord<>(topic, 0, bytes(key), bytes(value));
        for (int i = 0; i < headers.length; i += 2) {
            r.headers().add(new RecordHeader(headers[i], bytes(headers[i + 1])));
        }
        try {
            return producer.send(r).get().offset();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public long endOffset(String topic) {
        try {
            TopicPartition tp = new TopicPartition(topic, 0);
            return admin.listOffsets(Map.of(tp, OffsetSpec.latest())).all().get().get(tp).offset();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public void commit(String group, String topic, long offset) {
        try {
            admin.alterConsumerGroupOffsets(group, Map.of(new TopicPartition(topic, 0), new OffsetAndMetadata(offset)))
                    .all().get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public void deleteRecordsBefore(String topic, long offset) {
        try {
            admin.deleteRecords(Map.of(new TopicPartition(topic, 0), RecordsToDelete.beforeOffset(offset))).all().get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public List<ConsumerRecord<byte[], byte[]>> readAll(String topic) {
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class))) {
            TopicPartition tp = new TopicPartition(topic, 0);
            c.assign(List.of(tp));
            c.seekToBeginning(List.of(tp));
            long end = c.endOffsets(List.of(tp)).get(tp);
            List<ConsumerRecord<byte[], byte[]>> out = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 10_000;
            while (c.position(tp) < end && System.currentTimeMillis() < deadline) {
                c.poll(Duration.ofMillis(200)).forEach(out::add);
            }
            return out;
        }
    }

    public static String str(byte[] b) {
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }

    private static byte[] bytes(String s) {
        return s == null ? null : s.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        producer.close();
        admin.close();
    }
}
