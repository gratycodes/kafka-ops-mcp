package io.github.gratycodes.kafkaops.it;

import io.github.gratycodes.kafkaops.audit.AuditEvent;
import io.github.gratycodes.kafkaops.support.KafkaFixture;
import io.github.gratycodes.kafkaops.support.ServiceHarness;
import io.github.gratycodes.kafkaops.tools.Results;
import io.github.gratycodes.kafkaops.tools.ToolRunner.ToolFailure;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.github.gratycodes.kafkaops.support.KafkaFixture.str;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end behaviour against a real broker. Subclasses decide which broker: an embedded KRaft broker locally,
 * a Testcontainers Apache Kafka broker in CI.
 */
abstract class KafkaOpsScenarios {

    protected abstract String bootstrap();

    KafkaFixture kafka;
    ServiceHarness ops;
    ServiceHarness readOnly;
    String orders;
    String dlq;
    String hidden;

    @BeforeEach
    void setUp() {
        kafka = new KafkaFixture(bootstrap());
        ops = new ServiceHarness(bootstrap(), true);
        readOnly = new ServiceHarness(bootstrap(), false);
        orders = KafkaFixture.unique("orders.");
        dlq = orders + ".DLT";
        hidden = KafkaFixture.unique("ledger.");
        kafka.createTopics(orders, dlq, hidden);
    }

    @AfterEach
    void tearDown() {
        ops.close();
        readOnly.close();
        kafka.close();
    }

    private String dead(String key, String payload) {
        return String.valueOf(kafka.send(dlq, key, payload,
                "kafka_dlt-original-topic", orders,
                "kafka_dlt-exception-fqcn", "org.example.ValidationException",
                "trace-id", "t-" + key));
    }

    // ------------------------------------------------------------- topics and groups

    @Test
    void listTopicsShowsOnlyAllowlistedTopics() {
        Results.TopicList r = ops.tools.listTopics(null, orders);
        assertThat(r.topics()).extracting(Results.TopicSummary::name).containsExactlyInAnyOrder(orders, dlq);
        assertThat(r.hiddenByAllowlist()).isGreaterThanOrEqualTo(1);
        assertThat(ops.tools.listTopics(null, null).topics()).extracting(Results.TopicSummary::name)
                .doesNotContain(hidden);
    }

    @Test
    void describeTopicReportsOffsetsAndConfig() {
        kafka.send(orders, "k1", "{\"orderId\":\"o-1\"}");
        kafka.send(orders, "k2", "{\"orderId\":\"o-2\"}");
        Results.TopicDetails d = ops.tools.describeTopic(null, orders);
        assertThat(d.partitions()).isEqualTo(1);
        assertThat(d.approxMessages()).isEqualTo(2);
        assertThat(d.partitionDetails().getFirst().latestOffset()).isEqualTo(2);
        assertThat(d.config()).containsKey("cleanup.policy");
        assertThat(d.underReplicated()).isFalse();
    }

    @Test
    void describeTopicOutsideAllowlistIsDeniedAndAudited() {
        assertThatThrownBy(() -> ops.tools.describeTopic(null, hidden))
                .isInstanceOf(ToolFailure.class).hasMessageContaining("allowlist");
        assertThat(ops.audit.last().outcome()).isEqualTo(AuditEvent.Outcome.DENIED);
        assertThat(ops.audit.last().principal()).isEqualTo("test-agent");
    }

    @Test
    void consumerGroupLagHidesTopicsOutsideAllowlist() {
        for (int i = 0; i < 5; i++) {
            kafka.send(orders, "k" + i, "v" + i);
            kafka.send(hidden, "k" + i, "v" + i);
        }
        String group = KafkaFixture.unique("billing-consumer-");
        kafka.commit(group, orders, 2);
        kafka.commit(group, hidden, 1);
        Results.GroupLag lag = ops.tools.consumerGroupLag(null, group);
        assertThat(lag.totalLag()).isEqualTo(3);
        assertThat(lag.partitions()).singleElement().satisfies(p -> {
            assertThat(p.topic()).isEqualTo(orders);
            assertThat(p.committedOffset()).isEqualTo(2);
            assertThat(p.endOffset()).isEqualTo(5);
        });
        assertThat(lag.hiddenPartitions()).isEqualTo(1);
    }

    @Test
    void invalidGroupIdIsRejected() {
        assertThatThrownBy(() -> ops.tools.consumerGroupLag(null, "../../etc"))
                .isInstanceOf(ToolFailure.class).hasMessageContaining("Invalid argument");
    }

    // ------------------------------------------------------------- browse

    @Test
    void browseRedactsHeadersAndPayloadFields() {
        kafka.send(dlq, "cust-42",
                "{\"orderId\":\"o-9\",\"cardNumber\":\"4111111111111111\",\"email\":\"jane@example.com\","
                        + "\"note\":\"call me at jane@example.com\"}",
                "Authorization", "Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2lnbmF0dXJl",
                "kafka_dlt-original-topic", orders);
        Results.DlqPage page = ops.tools.browseDlq(null, dlq, null, null, null, null, null, null);
        String asJson = ServiceHarness.JSON.writeValueAsString(page);
        assertThat(asJson).doesNotContain("4111111111111111", "jane@example.com", "eyJhbGci");
        Results.DlqMessage m = page.messages().getFirst();
        assertThat(m.headers()).containsEntry("Authorization", "[REDACTED:header]");
        assertThat(m.payload()).contains("\"orderId\":\"o-9\"");
        assertThat(m.originalTopic()).isEqualTo(orders);
        assertThat(m.redactions()).isEqualTo(4);
        assertThat(page.contentTrust()).isEqualTo("untrusted");
    }

    @Test
    void browseReturnsMostRecentByDefaultAndHonoursOffsets() {
        for (int i = 0; i < 6; i++) {
            dead("k" + i, "{\"n\":" + i + "}");
        }
        Results.DlqPage latest = ops.tools.browseDlq(null, dlq, null, null, 2, null, null, null);
        assertThat(latest.messages()).extracting(Results.DlqMessage::offset).containsExactly(4L, 5L);
        Results.DlqPage from = ops.tools.browseDlq(null, dlq, 0, 1L, 3, null, null, null);
        assertThat(from.messages()).extracting(Results.DlqMessage::offset).containsExactly(1L, 2L, 3L);
    }

    @Test
    void browseFiltersRunOnRedactedTextSoTheyCannotProbeSecrets() {
        kafka.send(dlq, "k1", "{\"password\":\"hunter2\",\"status\":\"FAILED\"}", "kafka_dlt-original-topic", orders);
        kafka.send(dlq, "k2", "{\"status\":\"OK\"}", "kafka_dlt-original-topic", orders);
        assertThat(ops.tools.browseDlq(null, dlq, null, null, null, "FAILED", null, null).returned()).isEqualTo(1);
        assertThat(ops.tools.browseDlq(null, dlq, null, null, null, "hunter2", null, null).returned()).isZero();
        assertThat(ops.tools.browseDlq(null, dlq, null, null, null, null, "kafka_dlt-original-topic", "orders")
                .returned()).isEqualTo(2);
    }

    @Test
    void browseRefusesNonDlqTopics() {
        assertThatThrownBy(() -> ops.tools.browseDlq(null, orders, null, null, null, null, null, null))
                .isInstanceOf(ToolFailure.class).hasMessageContaining("dlq-topic-patterns");
    }

    @Test
    void browseDoesNotCommitOrJoinAGroup() {
        dead("k", "{}");
        ops.tools.browseDlq(null, dlq, null, null, null, null, null, null);
        // A browse that joined a group (default client id "kafka-ops-mcp") would make this group exist.
        assertThatThrownBy(() -> ops.tools.consumerGroupLag(null, "kafka-ops-mcp"))
                .isInstanceOf(ToolFailure.class).hasMessageContaining("not found");
    }

    // ------------------------------------------------------------- replay

    @Test
    void dryRunThenReplayCopiesRecordsBackWithProvenanceHeaders() {
        dead("k0", "{\"orderId\":\"o-0\"}");
        dead("k1", "{\"orderId\":\"o-1\"}");
        dead("k2", "{\"orderId\":\"o-2\"}");

        Results.DryRun plan = ops.tools.replayDlqDryRun(null, dlq, 0, 0, 2, null);
        assertThat(plan.replayEnabled()).isTrue();
        assertThat(plan.plan().replayable()).isEqualTo(3);
        assertThat(plan.plan().countsByTarget()).containsEntry(orders, 3);
        assertThat(plan.approvalToken()).startsWith("kopr_");
        assertThat(kafka.endOffset(orders)).as("dry run must not write").isZero();

        Results.Replay done = ops.tools.replayDlq(null, plan.approvalToken());
        assertThat(done.status()).isEqualTo("completed");
        assertThat(done.replayed()).isEqualTo(3);

        List<ConsumerRecord<byte[], byte[]>> replayed = kafka.readAll(orders);
        assertThat(replayed).hasSize(3);
        ConsumerRecord<byte[], byte[]> first = replayed.getFirst();
        assertThat(str(first.key())).isEqualTo("k0");
        assertThat(str(first.value())).isEqualTo("{\"orderId\":\"o-0\"}");
        assertThat(str(first.headers().lastHeader("kafka-ops-replay-of").value())).isEqualTo(dlq + "/0/0");
        assertThat(str(first.headers().lastHeader("kafka-ops-replay-plan").value())).isEqualTo(plan.plan().planId());
        assertThat(str(first.headers().lastHeader("trace-id").value())).isEqualTo("t-k0");
        assertThat(first.headers().lastHeader("kafka_dlt-original-topic")).as("DLT headers stripped").isNull();
        assertThat(kafka.endOffset(dlq)).as("DLQ untouched").isEqualTo(3);

        assertThat(ops.audit.events).extracting(AuditEvent::tool, AuditEvent::outcome).containsSubsequence(
                org.assertj.core.groups.Tuple.tuple("replay_dlq_dry_run", AuditEvent.Outcome.OK),
                org.assertj.core.groups.Tuple.tuple("replay_dlq", AuditEvent.Outcome.ATTEMPT),
                org.assertj.core.groups.Tuple.tuple("replay_dlq", AuditEvent.Outcome.OK));
        assertThat(ServiceHarness.JSON.writeValueAsString(ops.audit.events)).doesNotContain(plan.approvalToken());
    }

    @Test
    void approvalTokenIsSingleUse() {
        dead("k0", "{}");
        String token = ops.tools.replayDlqDryRun(null, dlq, 0, 0, 0, null).approvalToken();
        ops.tools.replayDlq(null, token);
        assertThatThrownBy(() -> ops.tools.replayDlq(null, token))
                .isInstanceOf(ToolFailure.class).hasMessageContaining("already used");
        assertThat(kafka.endOffset(orders)).isEqualTo(1);
    }

    @Test
    void replayWithoutWriteScopeIsDeniedAndDryRunIssuesNoToken() {
        dead("k0", "{}");
        Results.DryRun plan = readOnly.tools.replayDlqDryRun(null, dlq, 0, 0, 0, null);
        assertThat(plan.replayEnabled()).isFalse();
        assertThat(plan.approvalToken()).isNull();
        assertThat(plan.plan().replayable()).isEqualTo(1);

        // Even a valid token from a write-enabled server is useless on a read-only one.
        String token = ops.tools.replayDlqDryRun(null, dlq, 0, 0, 0, null).approvalToken();
        assertThatThrownBy(() -> readOnly.tools.replayDlq(null, token))
                .isInstanceOf(ToolFailure.class).hasMessageContaining("dlq:replay");
        assertThat(kafka.endOffset(orders)).isZero();
    }

    @Test
    void dryRunSkipsRecordsItMustNotReplay() {
        kafka.send(dlq, "a", "{}");                                                     // no header
        kafka.send(dlq, "b", "{}", "kafka_dlt-original-topic", hidden);                 // not allowlisted
        kafka.send(dlq, "c", "{}", "kafka_dlt-original-topic", dlq);                    // loop into a DLQ
        kafka.send(dlq, "d", "{}", "kafka_dlt-original-topic", orders + "-missing");    // does not exist
        kafka.send(dlq, "e", "{}", "kafka_dlt-original-topic", "__consumer_offsets");   // internal
        kafka.send(dlq, "f", "{}", "kafka_dlt-original-topic", orders);                 // fine

        Results.DryRun plan = ops.tools.replayDlqDryRun(null, dlq, 0, 0, 5, null);
        assertThat(plan.plan().replayable()).isEqualTo(1);
        assertThat(plan.plan().skipped()).extracting(s -> s.offset() + ":" + s.reason()).containsExactly(
                "0:no original-topic header and no target_topic given",
                "1:target topic is not on the allowlist",
                "2:target topic is itself a dead-letter topic",
                "3:target topic does not exist",
                "4:target topic is not on the allowlist");
    }

    @Test
    void explicitTargetMustBeAllowlisted() {
        dead("k0", "{}");
        assertThatThrownBy(() -> ops.tools.replayDlqDryRun(null, dlq, 0, 0, 0, hidden))
                .isInstanceOf(ToolFailure.class).hasMessageContaining("allowlist");
    }

    @Test
    void plansAreBounded() {
        assertThatThrownBy(() -> ops.tools.replayDlqDryRun(null, dlq, 0, 0, 10_000, null))
                .isInstanceOf(ToolFailure.class).hasMessageContaining("at most 10");
        assertThatThrownBy(() -> ops.tools.replayDlqDryRun(null, dlq, 0, 5, 1, null))
                .isInstanceOf(ToolFailure.class).hasMessageContaining("Invalid argument");
    }

    @Test
    void replayAbortsWithoutWritingIfThePlannedRecordsChanged() {
        dead("k0", "{}");
        dead("k1", "{}");
        String token = ops.tools.replayDlqDryRun(null, dlq, 0, 0, 1, null).approvalToken();
        kafka.deleteRecordsBefore(dlq, 1); // retention/cleanup removed offset 0 after the plan was approved
        assertThatThrownBy(() -> ops.tools.replayDlq(null, token))
                .isInstanceOf(ToolFailure.class).hasMessageContaining("Nothing was replayed");
        assertThat(kafka.endOffset(orders)).isZero();
    }
}
