package io.github.gratycodes.kafkaops.tools;

import java.util.List;
import java.util.Map;

/** Shapes returned to MCP clients. Everything here has already passed the allowlist and the redactor. */
public final class Results {

    private Results() {
    }

    public record TopicSummary(String name, int partitions, int replicationFactor) {
    }

    public record TopicList(List<TopicSummary> topics, int hiddenByAllowlist) implements Summarized {
        @Override
        public String auditSummary() {
            return topics.size() + " topic(s) listed, " + hiddenByAllowlist + " hidden";
        }
    }

    public record PartitionDetail(int partition, Integer leader, List<Integer> replicas, List<Integer> isr,
                                  long earliestOffset, long latestOffset) {
    }

    public record TopicDetails(String name, int partitions, int replicationFactor, long approxMessages,
                               boolean underReplicated, Map<String, String> config,
                               List<PartitionDetail> partitionDetails) implements Summarized {
        @Override
        public String auditSummary() {
            return name + ": " + partitions + " partition(s), ~" + approxMessages + " message(s)";
        }
    }

    public record PartitionLag(String topic, int partition, long committedOffset, long endOffset, long lag) {
    }

    public record GroupLag(String groupId, String state, int members, long totalLag, List<PartitionLag> partitions,
                           int hiddenPartitions) implements Summarized {
        @Override
        public String auditSummary() {
            return groupId + ": state=" + state + ", totalLag=" + totalLag + ", " + partitions.size()
                    + " partition(s), " + hiddenPartitions + " hidden";
        }
    }

    public record DlqMessage(int partition, long offset, String timestamp, String key, Map<String, String> headers,
                             String payload, boolean payloadTruncated, String originalTopic, int redactions,
                             List<String> suspectedInjection) {
    }

    /**
     * {@code contentTrust} and {@code notice} are on every browse result so an agent reading the JSON sees,
     * next to the data, that message content is not instructions.
     */
    public record DlqPage(String topic, String contentTrust, String notice, int scanned, int returned,
                          int redactions, int flaggedMessages, List<DlqMessage> messages) implements Summarized {
        @Override
        public String auditSummary() {
            return topic + ": scanned " + scanned + ", returned " + returned + ", " + redactions
                    + " redaction(s), " + flaggedMessages + " flagged as possible prompt injection";
        }
    }

    public record PlanView(String planId, String dlqTopic, int partition, long fromOffset, long toOffset,
                           int replayable, Map<String, Integer> countsByTarget,
                           List<io.github.gratycodes.kafkaops.replay.ReplayPlan.SkippedMessage> skipped) {
    }

    public record DryRun(PlanView plan, boolean replayEnabled, String approvalToken, String expiresAt,
                         String next) implements Summarized {
        @Override
        public String auditSummary() {
            return "plan " + plan.planId() + ": " + plan.replayable() + " replayable, " + plan.skipped().size()
                    + " skipped, token " + (approvalToken != null ? "issued" : "not issued");
        }
    }

    public record Replay(String planId, int replayed, Map<String, Integer> countsByTarget, String status,
                         String detail) implements Summarized {
        @Override
        public String auditSummary() {
            return "plan " + planId + ": " + status + ", replayed " + replayed;
        }
    }
}
