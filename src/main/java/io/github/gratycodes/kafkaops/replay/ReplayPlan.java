package io.github.gratycodes.kafkaops.replay;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Immutable result of a dry run. The plan is stored server-side and bound to an approval token,
 * so the agent cannot alter offsets or targets between "plan" and "execute".
 *
 * @param planId       random id, also written to replayed records as a header
 * @param dlqTopic     source dead-letter topic
 * @param partition    source partition
 * @param fromOffset   first offset considered (inclusive)
 * @param toOffset     last offset considered (inclusive)
 * @param messages     records that will be replayed, with a fingerprint of their content
 * @param skipped      records that will not be replayed, with the reason
 * @param countsByTarget number of records per target topic
 * @param createdAt    when the plan was made
 */
public record ReplayPlan(
        String planId,
        String dlqTopic,
        int partition,
        long fromOffset,
        long toOffset,
        List<PlannedMessage> messages,
        List<SkippedMessage> skipped,
        Map<String, Integer> countsByTarget,
        Instant createdAt) {

    public ReplayPlan {
        messages = List.copyOf(messages);
        skipped = List.copyOf(skipped);
        countsByTarget = Map.copyOf(countsByTarget);
    }

    public record PlannedMessage(long offset, String targetTopic, String fingerprint) {
    }

    public record SkippedMessage(long offset, String reason) {
    }
}
