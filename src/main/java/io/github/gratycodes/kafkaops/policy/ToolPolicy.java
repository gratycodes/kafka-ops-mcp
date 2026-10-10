package io.github.gratycodes.kafkaops.policy;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Decides whether a tool may run and which topics it may touch.
 *
 * <ul>
 *   <li>Scopes: deny unless granted. Write scopes are never granted by default.</li>
 *   <li>Topic allowlist: deny unless a glob matches. An empty allowlist denies everything.</li>
 *   <li>Internal topics ({@code __consumer_offsets}, {@code __transaction_state}, ...) are always denied.</li>
 *   <li>DLQ tools additionally require the topic to match a DLQ pattern, so a broad allowlist
 *       does not turn every business topic into something an agent can "browse" or replay from.</li>
 * </ul>
 */
public final class ToolPolicy {

    private final Set<Scope> granted;
    private final GlobMatcher allowlist;
    private final GlobMatcher dlqPatterns;

    public ToolPolicy(Collection<Scope> granted, List<String> topicAllowlist, List<String> dlqTopicPatterns) {
        this.granted = granted.isEmpty() ? EnumSet.noneOf(Scope.class) : EnumSet.copyOf(granted);
        this.allowlist = new GlobMatcher(topicAllowlist, false);
        this.dlqPatterns = new GlobMatcher(dlqTopicPatterns, false);
    }

    public Set<Scope> grantedScopes() {
        return Set.copyOf(granted);
    }

    public boolean has(Scope scope) {
        return granted.contains(scope);
    }

    public void requireScope(Scope scope) {
        if (!granted.contains(scope)) {
            throw new PolicyViolationException("scope_missing",
                    "Scope '" + scope.id() + "' is not granted to this server. "
                            + (scope.isWrite()
                                    ? "Writes are disabled by default; an operator must add it to kafka-ops.scopes."
                                    : "Add it to kafka-ops.scopes to enable this tool."));
        }
    }

    public boolean isTopicAllowed(String topic) {
        return topic != null && !topic.startsWith("__") && allowlist.matches(topic);
    }

    public void requireTopic(String topic) {
        if (!isTopicAllowed(topic)) {
            throw new PolicyViolationException("topic_not_allowed",
                    "Topic '" + topic + "' is not on the kafka-ops.topic-allowlist.");
        }
    }

    public void requireDlqTopic(String topic) {
        requireTopic(topic);
        if (!dlqPatterns.matches(topic)) {
            throw new PolicyViolationException("not_a_dlq_topic",
                    "Topic '" + topic + "' does not match kafka-ops.dlq-topic-patterns.");
        }
    }
}
