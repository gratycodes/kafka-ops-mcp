package io.github.gratycodes.kafkaops.policy;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ToolPolicyTest {

    private final ToolPolicy readOnly = new ToolPolicy(
            Set.of(Scope.TOPICS_READ, Scope.GROUPS_READ, Scope.DLQ_READ),
            List.of("orders.*", "payments.refunds"),
            List.of("*.DLT", "*-dlq"));

    @Test
    void grantedScopesPass() {
        readOnly.requireScope(Scope.TOPICS_READ);
        readOnly.requireScope(Scope.DLQ_READ);
    }

    @Test
    void writeScopeIsDeniedUnlessGranted() {
        assertThatThrownBy(() -> readOnly.requireScope(Scope.DLQ_REPLAY))
                .isInstanceOf(PolicyViolationException.class)
                .hasMessageContaining("dlq:replay")
                .extracting(e -> ((PolicyViolationException) e).reason()).isEqualTo("scope_missing");
    }

    @Test
    void noScopesMeansEverythingDenied() {
        ToolPolicy none = new ToolPolicy(Set.of(), List.of("*"), List.of("*.DLT"));
        for (Scope s : Scope.values()) {
            assertThatThrownBy(() -> none.requireScope(s)).isInstanceOf(PolicyViolationException.class);
        }
    }

    @Test
    void allowlistUsesGlobs() {
        assertThat(readOnly.isTopicAllowed("orders.created")).isTrue();
        assertThat(readOnly.isTopicAllowed("orders.created.DLT")).isTrue();
        assertThat(readOnly.isTopicAllowed("payments.refunds")).isTrue();
        assertThat(readOnly.isTopicAllowed("payments.refunds.DLT")).isFalse();
        assertThat(readOnly.isTopicAllowed("ordersX")).isFalse();
        assertThat(readOnly.isTopicAllowed(null)).isFalse();
    }

    @Test
    void globMetacharactersInTopicNamesAreLiteral() {
        ToolPolicy p = new ToolPolicy(Set.of(Scope.TOPICS_READ), List.of("a.b"), List.of());
        assertThat(p.isTopicAllowed("a.b")).isTrue();
        assertThat(p.isTopicAllowed("aXb")).isFalse();
    }

    @Test
    void emptyAllowlistDeniesEverything() {
        ToolPolicy p = new ToolPolicy(Set.of(Scope.TOPICS_READ), List.of(), List.of("*.DLT"));
        assertThat(p.isTopicAllowed("orders")).isFalse();
        assertThatThrownBy(() -> p.requireTopic("orders"))
                .isInstanceOf(PolicyViolationException.class)
                .extracting(e -> ((PolicyViolationException) e).reason()).isEqualTo("topic_not_allowed");
    }

    @Test
    void internalTopicsAreAlwaysDeniedEvenWithWildcard() {
        ToolPolicy p = new ToolPolicy(Set.of(Scope.TOPICS_READ), List.of("*"), List.of("*"));
        assertThat(p.isTopicAllowed("__consumer_offsets")).isFalse();
        assertThat(p.isTopicAllowed("__transaction_state")).isFalse();
        assertThat(p.isTopicAllowed("orders")).isTrue();
    }

    @Test
    void dlqToolsNeedADlqTopic() {
        readOnly.requireDlqTopic("orders.created.DLT");
        assertThatThrownBy(() -> readOnly.requireDlqTopic("orders.created"))
                .isInstanceOf(PolicyViolationException.class)
                .extracting(e -> ((PolicyViolationException) e).reason()).isEqualTo("not_a_dlq_topic");
    }

    @Test
    void dlqPatternDoesNotBypassAllowlist() {
        assertThatThrownBy(() -> readOnly.requireDlqTopic("billing.invoices.DLT"))
                .extracting(e -> ((PolicyViolationException) e).reason()).isEqualTo("topic_not_allowed");
    }

    @Test
    void scopeIdsParseAndUnknownScopesFailFast() {
        assertThat(Scope.fromId("dlq:replay")).isEqualTo(Scope.DLQ_REPLAY);
        assertThat(Scope.fromId(" TOPICS:READ ")).isEqualTo(Scope.TOPICS_READ);
        assertThatThrownBy(() -> Scope.fromId("admin:*")).isInstanceOf(IllegalArgumentException.class);
        assertThat(Scope.DLQ_REPLAY.isWrite()).isTrue();
        assertThat(Scope.DLQ_READ.isWrite()).isFalse();
    }
}
