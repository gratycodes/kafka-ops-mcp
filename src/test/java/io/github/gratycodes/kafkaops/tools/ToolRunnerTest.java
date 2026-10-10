package io.github.gratycodes.kafkaops.tools;

import io.github.gratycodes.kafkaops.audit.AuditEvent;
import io.github.gratycodes.kafkaops.audit.AuditEvent.Outcome;
import io.github.gratycodes.kafkaops.policy.PolicyViolationException;
import io.github.gratycodes.kafkaops.policy.Scope;
import io.github.gratycodes.kafkaops.policy.ToolPolicy;
import io.github.gratycodes.kafkaops.redact.Redactor;
import io.github.gratycodes.kafkaops.support.MutableClock;
import io.github.gratycodes.kafkaops.support.RecordingAuditSink;
import io.github.gratycodes.kafkaops.telemetry.ToolTracer;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ToolRunnerTest {

    private final InMemorySpanExporter spans = InMemorySpanExporter.create();
    private final OpenTelemetrySdk otel = OpenTelemetrySdk.builder()
            .setTracerProvider(SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(spans)).build())
            .build();
    private final RecordingAuditSink audit = new RecordingAuditSink();
    private final JsonMapper json = JsonMapper.builder().build();
    private final MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
    private final Caller caller = new Caller("oncall-bot", "claude-desktop/1.0", "sess-1");

    record Ok(String s) implements Summarized {
        @Override
        public String auditSummary() {
            return "summary:" + s;
        }
    }

    private ToolRunner runner(Set<Scope> scopes, boolean captureArgs) {
        return runner(scopes, captureArgs, audit::record);
    }

    private ToolRunner runner(Set<Scope> scopes, boolean captureArgs, io.github.gratycodes.kafkaops.audit.AuditSink sink) {
        return new ToolRunner(new ToolPolicy(scopes, List.of("*"), List.of("*.DLT")), sink, new ToolTracer(otel),
                Redactor.withDefaults(json), json, clock, captureArgs);
    }

    @AfterEach
    void close() {
        otel.close();
    }

    @Test
    void successIsAuditedAndTracedWithGenAiConventions() {
        Ok r = runner(Set.of(Scope.TOPICS_READ), false)
                .run("list_topics", "List topics", Scope.TOPICS_READ, Map.of("nameContains", "orders"), caller,
                        () -> new Ok("x"));
        assertThat(r.s()).isEqualTo("x");

        AuditEvent e = audit.last();
        assertThat(e.principal()).isEqualTo("oncall-bot");
        assertThat(e.client()).isEqualTo("claude-desktop/1.0");
        assertThat(e.tool()).isEqualTo("list_topics");
        assertThat(e.scope()).isEqualTo("topics:read");
        assertThat(e.outcome()).isEqualTo(Outcome.OK);
        assertThat(e.summary()).isEqualTo("summary:x");
        assertThat(e.args()).containsEntry("nameContains", "orders");
        assertThat(e.traceId()).hasSize(32);

        SpanData span = spans.getFinishedSpanItems().getFirst();
        assertThat(span.getName()).isEqualTo("execute_tool list_topics");
        assertThat(span.getKind()).isEqualTo(SpanKind.INTERNAL);
        assertThat(span.getAttributes().get(ToolTracer.OPERATION_NAME)).isEqualTo("execute_tool");
        assertThat(span.getAttributes().get(ToolTracer.TOOL_NAME)).isEqualTo("list_topics");
        assertThat(span.getAttributes().get(ToolTracer.TOOL_TYPE)).isEqualTo("function");
        assertThat(span.getAttributes().get(ToolTracer.TOOL_DESCRIPTION)).isEqualTo("List topics");
        assertThat(span.getAttributes().get(ToolTracer.MCP_SESSION)).isEqualTo("sess-1");
        assertThat(span.getAttributes().get(ToolTracer.TOOL_CALL_ARGUMENTS)).isNull();
        assertThat(span.getTraceId()).isEqualTo(e.traceId());
    }

    @Test
    void missingScopeIsDeniedBeforeTheBodyRuns() {
        AtomicBoolean ran = new AtomicBoolean();
        assertThatThrownBy(() -> runner(Set.of(Scope.DLQ_READ), false)
                .run("replay_dlq", "d", Scope.DLQ_REPLAY, Map.of("approvalToken", "kopr_abcdefghijklmnopqrstuvwxyz"),
                        caller, () -> {
                            ran.set(true);
                            return new Ok("x");
                        }))
                .isInstanceOf(ToolRunner.ToolFailure.class)
                .hasMessageStartingWith("Denied:");
        assertThat(ran).isFalse();
        AuditEvent e = audit.last();
        assertThat(e.outcome()).isEqualTo(Outcome.DENIED);
        assertThat(e.reason()).isEqualTo("scope_missing");
        assertThat(e.args()).containsEntry("approvalToken", "[REDACTED:field]");
        SpanData span = spans.getFinishedSpanItems().getFirst();
        assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
        assertThat(span.getAttributes().get(ToolTracer.ERROR_TYPE)).isEqualTo("scope_missing");
    }

    @Test
    void policyViolationInsideBodyIsDenied() {
        assertThatThrownBy(() -> runner(Set.of(Scope.TOPICS_READ), false)
                .run("describe_topic", "d", Scope.TOPICS_READ, Map.of("topic", "secret"), caller, () -> {
                    throw new PolicyViolationException("topic_not_allowed", "nope");
                }))
                .isInstanceOf(ToolRunner.ToolFailure.class);
        assertThat(audit.last().outcome()).isEqualTo(Outcome.DENIED);
        assertThat(audit.last().reason()).isEqualTo("topic_not_allowed");
    }

    @Test
    void unexpectedErrorsAreAuditedWithRedactedMessages() {
        assertThatThrownBy(() -> runner(Set.of(Scope.TOPICS_READ), false)
                .run("list_topics", "d", Scope.TOPICS_READ, Map.of(), caller, () -> {
                    throw new IllegalStateException("broker said password=hunter2");
                }))
                .isInstanceOf(ToolRunner.ToolFailure.class)
                .hasMessageNotContaining("hunter2");
        AuditEvent e = audit.last();
        assertThat(e.outcome()).isEqualTo(Outcome.ERROR);
        assertThat(e.reason()).isEqualTo("java.lang.IllegalStateException");
        assertThat(e.summary()).doesNotContain("hunter2");
        assertThat(spans.getFinishedSpanItems().getFirst().getAttributes().get(ToolTracer.ERROR_TYPE))
                .isEqualTo("java.lang.IllegalStateException");
    }

    @Test
    void writesGetAWriteAheadAuditEvent() {
        runner(Set.of(Scope.DLQ_REPLAY), false)
                .run("replay_dlq", "d", Scope.DLQ_REPLAY, Map.of(), caller, () -> new Ok("done"));
        assertThat(audit.events).extracting(AuditEvent::outcome).containsExactly(Outcome.ATTEMPT, Outcome.OK);
    }

    @Test
    void ifTheAuditLogCannotBeWrittenTheWriteDoesNotHappen() {
        AtomicBoolean ran = new AtomicBoolean();
        ToolRunner r = runner(Set.of(Scope.DLQ_REPLAY), false, e -> {
            throw new IllegalStateException("disk full");
        });
        assertThatThrownBy(() -> r.run("replay_dlq", "d", Scope.DLQ_REPLAY, Map.of(), caller, () -> {
            ran.set(true);
            return new Ok("x");
        })).isInstanceOf(ToolRunner.ToolFailure.class);
        assertThat(ran).isFalse();
    }

    @Test
    void readsStillSucceedWhenTheAuditLogFails() {
        ToolRunner r = runner(Set.of(Scope.TOPICS_READ), false, e -> {
            throw new IllegalStateException("disk full");
        });
        assertThat(r.run("list_topics", "d", Scope.TOPICS_READ, Map.of(), caller, () -> new Ok("x")).s()).isEqualTo("x");
    }

    @Test
    void argumentCaptureIsOptInAndRedacted() {
        runner(Set.of(Scope.DLQ_READ), true)
                .run("browse_dlq", "d", Scope.DLQ_READ, Map.of("topic", "a.DLT", "contains", "bob@example.com"), caller,
                        () -> new Ok("x"));
        String captured = spans.getFinishedSpanItems().getFirst().getAttributes().get(ToolTracer.TOOL_CALL_ARGUMENTS);
        assertThat(captured).contains("a.DLT").contains("[REDACTED:email]").doesNotContain("bob@example.com");
    }
}
