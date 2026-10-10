package io.github.gratycodes.kafkaops.support;

import io.github.gratycodes.kafkaops.config.KafkaOpsProperties;
import io.github.gratycodes.kafkaops.kafka.KafkaGateway;
import io.github.gratycodes.kafkaops.policy.Scope;
import io.github.gratycodes.kafkaops.policy.ToolPolicy;
import io.github.gratycodes.kafkaops.redact.Redactor;
import io.github.gratycodes.kafkaops.replay.ApprovalTokenService;
import io.github.gratycodes.kafkaops.telemetry.ToolTracer;
import io.github.gratycodes.kafkaops.tools.KafkaOpsService;
import io.github.gratycodes.kafkaops.tools.KafkaOpsTools;
import io.github.gratycodes.kafkaops.tools.ToolRunner;
import io.opentelemetry.api.OpenTelemetry;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Duration;
import java.util.List;

/** Wires the real components (no Spring) against a broker, the same way KafkaOpsConfiguration does. */
public class ServiceHarness implements AutoCloseable {

    public static final List<String> ALLOWLIST = List.of("orders*", "payments.refunds*");
    public static final JsonMapper JSON = JsonMapper.builder().build();

    public final KafkaOpsProperties props = new KafkaOpsProperties();
    public final RecordingAuditSink audit = new RecordingAuditSink();
    public final ApprovalTokenService approvals;
    public final KafkaGateway gateway;
    public final KafkaOpsService service;
    public final KafkaOpsTools tools;

    public ServiceHarness(String bootstrap, boolean replayEnabled) {
        props.setBootstrapServers(bootstrap);
        props.setPrincipal("test-agent");
        props.setTopicAllowlist(ALLOWLIST);
        props.setRequestTimeout(Duration.ofSeconds(10));
        props.getReplay().setMaxMessages(10);
        List<Scope> scopes = replayEnabled
                ? List.of(Scope.TOPICS_READ, Scope.GROUPS_READ, Scope.DLQ_READ, Scope.DLQ_REPLAY)
                : List.of(Scope.TOPICS_READ, Scope.GROUPS_READ, Scope.DLQ_READ);
        ToolPolicy policy = new ToolPolicy(scopes, props.getTopicAllowlist(), props.getDlqTopicPatterns());
        Redactor redactor = Redactor.withDefaults(JSON);
        approvals = new ApprovalTokenService(Clock.systemUTC(), Duration.ofMinutes(5), 20);
        gateway = new KafkaGateway(bootstrap, props.getClient(), props.getRequestTimeout());
        service = new KafkaOpsService(gateway, policy, redactor, approvals, props, Clock.systemUTC());
        ToolRunner runner = new ToolRunner(policy, audit, new ToolTracer(OpenTelemetry.noop()), redactor, JSON,
                Clock.systemUTC(), false);
        tools = new KafkaOpsTools(service, runner, props.getPrincipal());
    }

    @Override
    public void close() {
        gateway.close();
    }
}
