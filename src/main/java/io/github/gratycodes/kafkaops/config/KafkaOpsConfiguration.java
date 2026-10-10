package io.github.gratycodes.kafkaops.config;

import io.github.gratycodes.kafkaops.audit.AuditSink;
import io.github.gratycodes.kafkaops.audit.JsonLogAuditSink;
import io.github.gratycodes.kafkaops.kafka.KafkaGateway;
import io.github.gratycodes.kafkaops.policy.Scope;
import io.github.gratycodes.kafkaops.policy.ToolPolicy;
import io.github.gratycodes.kafkaops.redact.Redactor;
import io.github.gratycodes.kafkaops.replay.ApprovalTokenService;
import io.github.gratycodes.kafkaops.telemetry.ToolTracer;
import io.github.gratycodes.kafkaops.tools.KafkaOpsService;
import io.github.gratycodes.kafkaops.tools.KafkaOpsTools;
import io.github.gratycodes.kafkaops.tools.ToolRunner;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.util.List;

@Configuration(proxyBeanMethods = false)
public class KafkaOpsConfiguration {

    private static final Logger log = LoggerFactory.getLogger(KafkaOpsConfiguration.class);

    @Bean
    @ConditionalOnMissingBean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    ToolPolicy toolPolicy(KafkaOpsProperties props) {
        List<Scope> scopes = props.getScopes().stream().map(Scope::fromId).toList();
        ToolPolicy policy = new ToolPolicy(scopes, props.getTopicAllowlist(), props.getDlqTopicPatterns());
        if (props.getTopicAllowlist().isEmpty()) {
            log.warn("kafka-ops.topic-allowlist is empty: every topic is denied until you add patterns.");
        }
        if (policy.has(Scope.DLQ_REPLAY)) {
            log.warn("Write scope dlq:replay is ENABLED for principal '{}'.", props.getPrincipal());
        }
        return policy;
    }

    /** Private mapper so we never replace the application's own JsonMapper bean. */
    private static final JsonMapper JSON = JsonMapper.builder().build();

    @Bean
    Redactor redactor(KafkaOpsProperties props) {
        var r = props.getRedaction();
        return new Redactor(r.getHeaderPatterns(), r.getFieldPatterns(), r.getMaxPayloadChars(), JSON);
    }

    @Bean
    ApprovalTokenService approvalTokenService(KafkaOpsProperties props, Clock clock) {
        return new ApprovalTokenService(clock, props.getReplay().getTokenTtl(), props.getReplay().getMaxPendingApprovals());
    }

    @Bean(destroyMethod = "close")
    KafkaGateway kafkaGateway(KafkaOpsProperties props) {
        return new KafkaGateway(props.getBootstrapServers(), props.getClient(), props.getRequestTimeout());
    }

    @Bean
    @ConditionalOnMissingBean
    AuditSink auditSink() {
        return new JsonLogAuditSink(JSON);
    }

    /** Uses an OpenTelemetry bean if the app has one, else the global instance (no-op unless the Java agent is attached). */
    @Bean
    ToolTracer toolTracer(ObjectProvider<OpenTelemetry> openTelemetry) {
        return new ToolTracer(openTelemetry.getIfAvailable(GlobalOpenTelemetry::get));
    }

    @Bean
    ToolRunner toolRunner(ToolPolicy policy, AuditSink audit, ToolTracer tracer, Redactor redactor,
                          Clock clock, KafkaOpsProperties props) {
        return new ToolRunner(policy, audit, tracer, redactor, JSON, clock, props.isCaptureToolArguments());
    }

    @Bean
    KafkaOpsService kafkaOpsService(KafkaGateway gateway, ToolPolicy policy, Redactor redactor,
                                    ApprovalTokenService approvals, KafkaOpsProperties props, Clock clock) {
        return new KafkaOpsService(gateway, policy, redactor, approvals, props, clock);
    }

    @Bean
    KafkaOpsTools kafkaOpsTools(KafkaOpsService service, ToolRunner runner, KafkaOpsProperties props) {
        return new KafkaOpsTools(service, runner, props.getPrincipal());
    }
}
