package io.github.gratycodes.kafkaops.telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;

/**
 * Creates {@code execute_tool {name}} spans following the OpenTelemetry GenAI semantic conventions
 * (span kind INTERNAL, {@code gen_ai.operation.name=execute_tool}, {@code gen_ai.tool.*}, {@code error.type}).
 *
 * <p>Uses whatever {@link OpenTelemetry} instance it is given. With no SDK or Java agent present that is the
 * no-op implementation, so tracing costs nothing until an exporter is configured.
 */
public class ToolTracer {

    public static final AttributeKey<String> OPERATION_NAME = AttributeKey.stringKey("gen_ai.operation.name");
    public static final AttributeKey<String> TOOL_NAME = AttributeKey.stringKey("gen_ai.tool.name");
    public static final AttributeKey<String> TOOL_TYPE = AttributeKey.stringKey("gen_ai.tool.type");
    public static final AttributeKey<String> TOOL_DESCRIPTION = AttributeKey.stringKey("gen_ai.tool.description");
    public static final AttributeKey<String> TOOL_CALL_ARGUMENTS = AttributeKey.stringKey("gen_ai.tool.call.arguments");
    public static final AttributeKey<String> ERROR_TYPE = AttributeKey.stringKey("error.type");
    public static final AttributeKey<String> MCP_METHOD = AttributeKey.stringKey("mcp.method.name");
    public static final AttributeKey<String> MCP_SESSION = AttributeKey.stringKey("mcp.session.id");
    public static final AttributeKey<String> OUTCOME = AttributeKey.stringKey("kafka_ops.outcome");

    private final Tracer tracer;

    public ToolTracer(OpenTelemetry openTelemetry) {
        this.tracer = openTelemetry.getTracer("io.github.gratycodes.kafka-ops-mcp");
    }

    public Span start(String toolName, String description, String sessionId) {
        var builder = tracer.spanBuilder("execute_tool " + toolName)
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute(OPERATION_NAME, "execute_tool")
                .setAttribute(TOOL_NAME, toolName)
                .setAttribute(TOOL_TYPE, "function")
                .setAttribute(MCP_METHOD, "tools/call");
        if (description != null) {
            builder.setAttribute(TOOL_DESCRIPTION, description);
        }
        if (sessionId != null) {
            builder.setAttribute(MCP_SESSION, sessionId);
        }
        return builder.startSpan();
    }

    public static void fail(Span span, String errorType, String outcome, String message) {
        span.setAttribute(ERROR_TYPE, errorType);
        span.setAttribute(OUTCOME, outcome);
        span.setStatus(StatusCode.ERROR, message);
    }
}
