package io.github.gratycodes.kafkaops.it;

import io.github.gratycodes.kafkaops.audit.AuditEvent;
import io.github.gratycodes.kafkaops.audit.AuditSink;
import io.github.gratycodes.kafkaops.support.KafkaFixture;
import io.github.gratycodes.kafkaops.support.RecordingAuditSink;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.test.context.EmbeddedKafka;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Boots the real application and drives it with the official MCP Java client over Streamable HTTP,
 * the way Claude Desktop, Cursor or an agent framework would.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "kafka-ops.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "kafka-ops.principal=e2e-agent",
        "kafka-ops.topic-allowlist=orders*",
        "kafka-ops.scopes=topics:read,groups:read,dlq:read,dlq:replay",
        // sha256("e2e-secret")
        "kafka-ops.http.bearer-token-sha256=88458bad1f357fbcc409825b5d9a83214f501304132f08bbcf6b0cd24b8629f9",
})
@EmbeddedKafka(partitions = 1)
class McpServerEndToEndTest {

    static final String TOKEN = "e2e-secret";

    @TestConfiguration
    static class Audit {
        @Bean
        @Primary
        RecordingAuditSink recordingAuditSink() {
            return new RecordingAuditSink();
        }
    }

    @Value("${local.server.port}")
    int port;

    @Value("${spring.embedded.kafka.brokers}")
    String brokers;

    @Autowired
    RecordingAuditSink audit;

    final JsonMapper json = JsonMapper.builder().build();
    KafkaFixture kafka;
    McpSyncClient client;

    @BeforeEach
    void setUp() {
        kafka = new KafkaFixture(brokers);
        client = client(TOKEN);
        client.initialize();
    }

    @AfterEach
    void tearDown() {
        client.closeGracefully();
        kafka.close();
    }

    private McpSyncClient client(String bearer) {
        HttpRequest.Builder req = HttpRequest.newBuilder();
        if (bearer != null) {
            req.header("Authorization", "Bearer " + bearer);
        }
        var transport = HttpClientStreamableHttpTransport.builder("http://localhost:" + port)
                .endpoint("/mcp")
                .requestBuilder(req)
                .build();
        return McpClient.sync(transport)
                .clientInfo(new McpSchema.Implementation("e2e-test-client", "1.0.0"))
                .requestTimeout(Duration.ofSeconds(30))
                .build();
    }

    private JsonNode call(String tool, Map<String, Object> args) {
        McpSchema.CallToolResult r = client.callTool(new McpSchema.CallToolRequest(tool, args));
        assertThat(r.isError()).as(tool + " error: " + r.content()).isNotEqualTo(Boolean.TRUE);
        return json.readTree(((McpSchema.TextContent) r.content().getFirst()).text());
    }

    @Test
    void advertisesSixToolsWithHonestHints() {
        List<McpSchema.Tool> tools = client.listTools().tools();
        assertThat(tools).extracting(McpSchema.Tool::name).containsExactlyInAnyOrder(
                "list_topics", "describe_topic", "consumer_group_lag", "browse_dlq", "replay_dlq_dry_run", "replay_dlq");
        for (McpSchema.Tool t : tools) {
            boolean write = t.name().equals("replay_dlq");
            assertThat(t.annotations().readOnlyHint()).as(t.name()).isEqualTo(!write);
            assertThat(t.annotations().destructiveHint()).as(t.name()).isEqualTo(write);
        }
        McpSchema.Tool replay = tools.stream().filter(t -> t.name().equals("replay_dlq")).findFirst().orElseThrow();
        assertThat(String.valueOf(replay.inputSchema().get("required"))).isEqualTo("[approvalToken]");
    }

    @Test
    void fullDlqWorkflowOverMcp() {
        String orders = KafkaFixture.unique("orders.");
        String dlq = orders + ".DLT";
        kafka.createTopics(orders, dlq);
        kafka.send(dlq, "k1", "{\"orderId\":\"o-1\",\"email\":\"buyer@example.com\"}",
                "kafka_dlt-original-topic", orders, "x-api-key", "sk_test_123");

        JsonNode page = call("browse_dlq", Map.of("topic", dlq));
        assertThat(page.get("contentTrust").stringValue()).isEqualTo("untrusted");
        assertThat(page.toString()).doesNotContain("buyer@example.com", "sk_test_123");

        JsonNode plan = call("replay_dlq_dry_run", Map.of("dlqTopic", dlq, "partition", 0, "fromOffset", 0, "toOffset", 0));
        String token = plan.get("approvalToken").stringValue();
        assertThat(token).startsWith("kopr_");

        JsonNode done = call("replay_dlq", Map.of("approvalToken", token));
        assertThat(done.get("replayed").intValue()).isEqualTo(1);
        assertThat(kafka.endOffset(orders)).isEqualTo(1);

        AuditEvent last = audit.last();
        assertThat(last.tool()).isEqualTo("replay_dlq");
        assertThat(last.principal()).isEqualTo("e2e-agent");
        assertThat(last.client()).isEqualTo("e2e-test-client/1.0.0");
        assertThat(last.args().get("approvalToken")).isEqualTo("[REDACTED:field]");
    }

    @Test
    void deniedCallsComeBackAsToolErrorsNotProtocolErrors() {
        McpSchema.CallToolResult r = client.callTool(new McpSchema.CallToolRequest("describe_topic",
                Map.of("topic", "__consumer_offsets")));
        assertThat(r.isError()).isTrue();
        assertThat(((McpSchema.TextContent) r.content().getFirst()).text()).contains("allowlist");

        McpSchema.CallToolResult bad = client.callTool(new McpSchema.CallToolRequest("replay_dlq",
                Map.of("approvalToken", "just-do-it")));
        assertThat(bad.isError()).isTrue();
    }

    @Test
    void httpClientsWithoutTheBearerTokenAreRejected() {
        McpSyncClient anonymous = client(null);
        assertThatThrownBy(anonymous::initialize).isInstanceOf(RuntimeException.class);
        McpSyncClient wrong = client("guess");
        assertThatThrownBy(wrong::initialize).isInstanceOf(RuntimeException.class);
    }
}
