package io.github.gratycodes.kafkaops.redact;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RedactorTest {

    private final JsonMapper json = JsonMapper.builder().build();
    private final Redactor redactor = Redactor.withDefaults(json);

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void sensitiveHeadersAreDroppedWhatever_theirValue() {
        assertThat(redactor.header("Authorization", b("Bearer abc")).text()).isEqualTo("[REDACTED:header]");
        assertThat(redactor.header("x-api-key", b("k")).text()).isEqualTo("[REDACTED:header]");
        assertThat(redactor.header("X-Refresh-Token", b("k")).text()).isEqualTo("[REDACTED:header]");
        assertThat(redactor.header("cookie", b("sid=1")).text()).isEqualTo("[REDACTED:header]");
    }

    @Test
    void ordinaryHeadersAreKeptButStillScannedForValues() {
        assertThat(redactor.header("kafka_dlt-original-topic", b("orders.created")).text()).isEqualTo("orders.created");
        assertThat(redactor.header("kafka_dlt-exception-message", b("bad user jane.doe@example.com")).text())
                .isEqualTo("bad user [REDACTED:email]");
    }

    @Test
    void jsonFieldsAreRedactedByNormalisedName() throws Exception {
        String payload = """
                {"orderId":"ord-1001","amount":42.5,"cardNumber":"4111 1111 1111 1111","card_number":"x",
                 "customer":{"email":"a@b.co","Phone-Number":"+1 555 0100","name":"Test User"},
                 "auth":{"accessToken":"t","password":"p"},"notes":null}""";
        Redactor.Redacted r = redactor.payload(b(payload));
        JsonNode n = json.readTree(r.text());
        assertThat(n.get("orderId").stringValue()).isEqualTo("ord-1001");
        assertThat(n.get("amount").doubleValue()).isEqualTo(42.5);
        assertThat(n.get("cardNumber").stringValue()).isEqualTo("[REDACTED:field]");
        assertThat(n.get("card_number").stringValue()).isEqualTo("[REDACTED:field]");
        assertThat(n.at("/customer/email").stringValue()).isEqualTo("[REDACTED:field]");
        assertThat(n.at("/customer/Phone-Number").stringValue()).isEqualTo("[REDACTED:field]");
        assertThat(n.at("/customer/name").stringValue()).isEqualTo("Test User");
        assertThat(n.at("/auth/accessToken").stringValue()).isEqualTo("[REDACTED:field]");
        assertThat(n.at("/auth/password").stringValue()).isEqualTo("[REDACTED:field]");
        assertThat(n.get("notes").isNull()).isTrue();
        assertThat(r.redactions()).isEqualTo(6);
    }

    @Test
    void secretsInsideArraysAndFreeTextFieldsAreFound() throws Exception {
        String payload = """
                {"events":[{"msg":"retry with Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2lnbmF0dXJlX3Rlc3Q"},
                           {"msg":"customer wrote 5555-5555-5555-4444 in the comment"}]}""";
        JsonNode n = json.readTree(redactor.payload(b(payload)).text());
        assertThat(n.at("/events/0/msg").stringValue()).doesNotContain("eyJ").contains("[REDACTED:");
        assertThat(n.at("/events/1/msg").stringValue()).isEqualTo("customer wrote [REDACTED:card_number] in the comment");
    }

    @Test
    void freeTextDetectors() {
        String text = "user=jane@example.org key=AKIAABCDEFGHIJKLMNOP password=hunter2 token: 'abc123' "
                + "order 1234567890123 card 4111111111111111";
        String out = redactor.text(text).text();
        assertThat(out)
                .contains("[REDACTED:email]")
                .contains("[REDACTED:aws_key]")
                .contains("password=[REDACTED:secret]")
                .contains("token: [REDACTED:secret]")
                .contains("[REDACTED:card_number]")
                .doesNotContain("hunter2", "AKIAABCDEFGHIJKLMNOP", "4111111111111111", "abc123");
        // 13 digits that fail Luhn: an order id, not a card. Keep it.
        assertThat(out).contains("order 1234567890123");
    }

    @Test
    void privateKeysAndApprovalTokensAreRemoved() {
        String text = "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA\n-----END RSA PRIVATE KEY----- and "
                + "kopr_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        String out = redactor.text(text).text();
        assertThat(out).isEqualTo("[REDACTED:private_key] and [REDACTED:approval_token]");
    }

    @Test
    void luhn() {
        assertThat(Redactor.luhn("4111111111111111")).isTrue();
        assertThat(Redactor.luhn("4111111111111112")).isFalse();
    }

    @Test
    void binaryPayloadsAreNotDecoded() {
        byte[] bin = {0x00, 0x01, (byte) 0xff, (byte) 0xfe, 0x10};
        assertThat(redactor.payload(bin).text()).isEqualTo("[binary 5 bytes, not shown]");
        assertThat(redactor.payload(null).text()).isNull();
    }

    @Test
    void invalidJsonFallsBackToTextScanning() {
        assertThat(redactor.payload(b("{not json, email a@b.io")).text()).isEqualTo("{not json, email [REDACTED:email]");
    }

    @Test
    void longPayloadsAreTruncatedAfterRedaction() {
        Redactor small = new Redactor(Redactor.DEFAULT_HEADER_PATTERNS, Redactor.DEFAULT_FIELD_PATTERNS, 20, json);
        Redactor.Redacted r = small.payload(b("x".repeat(100)));
        assertThat(r.text()).hasSize(20);
        assertThat(r.truncated()).isTrue();
    }

    @Test
    void auditArgumentsAreRedacted() {
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("topic", "orders.DLT");
        args.put("approvalToken", "kopr_secretsecretsecretsecret");
        args.put("contains", "jane@example.com");
        args.put("limit", 5);
        args.put("partition", null);
        Map<String, Object> out = redactor.arguments(args);
        assertThat(out).containsEntry("topic", "orders.DLT")
                .containsEntry("approvalToken", "[REDACTED:field]")
                .containsEntry("contains", "[REDACTED:email]")
                .containsEntry("limit", 5)
                .doesNotContainKey("partition");
    }

    @Test
    void customPatternsReplaceDefaults() {
        Redactor custom = new Redactor(List.of("x-tenant"), List.of("merchant*"), 1000, json);
        assertThat(custom.header("x-tenant", b("t1")).text()).isEqualTo("[REDACTED:header]");
        assertThat(custom.payload(b("{\"merchantId\":\"m1\",\"password\":\"p\"}")).text())
                .isEqualTo("{\"merchantId\":\"[REDACTED:field]\",\"password\":\"p\"}");
    }
}
