package io.github.gratycodes.kafkaops.config;

import io.github.gratycodes.kafkaops.redact.Redactor;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Everything under {@code kafka-ops.*}. Defaults are the safe ones: read-only, no topics allowed. */
@ConfigurationProperties("kafka-ops")
public class KafkaOpsProperties {

    /** Kafka bootstrap servers. */
    private String bootstrapServers = "localhost:9092";

    /** Extra Kafka client properties (security.protocol, sasl.*, ssl.*). Passed to admin, consumer and producer. */
    private Map<String, String> client = new LinkedHashMap<>();

    /** Name recorded as "who" in the audit log and bound to approval tokens. */
    private String principal = "local-operator";

    /** Granted scopes. dlq:replay is the only write scope and is off unless listed here. */
    private List<String> scopes = new ArrayList<>(List.of("topics:read", "groups:read", "dlq:read"));

    /** Glob patterns of topics the server may touch. Empty means none. */
    private List<String> topicAllowlist = new ArrayList<>();

    /** Glob patterns identifying dead-letter topics. DLQ tools refuse anything else. */
    private List<String> dlqTopicPatterns = new ArrayList<>(List.of("*.DLT", "*-dlt", "*.dlq", "*-dlq", "*.DLQ"));

    /** Timeout for Kafka admin and fetch calls. */
    private Duration requestTimeout = Duration.ofSeconds(10);

    /** Record redacted tool arguments on execute_tool spans (gen_ai.tool.call.arguments). Opt-in. */
    private boolean captureToolArguments = false;

    private final Browse browse = new Browse();
    private final Replay replay = new Replay();
    private final Redaction redaction = new Redaction();
    private final Http http = new Http();

    public static class Browse {
        /** Upper bound on records returned by one browse_dlq call. */
        private int maxMessages = 50;

        public int getMaxMessages() {
            return maxMessages;
        }

        public void setMaxMessages(int maxMessages) {
            this.maxMessages = maxMessages;
        }
    }

    public static class Replay {
        /** How long an approval token from a dry run stays valid. */
        private Duration tokenTtl = Duration.ofMinutes(5);
        /** Upper bound on records in one replay plan. */
        private int maxMessages = 100;
        /** Headers checked, in order, for the topic a dead-lettered record originally came from. */
        private List<String> originalTopicHeaders = new ArrayList<>(
                List.of("kafka_dlt-original-topic", "__connect.errors.topic", "dlq-original-topic", "x-original-topic"));
        /** Header prefixes stripped when replaying (error details added by the DLQ writer). */
        private List<String> stripHeaderPrefixes = new ArrayList<>(List.of("kafka_dlt-", "dlq-", "__connect.errors."));
        /** Maximum unused approval tokens held at once. */
        private int maxPendingApprovals = 20;

        public Duration getTokenTtl() {
            return tokenTtl;
        }

        public void setTokenTtl(Duration tokenTtl) {
            this.tokenTtl = tokenTtl;
        }

        public int getMaxMessages() {
            return maxMessages;
        }

        public void setMaxMessages(int maxMessages) {
            this.maxMessages = maxMessages;
        }

        public List<String> getOriginalTopicHeaders() {
            return originalTopicHeaders;
        }

        public void setOriginalTopicHeaders(List<String> originalTopicHeaders) {
            this.originalTopicHeaders = originalTopicHeaders;
        }

        public List<String> getStripHeaderPrefixes() {
            return stripHeaderPrefixes;
        }

        public void setStripHeaderPrefixes(List<String> stripHeaderPrefixes) {
            this.stripHeaderPrefixes = stripHeaderPrefixes;
        }

        public int getMaxPendingApprovals() {
            return maxPendingApprovals;
        }

        public void setMaxPendingApprovals(int maxPendingApprovals) {
            this.maxPendingApprovals = maxPendingApprovals;
        }
    }

    public static class Redaction {
        /** Header names (globs, case-insensitive) whose values are never shown. */
        private List<String> headerPatterns = new ArrayList<>(Redactor.DEFAULT_HEADER_PATTERNS);
        /** JSON field names (globs, matched after lower-casing and removing _ and -) whose values are never shown. */
        private List<String> fieldPatterns = new ArrayList<>(Redactor.DEFAULT_FIELD_PATTERNS);
        /** Payloads longer than this are truncated after redaction. */
        private int maxPayloadChars = 4000;

        public List<String> getHeaderPatterns() {
            return headerPatterns;
        }

        public void setHeaderPatterns(List<String> headerPatterns) {
            this.headerPatterns = headerPatterns;
        }

        public List<String> getFieldPatterns() {
            return fieldPatterns;
        }

        public void setFieldPatterns(List<String> fieldPatterns) {
            this.fieldPatterns = fieldPatterns;
        }

        public int getMaxPayloadChars() {
            return maxPayloadChars;
        }

        public void setMaxPayloadChars(int maxPayloadChars) {
            this.maxPayloadChars = maxPayloadChars;
        }
    }

    public static class Http {
        /**
         * Hex SHA-256 of the bearer token HTTP clients must send. When empty, the HTTP endpoint is
         * unauthenticated and the server should only listen on loopback (the default).
         */
        private String bearerTokenSha256 = "";

        public String getBearerTokenSha256() {
            return bearerTokenSha256;
        }

        public void setBearerTokenSha256(String bearerTokenSha256) {
            this.bearerTokenSha256 = bearerTokenSha256;
        }
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public Map<String, String> getClient() {
        return client;
    }

    public void setClient(Map<String, String> client) {
        this.client = client;
    }

    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    public List<String> getScopes() {
        return scopes;
    }

    public void setScopes(List<String> scopes) {
        this.scopes = scopes;
    }

    public List<String> getTopicAllowlist() {
        return topicAllowlist;
    }

    public void setTopicAllowlist(List<String> topicAllowlist) {
        this.topicAllowlist = topicAllowlist;
    }

    public List<String> getDlqTopicPatterns() {
        return dlqTopicPatterns;
    }

    public void setDlqTopicPatterns(List<String> dlqTopicPatterns) {
        this.dlqTopicPatterns = dlqTopicPatterns;
    }

    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    public void setRequestTimeout(Duration requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    public boolean isCaptureToolArguments() {
        return captureToolArguments;
    }

    public void setCaptureToolArguments(boolean captureToolArguments) {
        this.captureToolArguments = captureToolArguments;
    }

    public Browse getBrowse() {
        return browse;
    }

    public Replay getReplay() {
        return replay;
    }

    public Redaction getRedaction() {
        return redaction;
    }

    public Http getHttp() {
        return http;
    }
}
