package io.github.gratycodes.kafkaops.tools;

/** Who is calling: the configured principal plus what the MCP client said about itself. */
public record Caller(String principal, String client, String sessionId) {
}
