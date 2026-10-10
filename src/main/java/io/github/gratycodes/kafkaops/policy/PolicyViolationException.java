package io.github.gratycodes.kafkaops.policy;

/** Thrown when a tool call is refused by configuration (scope, allowlist, approval). */
public class PolicyViolationException extends RuntimeException {

    private final String reason;

    public PolicyViolationException(String reason, String message) {
        super(message);
        this.reason = reason;
    }

    /** Short machine-readable reason, e.g. {@code scope_missing}. Used for audit and span error.type. */
    public String reason() {
        return reason;
    }
}
