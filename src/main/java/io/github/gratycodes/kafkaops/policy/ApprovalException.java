package io.github.gratycodes.kafkaops.policy;

/** Approval token problems: unknown, expired, already used, or issued to someone else. */
public class ApprovalException extends PolicyViolationException {

    public ApprovalException(String reason, String message) {
        super(reason, message);
    }
}
