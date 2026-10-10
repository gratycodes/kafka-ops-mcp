package io.github.gratycodes.kafkaops.tools;

/** Tool results describe themselves in one line for the audit log, without copying data. */
public interface Summarized {

    String auditSummary();
}
