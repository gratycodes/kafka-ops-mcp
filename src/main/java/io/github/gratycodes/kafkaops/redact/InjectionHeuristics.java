package io.github.gratycodes.kafkaops.redact;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Flags message content that reads like instructions aimed at an AI agent.
 *
 * <p>This does not make anything safe on its own. The server never executes message content; this only
 * adds a hint to the tool result so the calling agent (and the human reading the transcript) can see that
 * a record looks like a prompt-injection attempt. False negatives are expected.
 */
public final class InjectionHeuristics {

    private static final Map<String, Pattern> RULES = new LinkedHashMap<>();

    static {
        RULES.put("ignore_previous_instructions",
                Pattern.compile("(?i)\\b(ignore|disregard|forget)\\b.{0,30}\\b(previous|prior|above|all|earlier)\\b.{0,30}\\b(instructions?|rules?|prompts?)"));
        RULES.put("role_override",
                Pattern.compile("(?i)(\\byou are now\\b|\\bact as\\b|\\bnew instructions\\b|^\\s*system\\s*:|\\[\\s*system\\s*\\]|<\\s*/?\\s*system\\s*>)",
                        Pattern.MULTILINE));
        RULES.put("tool_invocation_request",
                Pattern.compile("(?i)\\b(call|invoke|run|execute|use)\\b.{0,20}\\b(tool|function|replay_dlq|replay_dlq_dry_run|browse_dlq)\\b"));
        RULES.put("mentions_approval_token", Pattern.compile("(?i)approval[_ ]?token|\\bkopr_"));
        RULES.put("exfiltration_request",
                Pattern.compile("(?i)\\b(send|post|upload|exfiltrate|email)\\b.{0,40}\\b(credentials?|secrets?|tokens?|passwords?|api keys?)\\b"));
    }

    private InjectionHeuristics() {
    }

    public static List<String> scan(String... texts) {
        List<String> hits = new ArrayList<>();
        for (Map.Entry<String, Pattern> rule : RULES.entrySet()) {
            for (String t : texts) {
                if (t != null && rule.getValue().matcher(t).find()) {
                    hits.add(rule.getKey());
                    break;
                }
            }
        }
        return hits;
    }
}
