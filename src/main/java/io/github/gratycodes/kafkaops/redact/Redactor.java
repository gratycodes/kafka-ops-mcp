package io.github.gratycodes.kafkaops.redact;

import io.github.gratycodes.kafkaops.policy.GlobMatcher;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;
import tools.jackson.databind.node.StringNode;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Removes secrets and personal data before anything leaves the server: header values,
 * JSON payload fields, free text, and the arguments written to the audit log.
 *
 * <p>Three layers, applied in order:
 * <ol>
 *   <li>Header names that look sensitive ({@code authorization}, {@code *token*}, ...) lose their value.</li>
 *   <li>JSON fields whose normalised name matches a pattern ({@code cardNumber}, {@code card_number} and
 *       {@code card-number} all normalise to {@code cardnumber}) lose their value.</li>
 *   <li>Every remaining string is scanned for values that look like secrets or PII regardless of where they
 *       sit: JWTs, bearer tokens, AWS keys, private keys, {@code password=...} pairs, emails, and card numbers
 *       that pass a Luhn check.</li>
 * </ol>
 *
 * <p>This is pattern matching, not a DLP product. It reduces accidental exposure; it does not prove absence.
 */
public final class Redactor {

    public static final List<String> DEFAULT_HEADER_PATTERNS = List.of(
            "authorization", "proxy-authorization", "cookie", "set-cookie", "*token*", "*secret*",
            "*password*", "*passwd*", "*credential*", "*api-key*", "*apikey*", "*api_key*", "*session*", "*signature*");

    public static final List<String> DEFAULT_FIELD_PATTERNS = List.of(
            "password", "passwd", "pwd", "*secret*", "*token*", "apikey", "*apikey", "authorization", "cookie",
            "ssn", "*socialsecurity*", "*cardnumber*", "pan", "cvv", "cvc", "cvv2", "expiry", "expirydate",
            "iban", "*accountnumber*", "routingnumber", "email", "*emailaddress*", "phone", "*phonenumber*", "mobile",
            "dob", "dateofbirth", "birthdate", "*privatekey*", "taxid", "nationalid", "passport*");

    private static final Pattern PRIVATE_KEY = Pattern.compile(
            "-----BEGIN [A-Z ]*PRIVATE KEY-----[\\s\\S]*?-----END [A-Z ]*PRIVATE KEY-----");
    private static final Pattern JWT = Pattern.compile("\\beyJ[A-Za-z0-9_-]{5,}\\.[A-Za-z0-9_-]{5,}\\.[A-Za-z0-9_-]{5,}");
    private static final Pattern BEARER = Pattern.compile("(?i)\\b(bearer|basic)\\s+[A-Za-z0-9._~+/=-]{8,}");
    private static final Pattern AWS_KEY = Pattern.compile("\\b(AKIA|ASIA)[0-9A-Z]{16}\\b");
    private static final Pattern APPROVAL_TOKEN = Pattern.compile("\\bkopr_[A-Za-z0-9_-]{20,}");
    private static final Pattern KEY_VALUE_SECRET = Pattern.compile(
            "(?i)\\b(password|passwd|pwd|secret|api[_-]?key|access[_-]?token|token)(\\s*[=:]\\s*)(\"[^\"]*\"|'[^']*'|[^\\s,;&\"'}]+)");
    private static final Pattern EMAIL = Pattern.compile("\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b");
    private static final Pattern CARD_CANDIDATE = Pattern.compile("\\b\\d(?:[ -]?\\d){12,18}\\b");

    private final GlobMatcher headerNames;
    private final GlobMatcher fieldNames;
    private final int maxPayloadChars;
    private final JsonMapper json;

    public Redactor(List<String> headerPatterns, List<String> fieldPatterns, int maxPayloadChars, JsonMapper json) {
        this.headerNames = new GlobMatcher(headerPatterns, true);
        this.fieldNames = new GlobMatcher(fieldPatterns.stream().map(Redactor::normalise).toList(), true);
        this.maxPayloadChars = maxPayloadChars;
        this.json = json;
    }

    public static Redactor withDefaults(JsonMapper json) {
        return new Redactor(DEFAULT_HEADER_PATTERNS, DEFAULT_FIELD_PATTERNS, 4_000, json);
    }

    /** Result of redacting one piece of text. */
    public record Redacted(String text, int redactions, boolean truncated) {
    }

    /** Redacts a header value, dropping it completely if the header name is sensitive. */
    public Redacted header(String name, byte[] value) {
        if (value == null) {
            return new Redacted(null, 0, false);
        }
        if (headerNames.matches(name)) {
            return new Redacted("[REDACTED:header]", 1, false);
        }
        String decoded = decode(value);
        if (decoded == null) {
            return new Redacted("[binary " + value.length + " bytes]", 0, false);
        }
        return text(decoded);
    }

    /** Redacts a record key or value. JSON is redacted structurally; anything else as free text. */
    public Redacted payload(byte[] bytes) {
        if (bytes == null) {
            return new Redacted(null, 0, false);
        }
        String decoded = decode(bytes);
        if (decoded == null) {
            return new Redacted("[binary " + bytes.length + " bytes, not shown]", 0, false);
        }
        Redacted r = looksLikeJson(decoded) ? jsonText(decoded) : text(decoded);
        return truncate(r);
    }

    /** Redacts free text with the value detectors only. */
    public Redacted text(String text) {
        if (text == null) {
            return new Redacted(null, 0, false);
        }
        int[] count = {0};
        String out = text;
        out = replace(out, PRIVATE_KEY, "[REDACTED:private_key]", count);
        out = replace(out, JWT, "[REDACTED:jwt]", count);
        out = replace(out, BEARER, "[REDACTED:credential]", count);
        out = replace(out, AWS_KEY, "[REDACTED:aws_key]", count);
        out = replace(out, APPROVAL_TOKEN, "[REDACTED:approval_token]", count);
        out = replaceKeyValue(out, count);
        out = replace(out, EMAIL, "[REDACTED:email]", count);
        out = replaceCards(out, count);
        return new Redacted(out, count[0], false);
    }

    /** Redacts tool arguments before they are written to the audit log. */
    public Map<String, Object> arguments(Map<String, Object> args) {
        Map<String, Object> out = new LinkedHashMap<>();
        args.forEach((k, v) -> {
            if (v == null) {
                return;
            }
            if (fieldNames.matches(normalise(k))) {
                out.put(k, "[REDACTED:field]");
            } else if (v instanceof String s) {
                out.put(k, text(s).text());
            } else {
                out.put(k, v);
            }
        });
        return out;
    }

    public boolean isSensitiveField(String name) {
        return fieldNames.matches(normalise(name));
    }

    private Redacted jsonText(String text) {
        JsonNode root;
        try {
            root = json.readTree(text);
        } catch (JacksonException e) {
            return text(text);
        }
        int[] count = {0};
        JsonNode cleaned = redactNode(root, count);
        return new Redacted(json.writeValueAsString(cleaned), count[0], false);
    }

    private JsonNode redactNode(JsonNode node, int[] count) {
        if (node instanceof ObjectNode obj) {
            for (Map.Entry<String, JsonNode> e : new ArrayList<>(obj.properties())) {
                if (fieldNames.matches(normalise(e.getKey())) && !e.getValue().isNull()) {
                    obj.put(e.getKey(), "[REDACTED:field]");
                    count[0]++;
                } else {
                    obj.set(e.getKey(), redactNode(e.getValue(), count));
                }
            }
            return obj;
        }
        if (node instanceof ArrayNode arr) {
            for (int i = 0; i < arr.size(); i++) {
                arr.set(i, redactNode(arr.get(i), count));
            }
            return arr;
        }
        if (node.isString()) {
            Redacted r = text(node.stringValue());
            count[0] += r.redactions();
            return r.redactions() == 0 ? node : StringNode.valueOf(r.text());
        }
        return node;
    }

    private Redacted truncate(Redacted r) {
        if (r.text() != null && r.text().length() > maxPayloadChars) {
            return new Redacted(r.text().substring(0, maxPayloadChars), r.redactions(), true);
        }
        return r;
    }

    private static String replace(String in, Pattern p, String replacement, int[] count) {
        Matcher m = p.matcher(in);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            count[0]++;
            m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private static String replaceKeyValue(String in, int[] count) {
        Matcher m = KEY_VALUE_SECRET.matcher(in);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            if (m.group(3).startsWith("[REDACTED")) {
                m.appendReplacement(sb, Matcher.quoteReplacement(m.group()));
                continue;
            }
            count[0]++;
            m.appendReplacement(sb, Matcher.quoteReplacement(m.group(1) + m.group(2) + "[REDACTED:secret]"));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private static String replaceCards(String in, int[] count) {
        Matcher m = CARD_CANDIDATE.matcher(in);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String digits = m.group().replaceAll("[ -]", "");
            if (digits.length() >= 13 && digits.length() <= 19 && luhn(digits)) {
                count[0]++;
                m.appendReplacement(sb, Matcher.quoteReplacement("[REDACTED:card_number]"));
            } else {
                m.appendReplacement(sb, Matcher.quoteReplacement(m.group()));
            }
        }
        m.appendTail(sb);
        return sb.toString();
    }

    static boolean luhn(String digits) {
        int sum = 0;
        boolean dbl = false;
        for (int i = digits.length() - 1; i >= 0; i--) {
            int d = digits.charAt(i) - '0';
            if (dbl) {
                d *= 2;
                if (d > 9) {
                    d -= 9;
                }
            }
            sum += d;
            dbl = !dbl;
        }
        return sum % 10 == 0;
    }

    static String normalise(String name) {
        return name.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9*?]", "");
    }

    private static boolean looksLikeJson(String s) {
        String t = s.stripLeading();
        return t.startsWith("{") || t.startsWith("[");
    }

    /** Strict UTF-8 decode; returns null for binary data or text with control characters. */
    static String decode(byte[] bytes) {
        try {
            String s = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                if (c < 0x20 && c != '\n' && c != '\r' && c != '\t') {
                    return null;
                }
            }
            return s;
        } catch (CharacterCodingException e) {
            return null;
        }
    }
}
