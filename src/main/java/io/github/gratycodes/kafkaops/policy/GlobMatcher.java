package io.github.gratycodes.kafkaops.policy;

import java.util.List;
import java.util.regex.Pattern;

/** Tiny glob matcher: {@code *} matches any run of characters, {@code ?} one character. */
public final class GlobMatcher {

    private final List<Pattern> patterns;

    public GlobMatcher(List<String> globs, boolean caseInsensitive) {
        this.patterns = globs.stream()
                .filter(g -> g != null && !g.isBlank())
                .map(g -> toRegex(g.trim(), caseInsensitive))
                .toList();
    }

    public boolean matches(String value) {
        if (value == null) {
            return false;
        }
        for (Pattern p : patterns) {
            if (p.matcher(value).matches()) {
                return true;
            }
        }
        return false;
    }

    public boolean isEmpty() {
        return patterns.isEmpty();
    }

    static Pattern toRegex(String glob, boolean caseInsensitive) {
        StringBuilder sb = new StringBuilder();
        StringBuilder literal = new StringBuilder();
        for (char c : glob.toCharArray()) {
            if (c == '*' || c == '?') {
                if (!literal.isEmpty()) {
                    sb.append(Pattern.quote(literal.toString()));
                    literal.setLength(0);
                }
                sb.append(c == '*' ? ".*" : ".");
            } else {
                literal.append(c);
            }
        }
        if (!literal.isEmpty()) {
            sb.append(Pattern.quote(literal.toString()));
        }
        return Pattern.compile(sb.toString(), caseInsensitive ? Pattern.CASE_INSENSITIVE : 0);
    }
}
