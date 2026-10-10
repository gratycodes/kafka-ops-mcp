package io.github.gratycodes.kafkaops.web;

import io.github.gratycodes.kafkaops.config.KafkaOpsProperties;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Locale;

/**
 * Optional bearer-token check for the HTTP transport. The expected token is configured as a SHA-256 hash so the
 * plaintext never sits in a config file. Comparison is constant-time.
 */
@Component
@ConditionalOnWebApplication
public class BearerTokenFilter extends OncePerRequestFilter {

    private final byte[] expectedHash;

    public BearerTokenFilter(KafkaOpsProperties props) {
        String hex = props.getHttp().getBearerTokenSha256();
        this.expectedHash = hex == null || hex.isBlank() ? null : HexFormat.of().parseHex(hex.trim().toLowerCase(Locale.ROOT));
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        if (expectedHash == null) {
            chain.doFilter(request, response);
            return;
        }
        String header = request.getHeader("Authorization");
        if (header != null && header.regionMatches(true, 0, "Bearer ", 0, 7)
                && MessageDigest.isEqual(expectedHash, sha256(header.substring(7).trim()))) {
            chain.doFilter(request, response);
            return;
        }
        response.setHeader("WWW-Authenticate", "Bearer");
        response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
    }

    static byte[] sha256(String s) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
