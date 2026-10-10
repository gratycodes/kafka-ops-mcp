package io.github.gratycodes.kafkaops.replay;

import io.github.gratycodes.kafkaops.policy.ApprovalException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Issues short-lived, single-use approval tokens for replay plans.
 *
 * <p>Only a SHA-256 of each token is kept in memory, so a heap dump or debug log of this map
 * does not leak usable tokens. Tokens are bound to the principal that ran the dry run.
 * Tokens do not survive a restart; that is deliberate.
 */
public class ApprovalTokenService {

    public static final String TOKEN_PREFIX = "kopr_";

    private final Clock clock;
    private final Duration ttl;
    private final int maxOutstanding;
    private final SecureRandom random = new SecureRandom();
    private final Map<String, Entry> byHash = new ConcurrentHashMap<>();

    public ApprovalTokenService(Clock clock, Duration ttl, int maxOutstanding) {
        if (ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("ttl must be positive");
        }
        this.clock = clock;
        this.ttl = ttl;
        this.maxOutstanding = maxOutstanding;
    }

    public record IssuedToken(String token, Instant expiresAt) {
    }

    private record Entry(ReplayPlan plan, String principal, Instant expiresAt) {
    }

    public synchronized IssuedToken issue(ReplayPlan plan, String principal) {
        evictExpired();
        if (byHash.size() >= maxOutstanding) {
            throw new ApprovalException("too_many_pending_approvals",
                    "There are already " + maxOutstanding + " unused approval tokens. Wait for them to expire.");
        }
        byte[] bytes = new byte[24];
        random.nextBytes(bytes);
        String token = TOKEN_PREFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        Instant expiresAt = clock.instant().plus(ttl);
        byHash.put(sha256(token), new Entry(plan, principal, expiresAt));
        return new IssuedToken(token, expiresAt);
    }

    /**
     * Consumes a token and returns its plan. A token can be redeemed once; a failed redemption
     * because of principal mismatch also burns the token so it cannot be probed.
     */
    public ReplayPlan redeem(String token, String principal) {
        if (token == null || !token.startsWith(TOKEN_PREFIX)) {
            throw new ApprovalException("approval_token_invalid",
                    "A valid approval_token from replay_dlq_dry_run is required.");
        }
        Entry entry = byHash.remove(sha256(token));
        if (entry == null) {
            throw new ApprovalException("approval_token_unknown",
                    "Approval token is unknown or was already used. Run replay_dlq_dry_run again.");
        }
        if (!clock.instant().isBefore(entry.expiresAt())) {
            throw new ApprovalException("approval_token_expired",
                    "Approval token expired at " + entry.expiresAt() + ". Run replay_dlq_dry_run again.");
        }
        if (!entry.principal().equals(principal)) {
            throw new ApprovalException("approval_token_principal_mismatch",
                    "Approval token was issued to a different principal.");
        }
        return entry.plan();
    }

    public int outstanding() {
        evictExpired();
        return byHash.size();
    }

    public Duration ttl() {
        return ttl;
    }

    private void evictExpired() {
        Instant now = clock.instant();
        byHash.values().removeIf(e -> !now.isBefore(e.expiresAt()));
    }

    static String sha256(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(md.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
