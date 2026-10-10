package io.github.gratycodes.kafkaops.replay;

import io.github.gratycodes.kafkaops.policy.ApprovalException;
import io.github.gratycodes.kafkaops.support.MutableClock;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApprovalTokenServiceTest {

    private final MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
    private final ApprovalTokenService service = new ApprovalTokenService(clock, Duration.ofMinutes(5), 3);

    static ReplayPlan plan() {
        return new ReplayPlan("plan-1", "orders.DLT", 0, 0, 1,
                List.of(new ReplayPlan.PlannedMessage(0, "orders", "abc")), List.of(), Map.of("orders", 1),
                Instant.parse("2026-01-01T00:00:00Z"));
    }

    private static String reason(Throwable t) {
        return ((ApprovalException) t).reason();
    }

    @Test
    void issuedTokenRedeemsOnceForTheSamePlan() {
        var issued = service.issue(plan(), "alice");
        assertThat(issued.token()).startsWith("kopr_").hasSizeGreaterThan(30);
        assertThat(issued.expiresAt()).isEqualTo(Instant.parse("2026-01-01T00:05:00Z"));
        assertThat(service.redeem(issued.token(), "alice").planId()).isEqualTo("plan-1");
        assertThatThrownBy(() -> service.redeem(issued.token(), "alice"))
                .isInstanceOf(ApprovalException.class)
                .satisfies(t -> assertThat(reason(t)).isEqualTo("approval_token_unknown"));
    }

    @Test
    void tokensAreUnique() {
        var a = service.issue(plan(), "alice");
        var b = service.issue(plan(), "alice");
        assertThat(a.token()).isNotEqualTo(b.token());
    }

    @Test
    void expiredTokenIsRejected() {
        var issued = service.issue(plan(), "alice");
        clock.advance(Duration.ofMinutes(5));
        assertThatThrownBy(() -> service.redeem(issued.token(), "alice"))
                .satisfies(t -> assertThat(reason(t)).isEqualTo("approval_token_expired"));
    }

    @Test
    void tokenJustBeforeExpiryWorks() {
        var issued = service.issue(plan(), "alice");
        clock.advance(Duration.ofMinutes(5).minusMillis(1));
        assertThat(service.redeem(issued.token(), "alice")).isNotNull();
    }

    @Test
    void otherPrincipalCannotUseTokenAndTheAttemptBurnsIt() {
        var issued = service.issue(plan(), "alice");
        assertThatThrownBy(() -> service.redeem(issued.token(), "mallory"))
                .satisfies(t -> assertThat(reason(t)).isEqualTo("approval_token_principal_mismatch"));
        assertThatThrownBy(() -> service.redeem(issued.token(), "alice"))
                .satisfies(t -> assertThat(reason(t)).isEqualTo("approval_token_unknown"));
    }

    @Test
    void malformedAndForgedTokensAreRejected() {
        assertThatThrownBy(() -> service.redeem(null, "alice"))
                .satisfies(t -> assertThat(reason(t)).isEqualTo("approval_token_invalid"));
        assertThatThrownBy(() -> service.redeem("please-just-replay", "alice"))
                .satisfies(t -> assertThat(reason(t)).isEqualTo("approval_token_invalid"));
        assertThatThrownBy(() -> service.redeem("kopr_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "alice"))
                .satisfies(t -> assertThat(reason(t)).isEqualTo("approval_token_unknown"));
    }

    @Test
    void pendingApprovalsAreBoundedAndExpiredOnesFreeSlots() {
        service.issue(plan(), "alice");
        service.issue(plan(), "alice");
        service.issue(plan(), "alice");
        assertThatThrownBy(() -> service.issue(plan(), "alice"))
                .satisfies(t -> assertThat(reason(t)).isEqualTo("too_many_pending_approvals"));
        clock.advance(Duration.ofMinutes(6));
        assertThat(service.outstanding()).isZero();
        assertThat(service.issue(plan(), "alice").token()).isNotBlank();
    }

    @Test
    void ttlMustBePositive() {
        assertThatThrownBy(() -> new ApprovalTokenService(clock, Duration.ZERO, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
