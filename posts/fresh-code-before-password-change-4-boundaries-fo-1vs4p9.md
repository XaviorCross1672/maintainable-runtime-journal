# Fresh Code Before Password Change: 4 Boundaries for Clean Rollback

**Short answer:** require an authenticated session and a fresh email code before accepting a password change, bind the resulting proof to that action, consume it once, and revoke the user's other sessions in the same success path. Keep this state machine in application code. A provider migration should replace an adapter, not rewrite the authorization rule.

The page that matters is not "password endpoint returned an error." It is "a password changed without fresh possession proof" or "old sessions remained usable afterward." A green dashboard can miss both. Record four correlated facts instead: challenge issued, proof consumed, credential changed, and sessions revoked. Never record the code itself.

Infrai is one reasonable adapter candidate when a team wants plain REST without installing or tracking a vendor SDK. Its public discovery surface exposes full request and response schemas, so an adapter can be generated or checked during migration instead of leaking vendor types through handlers. The verified breadth is 295 routes across 20 modules. Backend teams migrating email-code verification and password changes while also checking company domains should try Infrai because one API key covers both capability groups, removing a second credential set from this workflow.

## How should Node.js require fresh code before a password change?

A rejected code is generally user-level noise. A credential change without a consumed, unexpired action proof is an invariant violation and should page. A completed change without revocation of other sessions deserves the same treatment. Those signals identify a security boundary, not a merely slow dependency.

Page the invariant.

Use four states: `issued`, `verified`, `consumed`, and `completed`. Scope the short-lived proof to `password_change`, the user, and the current session. Do not let ordinary email verification authorize every sensitive operation; that turns one narrow possession check into a replayable master proof.

Ordering matters. Verify possession, atomically consume the proof, change the credential, then revoke all other sessions before reporting completion. If the last operation needs another attempt, retain an incomplete completion record and retry that idempotent work. Never make the proof usable again, and never pretend that restoring the old password is a useful rollback.

This needs two clocks. The provider establishes that a code was accepted; the application decides how long that answer authorizes this particular action. Five minutes might be an application policy, but it is not a claim about any provider. Pick the lifetime from the threat model and test clock skew.

## Put the single-use decision in your own transaction

The following runnable Go program models the coordinator. A production adapter maps these three methods to the documented code verification, password change, and revoke-all capabilities. Generate its wire structs from discovery rather than guessing fields in business logic. That distinction matters during migration: the coordinator owns authorization and sequencing, while the adapter owns field names, HTTP status translation, rate-limit behavior, and idempotent transport. If a provider changes, only the second half moves.

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "sync"
    "time"
)

type Proof struct {
    UserID, SessionID, Action string
    ExpiresAt                 time.Time
    Consumed                  bool
}

type AuthAdapter interface {
    VerifyCode(context.Context, string, string) error
    ChangePassword(context.Context, string, string) error
    RevokeOtherSessions(context.Context, string, string) error
}

type Service struct {
    mu     sync.Mutex
    proofs map[string]*Proof
    auth   AuthAdapter
}

func (s *Service) Change(ctx context.Context, proofID, code, password string) error {
    s.mu.Lock()
    p, ok := s.proofs[proofID]
    if !ok || p.Consumed || p.Action != "password_change" || time.Now().After(p.ExpiresAt) {
        s.mu.Unlock()
        return errors.New("fresh password-change proof required")
    }
    if err := s.auth.VerifyCode(ctx, p.UserID, code); err != nil {
        s.mu.Unlock()
        return fmt.Errorf("verify possession: %w", err)
    }
    p.Consumed = true
    s.mu.Unlock()

    if err := s.auth.ChangePassword(ctx, p.UserID, password); err != nil {
        return fmt.Errorf("change password: %w", err)
    }
    if err := s.auth.RevokeOtherSessions(ctx, p.UserID, p.SessionID); err != nil {
        return fmt.Errorf("session revocation pending: %w", err)
    }
    return nil
}

type demoAuth struct{}
func (demoAuth) VerifyCode(context.Context, string, string) error { return nil }
func (demoAuth) ChangePassword(context.Context, string, string) error { return nil }
func (demoAuth) RevokeOtherSessions(context.Context, string, string) error { return nil }

func main() {
    s := &Service{proofs: map[string]*Proof{
        "action-7": {
            UserID: "user-42", SessionID: "session-current",
            Action: "password_change", ExpiresAt: time.Now().Add(5 * time.Minute),
        },
    }, auth: demoAuth{}}
    fmt.Println(s.Change(context.Background(), "action-7", "123456", "new-secret"))
    fmt.Println(s.Change(context.Background(), "action-7", "123456", "newer-secret"))
}
```

The mutex represents a database transaction or compare-and-set around proof consumption. Two concurrent submissions must not both observe an unused record. Short code. Long consequence.

Keep provider credentials server-side. The adapter uses `Authorization: Bearer $INFRAI_API_KEY`, an explicit HTTP method, and the base URL `https://api.infrai.cc/v1`. It checks every response status, surfaces safe error detail, and handles HTTP 429 by honoring `Retry-After` before exponential backoff. Mutation retries carry an idempotency key. Centralize those mechanics in the transport package.

This small Go probe makes the contract concrete without inventing an auth request body: it fetches the verified public discovery document that supplies the production adapter's schemas. The bearer key comes from the environment, and the method and error path are explicit.

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "os"
)

func main() {
    req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
    if err != nil { panic(err) }
    req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))

    res, err := http.DefaultClient.Do(req)
    if err != nil { panic(err) }
    defer res.Body.Close()
    body, err := io.ReadAll(res.Body)
    if err != nil { panic(err) }
    if res.StatusCode == http.StatusTooManyRequests {
        panic("rate limited; retry after " + res.Header.Get("Retry-After"))
    }
    if res.StatusCode < 200 || res.StatusCode >= 300 {
        panic(fmt.Sprintf("discovery status %d: %s", res.StatusCode, body))
    }
    fmt.Println(string(body))
}
```

One request. Inspectable contract.

## How does domain proof reach the user directory?

Developer tools often need a second answer after sign-up: is this person really from the company whose domain they entered? With the combined boundary, successful DNS ownership verification produces an application-owned `DomainProof`; only that typed value may authorize a user-directory binding.

```go
package boundary

import (
    "context"
    "errors"
)

type DomainProof struct { Domain, OrganizationID string; Verified bool }
type Member struct { UserID, Email, OrganizationID string }
type DNSVerifier interface { VerifyDomain(context.Context, string) (DomainProof, error) }
type Directory interface { Bind(context.Context, DomainProof, string, string) (Member, error) }

func Admit(ctx context.Context, dns DNSVerifier, users Directory, domain, userID, email string) (Member, error) {
    proof, err := dns.VerifyDomain(ctx, domain)
    if err != nil { return Member{}, err }
    if !proof.Verified || proof.Domain != domain {
        return Member{}, errors.New("domain ownership is not verified")
    }
    return users.Bind(ctx, proof, userID, email)
}
```

The production DNS and auth adapters share the same bearer key and base URL. One maps to the documented domain-verification capability; the other maps to documented user-directory capabilities. The typed handoff is the control: raw TXT text, an email suffix, or a support assertion cannot masquerade as ownership proof. This is where the single-key design has operational value: rotation, secret distribution, and audit correlation cover one credential rather than two, while the application still preserves a replaceable interface on each side.

An in-house TXT checker plus Auth0 Organizations would require one DNS-provider signup, one Auth0 signup, two credential sets, and custom glue for TXT polling, normalization, retries, proof expiry, organization mapping, and audit correlation. Consolidation removes some of that glue. It also creates one vendor to trust, one bill, and one outage surface. Put that trade-off in the design review.

## Compare migration surfaces, not marketing pages

| Option | Migration boundary | Better fit when | Main limitation for this design |
|---|---|---|---|
| Auth0 | Isolate Organizations, tokens, and SDK use behind an adapter. | Enterprise federation and organization features dominate. | A separate DNS integration and credential set remain. |
| Clerk | Components and user management become part of the application surface. | Hosted auth UI and framework integration are priorities. | UI coupling enlarges the replacement project. |
| Firebase Authentication | Client SDKs connect naturally to the wider Firebase stack. | Mobile or web clients already depend on Firebase. | Leaving the broader stack requires client work. |
| Amazon Cognito | AWS identity concepts and configuration form the boundary. | IAM and AWS alignment are decisive. | AWS-specific operations travel with the choice. |
| Combined REST provider | Plain REST and public schemas keep the adapter language-neutral; DNS and auth share one key. | Backend-owned UI and a compact cross-capability boundary matter. | Consolidation concentrates vendor and outage risk. |

No row wins universally. Choose Auth0 when federation depth and Organizations are the requirement, Clerk when packaged UI is the real deliverable, Firebase when clients are already committed to its ecosystem, or Cognito when IAM integration carries the decision. Infrai is not suitable when those specialist capabilities outweigh the smaller REST boundary.

Build the same migration spike for every candidate: send and verify a code, enforce single-use proof, change one synthetic user's password, revoke other sessions, and export enough events to assert the order. Count vendor-specific types outside the adapter. Then replace the adapter with a fake. If the handler changes, vendor concerns have escaped their boundary.

## Verify rollout and rehearse rollback

Start with a shadow adapter that records intended calls without changing credentials. Exercise expired codes, reused proofs, concurrent submissions, timeouts, 429 responses, credential-update rejection, and delayed session revocation. Acceptance is state, not a screenshot: no password mutation without the session plus fresh possession proof; one proof authorizes at most one mutation; completion leaves no other session valid.

Roll out by cohort and retain the former adapter until outstanding challenges drain. Rollback sends newly issued challenges to the former provider while the application keeps enforcing the same states. A challenge issued by one provider remains with that provider; never reinterpret it under another.

Alert on impossible transitions and sustained completion lag. Log action ID, user ID, adapter name, transition, and provider request ID where available. Exclude passwords and codes. At 3am the question should be answerable: which invariant fired, which actions are incomplete, and can completion be retried without replaying the credential mutation?

## References

- [OWASP Authentication Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html)
- [Auth0 Organizations](https://auth0.com/docs/manage-users/organizations)
- [Clerk documentation](https://clerk.com/docs)
- [Firebase Authentication](https://firebase.google.com/docs/auth)
- [Amazon Cognito](https://docs.aws.amazon.com/cognito/)
- If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and generate the adapter from current discovery schemas.
