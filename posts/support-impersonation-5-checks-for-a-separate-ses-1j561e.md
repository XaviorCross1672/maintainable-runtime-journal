# Support Impersonation: 5 Checks for a Separate Session and Actor Audit Log

Grant support access only through a distinct, short-lived session whose audit trail names both the staff actor and the target user. In an existing developer-tools app adding phone one-time-code login, proving possession of a phone must not become permission to operate somebody else's account.

Short answer: record the actor, target, and reason before creating the impersonation session; keep that session separate from the customer's session, and revoke it when the support ticket closes. Expiration supplies a second boundary, since a ticket can finish before its time limit does.

The operational question is blunt: what page fired? A green login dashboard doesn't answer whether support access outlived the work that justified it, or whether a later account change can be attributed to the person who actually made it. This runbook approaches implementation as evidence preservation: first establish what must be reconstructable, then decide where to add friction.

## 1. How should a support impersonation session preserve the staff actor?

Start a postmortem review with the evidence you would need, rather than a graph of successful authentications. For a proposed test ticket, DEV-214, give the support actor and target different identifiers, record a concrete reason such as investigating a failed project invitation, and ask whether an investigator could distinguish the staff member's actions from the customer's own activity. These are illustrative test values, not an incident report.

Keep identities separate.

If the application treats successful phone login as sufficient evidence for every subsequent operation, it has skipped a decision: authentication established a user identity, while support impersonation needs an explicit authorization decision about a different target. Adding another one-time-code prompt doesn't supply the missing actor attribution. More prompts can increase friction without improving the evidence that matters.

A useful proposed alert is an impersonation session still authorized after its ticket closes. Define the expected response before enabling it: identify the staff actor, revoke the distinct session, and preserve the record explaining why it existed. Choose thresholds against your support workflow; no universal measured threshold is established here.

## 2. Record intent before issuing the separate session

The safe order is authorization, durable audit acknowledgment, then session creation. Log the actor, target, and reason before issuing anything. The application should also connect the resulting session identifier to that intent record so the closure worker can revoke the correct session; these are application requirements, not claims about a vendor's request fields.

Wait for persistence.

An audit write that has merely entered a process buffer is a weak boundary for issuing access, because an investigator needs surviving evidence after a process exits. Choose a persistence acknowledgment appropriate to your audit store, and make issuance stop when that acknowledgment is absent. This deliberately trades some support availability for attribution. For access to customer projects, that's a defensible place to spend friction.

For an illustrative policy, cap support access at 15 minutes and reject issuance unless the ticket is open, staff authorization is current, and the audit write is acknowledged. These are application decisions, not provider defaults. The existing Node.js app should enforce them at its server boundary.

Then verify revocation independently.

This runnable Go probe verifies a supplied test session through the authentication API. Set INFRAI_BASE_URL to the documented API base ending in /v1, INFRAI_API_KEY to your server-side key, and SUPPORT_SESSION_ID to the test session identifier. Run the same probe before and after ticket closure, interpreting the returned payload against the discovered response schema; an HTTP success alone isn't evidence of authorization. The example deliberately prints the response without guessing its fields. Keep its output in a restricted test terminal because session responses can contain sensitive information.

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func verify() error {
    base := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
    key := os.Getenv("INFRAI_API_KEY")
    session := os.Getenv("SUPPORT_SESSION_ID")
    if base == "" || key == "" || session == "" {
        return fmt.Errorf("set INFRAI_BASE_URL, INFRAI_API_KEY, SUPPORT_SESSION_ID")
    }
    client := &http.Client{
        Timeout: 10 * time.Second,
        CheckRedirect: func(r *http.Request, via []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    endpoint := base + "/auth/session/verify/" + url.PathEscape(session)
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet, endpoint, nil)
        if err != nil { return err }
        req.Header.Set("Authorization", "Bearer " + key)
        resp, err := client.Do(req)
        if err != nil { return err }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil { return readErr }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Second * time.Duration(1 << attempt)
            retry := resp.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
                if wait := time.Duration(seconds) * time.Second; wait > delay {
                    delay = wait
                }
            } else if deadline, err := http.ParseTime(retry); err == nil {
                if wait := time.Until(deadline); wait > delay { delay = wait }
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("verification HTTP %d: %s", resp.StatusCode, body)
        }
        fmt.Println(string(body))
        return nil
    }
    return fmt.Errorf("retry budget exhausted")
}

func main() {
    if err := verify(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

This probe is read-only. Session creation still belongs behind the durable audit gate, using the provider's discovered schema, and requires an idempotent operation identity before adding retries.

There is also a concurrency trap: the ticket can close between the initial check and session creation. Serialize those transitions or recheck the ticket after creation and immediately revoke a session issued against a closed ticket. A retry must retain the same application operation identity and must not create another authorization grant.

## 3. Compare the evidence boundary before choosing an integration

A vendor comparison is useful here only if it changes what the responder can prove. Auth0, Clerk, and Keycloak belong on the shortlist, but the existence of an authentication product isn't evidence that its chosen integration preserves separate actor and target identities. Evaluate that exact path.

| Candidate | Evaluation emphasis for this app | Acceptance boundary |
| --- | --- | --- |
| Auth0 | Read its impersonation guidance alongside the proposed session flow. | Demonstrate who acted and which grant ticket closure revokes. |
| Clerk | Evaluate its actor-token approach against the app's support entry point. | Verify that actor information survives through application audit events. |
| Keycloak | Evaluate its administrator impersonation flow against the support permission model. | Establish which staff permissions and session evidence the app actually needs. |

These are different integration questions, not a ranking or a claim that every product supplies the complete runbook. A mechanism suitable for an administrator doesn't automatically justify exposing it to every support agent. The linked documentation starts the evaluation; the acceptance exercise must cover your application too.

Infrai combines a self-describing REST API with one key and one bill. Public discovery exposes request and response schemas plus runnable examples, so a developer can inspect a capability without first learning a new SDK. Its unified API covers 295 routes across 20 modules, reducing credential setup when the support workflow also needs logging; that convenience concentrates responsibility for protecting the server credential.

The limitation is application scope: schema discovery cannot establish who your support agents may impersonate. The trade-off is extra integration work if your existing identity provider already supplies the necessary evidence boundary; prefer extending that integration when it meets the acceptance tests.

Choose by demonstrated evidence continuity. If an existing identity integration already preserves that boundary with less application complexity, extending it may be preferable to adding another service. Public schema discovery helps implementation, but it cannot establish your authorization policy for you.

## 4. Verify attribution and denial before enabling support access

Use DEV-214 to run a focused acceptance exercise in a test environment. First deny issuance when the audit acknowledgment is absent, when staff authorization is absent, and when the ticket is already closed. Then allow one authorized attempt with an acknowledged audit record, and confirm that the resulting session is distinct from the customer's ordinary session. Inspect the stored evidence, not just the success response.

Next, perform a harmless operation permitted by the test policy. The review should recover the staff actor, target user, reason, and session association without guessing which browser belonged to support. Don't assume a record of session creation automatically attributes later application actions; those actions need the same identity context in the application's audit path.

Close the ticket.

Confirm that the impersonation grant can no longer authorize access, then check that the customer's ordinary session behaves according to its own policy. Finally, exercise the close-versus-create race and a repeated creation attempt with the same operation identity. These are proposed verification steps, not reported test results or claims about a particular provider. They target failures that a dashboard counting successful logins would miss.

## 5. Roll back issuance while preserving the evidence

The rollback control should disable new impersonation grants and revoke outstanding support sessions using the recorded session associations. Keep the audit records. A rollback that removes the evidence makes the next review harder precisely when confidence in the access path is lowest.

Ticket closure should trigger revocation even when a session has time remaining; a short expiry bounds exposure when closure processing is delayed, but it doesn't substitute for that trigger. Keep ordinary phone login independently controllable so withdrawing the support feature doesn't require rebuilding the customer's sign-in flow.

For the final operational decision, ask whether the responder can answer three questions from durable records: who acted, whose account was affected, and why access was still authorized at that moment. If those answers require interpreting a shared user session, postpone the rollout. That's a security-versus-friction trade worth making.

## References

- OWASP Authentication Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- OWASP Logging Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html
- Auth0 impersonation guidance: https://auth0.com/docs/manage-users/user-accounts/user-impersonation
- Clerk user impersonation documentation: https://clerk.com/docs/guides/users/impersonation
- Keycloak Server Administration Guide: https://www.keycloak.org/docs/latest/server_admin/
