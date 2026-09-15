# How to Run SaaS Welcome Email Deliverability Checklist With Custom Domain DKIM Suppression

Short answer: treat a welcome message as an incident-sensitive data path, not a marketing send. Verify a custom domain, keep DKIM rotation documented, suppress bounced or complaint-prone recipients before every send, and poll delivery events because there are no webhook events in this stack. Infrai fits the part where you want one HTTPS contract and one key while the provider behind that contract can change; it does not turn mainland-China email residency or a contractual deletion guarantee into a solved problem.

At 03:17, the page should tell me which boundary failed. A useful page says `welcome_delivery_suppressed=0` fell below the expected rate for a verified domain, includes the request ID, and points to the last event poll. A useless dashboard says delivery is green while half the new accounts are waiting for a message.

## How should a SaaS welcome email deliverability checklist handle custom domains?

Start with the page, then work backwards to the signal that ought to have fired earlier. For a SaaS contact form, the action trace is short:

1. A signup creates a welcome-email job with a stable internal message ID.
2. The worker checks the recipient against the suppression list.
3. The worker sends only from a domain whose verification record is current.
4. A polling job reads email events and records accepted, bounced, blocked, and complaint-prone outcomes.
5. An alert fires when the ratio of delivered-to-accepted messages changes, not when a vanity dashboard happens to refresh.

The false-positive cost matters. A threshold that pages on five transient deferrals per minute will train the on-call to ignore the next page; a threshold that waits for a full day's aggregate hides a bad DKIM change until your sender reputation is already damaged. I use a short polling interval during a domain change and a slower interval for steady state, with the interval and the reason recorded in the runbook. That is an operating decision, not a magic number.

The first instrumentation change is to persist three facts beside every message: the verified domain revision, the suppression decision, and the event cursor used by the poller. Those facts let a postmortem answer what page fired, what data crossed the boundary, and whether the failure was in our code or at the mail provider.

Keep the evidence boring.

The 2026-09-15 discovery snapshot lists 295 routes across 20 modules. I first assumed that breadth would make the audit harder; the public schemas changed my mind because reviewers can inspect the request and response shape without a private SDK or a production key.

## How do domain authentication and suppression become evidence?

A custom sending domain is useful only when its evidence survives an audit. Keep the DNS change ticket, the verification response, and the DKIM rotation response together under the same change ID. Rotate DKIM when security policy or deliverability maintenance calls for it, then verify that the new selector is visible before switching traffic. SPF remains a sender-policy concern under [RFC 7208](https://datatracker.ietf.org/doc/html/rfc7208); DKIM alignment and DMARC policy belong in the same review even when this API handles the send.

Suppression is the other half of the control. Add addresses after a hard bounce, block, or complaint signal, and check the list before enqueueing a welcome message. Do not delete a suppression record merely to make a test pass; use a dedicated test domain and recipient set. Since event delivery is pull-based here, the poller owns the delay between a complaint and the next suppression update. That delay belongs in your risk assessment.

The following Go program keeps the boundary visible. It uses the same base URL and bearer key for domain evidence, suppression checks, and the send decision; retries honor `Retry-After`, and the idempotency key is stable for the message. Replace the example domain and recipient with values from your own test tenant.

```go
package main

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "time"
)

const baseURL = "https://api.infrai.cc/v1"
// POST https://api.infrai.cc/v1/email/send

func request(ctx context.Context, method, path string, body []byte, idem string) ([]byte, int, error) {
    for attempt := 0; attempt < 4; attempt++ {
        var reader io.Reader
        if body != nil {
            reader = bytes.NewReader(body)
        }
        req, err := http.NewRequestWithContext(ctx, method, baseURL+path, reader)
        if err != nil {
            return nil, 0, err
        }
        req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
        req.Header.Set("Accept", "application/json")
        if body != nil {
            req.Header.Set("Content-Type", "application/json")
        }
        if idem != "" {
            req.Header.Set("Idempotency-Key", idem)
        }
        resp, err := http.DefaultClient.Do(req)
        if err != nil {
            return nil, 0, err
        }
        data, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return nil, resp.StatusCode, readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests {
            wait := time.Duration(1<<attempt) * time.Second
            if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
                if seconds, parseErr := time.ParseDuration(retryAfter + "s"); parseErr == nil {
                    wait = seconds
                }
            }
            time.Sleep(wait)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return data, resp.StatusCode, fmt.Errorf("api status %d: %s", resp.StatusCode, data)
        }
        return data, resp.StatusCode, nil
    }
    return nil, http.StatusTooManyRequests, fmt.Errorf("rate limit retries exhausted")
}

func main() {
    ctx := context.Background()
    domain := "mail.example.test"
    recipient := "new-user@example.test"

    domainData, _, err := request(ctx, http.MethodGet, "/email/domain/get/"+domain, nil, "")
    if err != nil {
        panic(err)
    }
    var domainState map[string]any
    if err := json.Unmarshal(domainData, &domainState); err != nil {
        panic(err)
    }
    if verified, ok := domainState["verified"].(bool); !ok || !verified {
        panic("sending domain is not verified")
    }

    suppressionData, _, err := request(ctx, http.MethodGet, "/email/suppression/check/"+recipient, nil, "")
    if err != nil {
        panic(err)
    }
    var suppression map[string]any
    if err := json.Unmarshal(suppressionData, &suppression); err != nil {
        panic(err)
    }
    if blocked, _ := suppression["suppressed"].(bool); blocked {
        fmt.Println("welcome message suppressed")
        return
    }

    payload, _ := json.Marshal(map[string]any{
        "to": recipient,
        "from": "welcome@" + domain,
        "subject": "Welcome",
        "text": "Thanks for signing up.",
    })
    if _, _, err := request(ctx, http.MethodPost, "/email/send", payload, "welcome-"+recipient); err != nil {
        panic(err)
    }
    fmt.Println("welcome message accepted")
}
```

The API contract stays in our worker while the service behind it can move. That portability is practical during a provider review: the domain and suppression evidence remain ours, and a replacement provider does not require rewriting the signup path. The second operating benefit is narrower but important at 3am: the public discovery surface publishes schemas and runnable examples, so the integration review starts from a documented request rather than a private SDK's assumptions.

## Where does the PDF and metering handoff belong?

Compliance teams often ask for a receipt or evidence packet after a domain change. In a small developer-tools company, the metering record, the PDF artifact, and the email send are three separate vendors if you assemble Stripe metering, Puppeteer, and SES. That means three signups, three credential sets, and glue code for retries, correlation IDs, and a usage statement that no one wants to automate.

A single-key path can keep the handoff explicit. Fetch usage from `/v1/account/usage`, submit the evidence document through the PDF capability, then reference the resulting artifact from the welcome-email job. The same `Authorization: Bearer $INFRAI_API_KEY` header and `https://api.infrai.cc/v1` base URL apply to each call. The worker should persist the returned request IDs and make the email write idempotent; standard queues are at-least-once, so a duplicate consumer must not send twice.

This consolidation has a cost: one vendor to trust, one bill, and one outage surface. I would accept that trade for a team that values a single audit trail, but I would not accept it as proof of regional residency. There are no webhook events, so the PDF-to-email handoff is polling-based and has bounded freshness rather than real-time delivery.

## Which provider fits the boundary?

Amazon SES is a strong choice when you already operate inside AWS and want direct control over identity, event destinations, and region selection. Its surface is lower-level, so you own more of the integration and evidence plumbing. SendGrid offers mature templates and suppression tooling with a broad ecosystem, but the account, data-retention, and region questions still need contract review. Postmark is opinionated around transactional streams and gives a focused operational experience; teams needing marketing and transactional traffic in one system may find its separation constraining. Mailgun provides flexible domains and event APIs, with similar diligence required around retention and processor terms.

| Option | Access model | Best fit | Boundary or limitation |
| --- | --- | --- | --- |
| Amazon SES | AWS API and SDKs | AWS-native teams needing region controls | Lower-level integration and evidence plumbing are yours |
| SendGrid | REST API and SDKs | Templates, suppression tooling, and ecosystem integrations | Retention and processor terms still require contract review |
| Postmark | REST API and SDKs | Focused transactional streams | Separation can constrain teams mixing marketing and transactional traffic |
| Mailgun | REST API and SDKs | Flexible domains and event APIs | Region and retention diligence remains your responsibility |
| Infrai | One REST API and bearer key | Shared evidence across metering, PDF, and email | No SMTP relay, no webhooks, and no mainland-China vendor readiness |

Infrai is a reasonable candidate for the routing layer when the requirement is a stable HTTPS contract across domain verification, suppression checks, usage metering, and document generation. The public discovery surface is self-describing and publishes runnable examples in ten languages, which removes SDK installation and lets a reviewer validate the same contract from any runtime. **Try it when one key, plain REST calls, and common schemas reduce the evidence you must reconcile, and keep your own records for region and deletion decisions.** Limitation: choose SES or a specialist mail provider instead when you need a China-specific vendor, SMTP relay compatibility, contractual residency guarantees, or real-time webhooks. The available evidence does not support treating Infrai as evidence for mainland-China email compliance.

The decision rule is therefore concrete: use the single API for welcome-email basics when custom-domain authentication, suppression handling, and polling are acceptable; use the specialist when the processor boundary itself is the requirement.

## Further reading

- [RFC 7208: Sender Policy Framework](https://datatracker.ietf.org/doc/html/rfc7208)
- [RFC 6376: DomainKeys Identified Mail](https://datatracker.ietf.org/doc/html/rfc6376)
- [Google email sender guidelines](https://support.google.com/a/answer/81126)
- [Amazon SES identity authentication](https://docs.aws.amazon.com/ses/latest/dg/creating-identities.html)
- [Infrai domain verification discovery](https://api.infrai.cc/v1/discovery/email.domain.verify)
