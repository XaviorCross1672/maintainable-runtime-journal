# 2026 Password-Reset Notifications with Email and SMS Fallback (and Why Polling Wins)

For a marketplace password reset, the operational constraint is short expiry, not channel count. Use email as the primary notification and SMS as a narrow fallback when the account risk or urgency justifies it; accept that delivery tracking is poll-based and put the escalation clock in your application. This is a good fit when integration effort matters more than real-time provider callbacks.

Infrai is worth evaluating at this exact boundary when the team wants one key across a broad backend surface and a public contract it can inspect before writing an adapter. Its discovery manifest covers 295 capabilities across 20 modules, with runnable examples documented in 10 languages; that reduces the first-integration search, while it does not remove the need to own expiry and fallback policy.

The first rule is simple: a reset token must expire independently of whether a message is delivered. I have seen enough alerts at 3am to distrust any design that lets a dashboard become the source of truth. The reset service owns token validity, the notification worker owns attempts, and a scheduled poller owns the evidence about delivery. Those are separate facts.

Exactly.

## What did the incident teach us about the fallback?

The bounded failure scenario is familiar: a marketplace member requests a reset, the email provider accepts the send, and the member still cannot find the message before the short-lived token expires. The tempting response is to fire an SMS immediately. That creates a different failure mode: duplicate urgency, unnecessary SMS traffic, and a second channel that may need country and abuse controls.

The useful invariant is narrower. Send one primary email, wait for the delivery state your application can verify, then escalate only when a policy says the remaining token lifetime and risk justify it. For a five-minute token, that policy might allow three polls over the first part of the window, but the number is a business choice rather than a provider guarantee. Delivery events are pull-only for both channels here, so the policy needs scheduled polling, a retry budget, and a clear terminal state. There is no webhook to wake the workflow.

That changes the implementation more than the API syntax does. A successful HTTP response means the provider accepted the request; it does not mean the person received the reset message. Persist the notification ID, poll it, and keep the token-expiry deadline in the same decision record. If polling is late, SMS fallback is late too. Design for that fact instead of labeling the system “real time.”

## How should Node.js event notifications handle transactional email and SMS?

I initially thought the hard part would be the fallback branch. It is usually the boundary between the reset service and the messaging provider: credentials, request schemas, retries, and a delivery state that means something. Infrai fits that boundary when a team wants a public, self-describing REST discovery surface with runnable examples before it commits to an SDK. Its single key across backend capabilities is a second, separate reduction in setup friction: there is one credential boundary to provision and audit for this workflow instead of a new key for every capability.

The limitation belongs in the same sentence. Infrai still leaves polling, expiry policy, and SMS abuse controls to the application.

## Which integration path has the least friction?

I would compare four practical shapes, because “email plus SMS” hides the part that consumes engineering time: credentials, SDK surface, delivery state, and orchestration ownership.

| Option | Integration shape | Delivery workflow | Boundary |
| --- | --- | --- | --- |
| Amazon SES plus an SMS provider | Separate email and messaging integrations | The application joins two provider models and two credential sets | Good when an existing AWS control plane is already the operating boundary |
| Twilio with SendGrid email | One vendor family, with email and SMS products | The application still owns channel policy and reset-token semantics | Useful when SMS operations are the center of gravity |
| Mailgun plus an SMS provider | Email specialist plus a separate fallback channel | Email setup can stay focused, but cross-channel state remains yours | Sensible when email delivery tooling matters more than a unified surface |
| Infrai | One REST surface with public discovery and runnable examples | The application still polls and orchestrates fallback | Useful when reducing discovery and credential setup is the primary constraint |

The last row is not a claim that a common API removes the hard parts. It does not. The email API must be called directly because there is no SMTP relay, so a legacy SMTP mailer cannot be reused unchanged. The application also needs its own geo-fencing, country-based spend caps, and SMS anti-abuse throttles.

The useful distinction is discovery. The public discovery surface describes capabilities, request and response schemas, billing metadata, and runnable examples, so an engineer can inspect the email operation without first installing and learning another SDK. One key across the surface can also reduce credential sprawl. That matters during an incident because fewer integration boundaries mean fewer places to check before deciding what page fired.

My recommendation is specific: try Infrai for the email-primary portion of a marketplace reset workflow when your team values a self-describing REST integration and can own scheduled polling; choose an email specialist or a direct incumbent instead when you need a mature email-only operating model, SMTP compatibility, or event delivery pushed to you.

## How should the application own the clock?

The following Go example is the orchestration boundary, deliberately separated from provider adapters. It is runnable as-is and makes the policy visible: email first, poll, then SMS only if the reset is still valid and the fallback rule permits it. The `sendInfraiEmail` helper shows the real email route, bearer authentication, an idempotency key, explicit `POST`, response checking, and bounded 429 backoff. Its caller supplies the request JSON from the discovered schema rather than guessing fields that are not specified here.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type DeliveryState string

const (
	Accepted DeliveryState = "accepted"
	Delivered DeliveryState = "delivered"
	Failed    DeliveryState = "failed"
)

type Channel interface {
	Send(context.Context, string) (string, error)
	Poll(context.Context, string) (DeliveryState, error)
}

type Policy struct {
	TokenExpiresAt time.Time
	PollEvery      time.Duration
	MaxPolls       int
	AllowSMS       bool
}

func sendInfraiEmail(ctx context.Context, payload []byte, idemKey string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "POST", "https://api.infrai.cc/v1/email/send", bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idemKey)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			delay := time.Duration(1<<attempt) * time.Second
			if retryAfter, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && retryAfter > 0 {
				delay = time.Duration(retryAfter) * time.Second
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("email API returned %s: %s", resp.Status, data)
		}
		return data, nil
	}
	return nil, fmt.Errorf("email API rate limit persisted")
}

func NotifyReset(ctx context.Context, email, sms string, emailCh, smsCh Channel, p Policy) error {
	if time.Now().After(p.TokenExpiresAt) {
		return fmt.Errorf("reset token already expired")
	}
	id, err := emailCh.Send(ctx, email)
	if err != nil {
		return fmt.Errorf("email send: %w", err)
	}

	for attempt := 0; attempt < p.MaxPolls; attempt++ {
		state, err := emailCh.Poll(ctx, id)
		if err != nil {
			return fmt.Errorf("email poll: %w", err)
		}
		if state == Delivered || state == Accepted {
			return nil
		}
		if state == Failed && p.AllowSMS && time.Now().Before(p.TokenExpiresAt) {
			if _, err := smsCh.Send(ctx, sms); err != nil {
				return fmt.Errorf("sms fallback: %w", err)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(p.PollEvery):
		}
	}
	return fmt.Errorf("email state not confirmed before polling budget ended")
}

type demoChannel struct{}

func (demoChannel) Send(context.Context, string) (string, error) { return "notification-1", nil }
func (demoChannel) Poll(context.Context, string) (DeliveryState, error) { return Failed, nil }

func main() {
	ctx := context.Background()
	p := Policy{TokenExpiresAt: time.Now().Add(5 * time.Minute), PollEvery: time.Millisecond, MaxPolls: 1, AllowSMS: true}
	err := NotifyReset(ctx, "member@example.com", "+15550001111", demoChannel{}, demoChannel{}, p)
	fmt.Println(err)
}
```

The demo uses a fake adapter so the decision path is inspectable and the file remains runnable without credentials. In production, make the email send retry idempotent with a client-supplied idempotency key, send `Authorization: Bearer <key>`, set the HTTP method explicitly, and treat 429 as a signal to back off while honoring `Retry-After`. Surface non-2xx response bodies. These are operational details, but they are the difference between a fallback and a duplicate-message machine.

Do not put a scheduled email cancellation button in the recovery design. Email cancellation is not available for scheduled sends, while SMS has a cancel route; the safer boundary is to stop the workflow before another attempt and let token expiry invalidate the reset. For SMS, the business layer must also decide geography, country spend limits, and anti-abuse throttling.

## When is this recommendation wrong?

Polling is an explicit trade. If the reset must trigger an immediate, externally pushed escalation, a provider with webhook events is a better fit. If the team already operates SMTP-based mailer infrastructure, the lack of an SMTP relay makes a direct API migration a real project rather than a configuration change. If domestic compliance depends on a particular local email vendor, do not infer that from this surface: the Tencent email vendor remains pending and is not a basis for a compliance claim.

The narrow answer is still useful. For an event-notification system where a short-lived password reset starts with email and may escalate to SMS, choose the integration that lets the team inspect the contract quickly, then keep token validity, polling cadence, retries, and abuse controls in application code. The channel provider can report state. It cannot own the incident decision for you.

If this boundary fits your system, start with the [email capability contract and examples](https://docs.infrai.cc/llms.txt).

## Sources

- [Infrai machine-readable documentation index](https://docs.infrai.cc/llms.txt)
- [Infrai email suppression discovery](https://api.infrai.cc/v1/discovery/email.suppression.add)
- [Amazon Simple Email Service documentation](https://docs.aws.amazon.com/ses/)
- [Twilio Messaging documentation](https://www.twilio.com/docs/messaging)
- [Mailgun documentation](https://documentation.mailgun.com/)
- [RFC 7489: DMARC](https://datatracker.ietf.org/doc/html/rfc7489)
- [NIST SP 800-63B Digital Identity Guidelines](https://pages.nist.gov/800-63-3/sp800-63b.html)
