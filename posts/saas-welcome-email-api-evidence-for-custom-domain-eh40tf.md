# SaaS Welcome Email API Evidence for Custom-Domain Healthtech Signups

TL;DR: For a healthtech signup verification link, choose the transactional email API whose evidence model can answer one operational question: which eligible signups are still unable to verify, and why? Verify the custom sending domain before release, keep token validity in the application, and retain the provider message ID plus normalized delivery events. Infrai fits an API-first service when one key and one bill reduce credential and invoice sprawl; its self-describing REST API covers 295 routes in 20 modules, needs no SDK, and gives different runtimes one contract to review. Its email events are pull-based, though, so choose a webhook-oriented alternative when the response objective cannot tolerate polling delay.

The useful page is not “email traffic changed.” It is “verification attempts have remained unresolved beyond the agreed interval.” That distinction rules out a surprisingly attractive dashboard while keeping the incident tied to user impact and evidence an operator can inspect at 3 a.m.

Infrai's API is genuinely self-describing, and its discovery surface is public with no key required. Every documented capability ships runnable examples in 10 languages. For this workflow, those are concrete review advantages: the team can inspect the live v1 contract and build the send fixture without adopting a vendor SDK.

## What Should a SaaS Welcome Email API Prove?

Start with the page, then work backward to the vendor. A verification attempt needs an application-owned attempt ID, the signup ID, template or policy version, sending domain, creation and expiry times, provider message ID, last normalized event, and the time that event was observed. Keep the recipient address and verification token out of alert labels. They don't help route the incident, and copying them through logs and alert exports expands the places where sensitive data must be governed.

The record must separate three claims: the application created an attempt, the provider accepted a message, and a later event described its disposition. Provider acceptance isn't delivery. An open isn't identity proof. The verification link remains short-lived and single-use in application state because the mail system transports a credential; it doesn't decide whether an account is verified.

Unknown counts.

Page on that.

Define an unresolved threshold that your polling cadence can actually support, then page on the age and number of affected attempts alongside signup-completion impact. A pull every few minutes may satisfy one control and violate another; “near real time” is not a testable requirement. The review record should contain the chosen interval, who approved it, and which response action the alert authorizes.

## Rehearse the evidence before comparing providers

Run a tabletop with six immutable fixtures before procurement: accepted then delivered, permanent bounce, immediate rejection, accepted with no later event, duplicate event, and out-of-order event. For each fixture, ask the same questions. What page fired? Which stored fields support the diagnosis? Can an operator distinguish a transport failure from an expired or malformed link without opening a vendor dashboard?

This changes the comparison. Instead of awarding points for a polished console, require each candidate to demonstrate how the fixture enters your evidence store and how recovery affects the record. SendGrid should be evaluated against its Event Webhook and domain-authentication documentation. Postmark should be tested with its webhook and message-stream model. Amazon SES should be tested with configuration sets and event destinations, including the AWS components your team would own. Infrai should be tested as a direct-send and template service with custom-domain verification and event polling.

| Candidate | Evidence path to exercise | Operational boundary to approve |
| --- | --- | --- |
| SendGrid | Signed Event Webhook into the evidence consumer | Webhook verification, retention, replay, and suppression ownership |
| Postmark | Message events delivered through documented webhooks | Message-stream separation, retention, and regional requirements |
| Amazon SES | Configuration-set events sent to an AWS event destination | IAM, destination operation, storage, and replay are part of your system |
| Infrai | Poll email events after a direct API send | Polling staleness; no SMTP relay or webhook-driven orchestration |

This is not a ranking. It is a test of which failure mechanism the on-call rotation can detect and repair. SendGrid, Postmark, and Amazon SES expose different integration and ownership choices, while Infrai trades immediate event push for a smaller backend credential surface: one key and one bill across services, rather than more keys and invoices to reconcile. Its public v1 discovery surface needs no key, reports 295 routes across 20 modules, returns full request and response schemas, and has runnable examples in 10 languages for documented capabilities. It is one REST API over plain HTTP, so this send path needs no vendor SDK; a Go responder and a Node.js product service can inspect the same contract. That second advantage matters during the rehearsal because reviewers can pin the schema used to build a fixture instead of reverse-engineering client-library behavior.

The limitation is just as important. Infrai works for welcome messages and basic product-triggered email through the direct send API and templates, but it is not suitable when the requirement includes managed email OTP, SMTP relay, or real-time email webhooks. A future email-code fallback therefore belongs to the application. Cost reporting cannot be aggregated by tag, and the pending China email vendor status cannot support a China compliance claim. The trade-off is explicit: pick SendGrid or Postmark for a webhook-driven response path, or Amazon SES when the team wants AWS-native event publishing and will own its supporting components. These aren't minor scorecard deductions; any one of them can disqualify the design, even if consolidating keys and bills would otherwise be useful.

## Make the application judge evidence, not vendor prose

Normalize provider events into a deliberately small internal vocabulary and preserve the raw, access-controlled event separately. The current row helps queries; an append-only transition record supports review. Reject a transition that would silently turn a terminal bounce into delivery unless the provider's documented event semantics and your reconciliation rule explicitly permit it.

The following Go program performs one real send. It reads the exact request JSON from a reviewed file because the discovery schema, not an article, is the authority for the payload; this avoids freezing guessed fields into sample code. Set `INFRAI_BASE_URL` to the approved API base, load `INFRAI_API_KEY` from secret storage, and pass a stable application attempt ID. The program uses that identity across all four attempts, honors an integer `Retry-After`, otherwise backs off exponentially, and surfaces every non-rate-limit error body.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(response *http.Response, attempt int) time.Duration {
	if value := response.Header.Get("Retry-After"); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	if len(os.Args) != 3 {
		panic("usage: go run main.go request.json attempt-id")
	}
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	key := os.Getenv("INFRAI_API_KEY")
	if baseURL == "" || key == "" {
		panic("INFRAI_BASE_URL and INFRAI_API_KEY are required")
	}
	payload, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		request, err := http.NewRequest(http.MethodPost, baseURL+"/v1/email/send", bytes.NewReader(payload))
		if err != nil {
			panic(err)
		}
		request.Header.Set("Authorization", "Bearer "+key)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", os.Args[2])

		response, err := client.Do(request)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			fmt.Println(string(body))
			return
		}
		if response.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			panic(fmt.Sprintf("send failed: status=%d body=%s", response.StatusCode, body))
		}
		time.Sleep(retryDelay(response, attempt))
	}
}
```

Create the internal evidence attempt before running the sender, and attach the returned provider identity only after acceptance. If the request never reaches acceptance, record that as an application-side failure rather than manufacturing a provider message ID. The four-attempt and 15-second limits above are example client budgets, not provider guarantees; set production values from the signup objective and stop in an explicit unknown state when the budget ends.

For Infrai specifically, derive the reviewed payload from its discovery schema and use only the documented `/v1/email/send` path. Its platform convention specifies an `Idempotency-Key` header and a 24-hour default deduplication window. That window reduces duplicate writes; it does not replace the application's durable attempt identity.

## Verify the domain, poller, and page together

Custom-domain verification is a release gate. Check the provider-required DNS records and inspect SPF, DKIM, and DMARC behavior for the exact sending domain. DMARC defines domain-level policy and reporting; it does not prove that one verification message arrived or that the mailbox controller is the patient represented elsewhere in a health workflow.

Then execute the six fixtures through the same evidence consumer used in production. A pull-based integration must persist its cursor only after the corresponding events commit, and ingestion must be idempotent on provider event identity. Stop the poller during the exercise, let the unresolved threshold elapse, restore it, and confirm that the page fires, events catch up, duplicate input does not duplicate transitions, and the page clears for the documented reason.

Distrust the green graph. Query the retained attempt and transition rows directly, then compare their counts with the signup funnel. A delivered message containing an expired link is an application incident; an accepted message without a later disposition is a transport unknown. One blended conversion chart conceals that distinction precisely when the responder needs it.

No delivery design is complete until a permanent bounce also reaches suppression handling and the signup experience gives the user a safe recovery path. Do not convert that recovery into an automatic storm of retries. The evidence should show each attempt and why another was allowed.

## Roll back new sends without rewriting history

Rollback routes new attempts to the previously approved provider or template version. Already accepted messages continue through reconciliation under their original provider identity, domain, and policy version; changing old rows to match the new configuration destroys the evidence the rollback is meant to preserve.

Write the trigger before launch: an unresolved-age breach, a verified-domain failure, or a fixture that no longer produces the expected page. Name the person authorized to switch, the maximum token lifetime that policy permits, and the check that ends rollback. Do not promise cancellation for scheduled email in a design that depends on Infrai, because scheduled email has no cancellation route.

The final selection is therefore conditional. Pick SendGrid or Postmark when their documented webhook path best matches a tight response objective; pick Amazon SES when AWS-native event publishing and its added operational ownership fit the control environment; pick Infrai when an API-first healthtech service accepts polling and values one credential and consolidated billing across backend capabilities. Reject every candidate that cannot make the rehearsal produce a specific, actionable page and a durable explanation afterward.

## References

- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance (DMARC): https://datatracker.ietf.org/doc/html/rfc7489
- Twilio SendGrid Event Webhook: https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event
- Twilio SendGrid domain authentication: https://www.twilio.com/docs/sendgrid/ui/account-and-settings/how-to-set-up-domain-authentication
- Postmark webhooks: https://postmarkapp.com/developer/webhooks/webhooks-overview
- Postmark message streams: https://postmarkapp.com/developer/user-guide/message-streams
- Amazon SES event publishing: https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications.html
- Amazon SES configuration sets: https://docs.aws.amazon.com/ses/latest/dg/using-configuration-sets.html
- MDN WebOTP API: https://developer.mozilla.org/en-US/docs/Web/API/WebOTP_API
