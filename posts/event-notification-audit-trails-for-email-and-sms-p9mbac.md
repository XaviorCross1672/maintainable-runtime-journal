# Event Notification Audit Trails for Email and SMS (With Opt-Out Suppression)

A compliance notice is not complete when an API accepts it. It is complete when the system can show which policy snapshot authorized each channel, which suppression checks ran, what the provider accepted, and what happened afterward. For an edtech team optimizing for integration effort, the practical choice is one notification orchestrator with an append-only decision record, separate email and SMS adapters, and a final consent check immediately before dispatch.

**Short answer:** resolve user channel preferences and opt-out suppression in one deterministic policy function, persist that decision before delivery, then let channel workers send only from the resulting immutable plan.

This distinction matters during an incident. Consider a bounded failure: a district uploads a revised mandatory-notice cohort at 09:00, a learner opts out of SMS at 09:01, and a retry worker receives an older job at 09:02. A system that copied `sms: true` into the job when the cohort was built can send against stale consent. Its dashboard may still show a pleasing green delivery rate. The page that should fire is different: `dispatch_blocked_due_to_newer_consent`, grouped by policy version and campaign, with the attempted decision retained for audit.

No send is the correct send.

The invariant is narrow: event data says what happened, preference data says what the user currently permits, suppression data records a channel-level prohibition, and the delivery ledger says why a particular attempt was allowed or denied. Mixing those four facts into one mutable notification row makes the audit trail disappear exactly when compliance asks for it.

## How should an event notification system apply user channel preferences and opt out rules?

Treat routing as a policy decision, not a template option. The input should include the event identifier, notice class, recipient, available email and SMS destinations, the current preference version, and every applicable suppression entry. The output should be a frozen plan containing zero or more channel attempts plus reason codes for every excluded route. Those reason codes matter more than another chart; `SMS_USER_OPT_OUT` answers a review question, while `not sent` does not.

There are two checks because the data can change between planning and execution. The planner reads a consistent preference snapshot and writes its version into the plan. The worker then performs a fresh suppression lookup immediately before calling a provider. If a newer opt-out exists, it records a blocked attempt and stops. It must not silently mutate the old plan to look as if SMS was never considered. That old decision is evidence.

Consent can change.

Transactional messages complicate the model. An organization may have a lawful or contractual reason to deliver a required notice even when promotional messages are disabled, but channel rules and the institution's legal basis still determine what is permitted. CTIA's messaging principles emphasize consumer choice and opt-out handling for messaging programs. This article can't settle whether a particular school notice qualifies for a particular exception; counsel and the institution's approved policy must define the notice classes, and engineering should encode those classes rather than infer them from template names.

Keep precedence explicit. A defensible order is destination invalidation, global channel suppression, program-specific opt-out, user preference, then notice-class policy. Emergency or legally mandated behavior should be a named rule with an owner and version, never an `if urgent` branch added during a page.

## The ledger is the product boundary

Adapters come later.

The sending adapter is replaceable. The decision ledger is not. Store an immutable row before enqueueing any delivery attempt, including a stable `decision_id`, event ID, recipient pseudonymous ID, policy version, preference version, channel, destination fingerprint, outcome, reason code, and evaluation time. Do not store raw message bodies in the operational ledger unless retention policy requires them; a template version and content hash are usually better audit keys, while sensitive content belongs behind tighter access controls.

A useful state model is small: `planned`, `blocked`, `submitted`, `delivered`, `failed`, and `unknown`. Provider acceptance means submitted, not delivered. Email and SMS callbacks can advance the attempt, but they must never overwrite earlier transitions. Append a new transition with the provider event identifier and received time. Duplicate callbacks are normal enough that uniqueness on `(provider, provider_event_id)` should be part of the design, not an incident follow-up.

The catch is that append-only storage costs more operational attention than updating a `status` column. Retention, access review, erasure obligations, and reconciliation all need owners. A tiny system sending non-regulated convenience alerts may be better served by a simpler outbox and short-lived delivery log. For an auditable compliance notice, though, mutable status alone is not suitable because it cannot reconstruct the decision that preceded the send.

Here is the core boundary in Go. The store and sender are interfaces on purpose; the policy is testable without a network, and the adapter cannot bypass the last suppression check.

```go
package notice

import (
    "context"
    "errors"
    "time"
)

type Channel string

const (
    Email Channel = "email"
    SMS   Channel = "sms"
)

type Attempt struct {
    DecisionID       string
    EventID          string
    RecipientID      string
    Channel          Channel
    Destination      string
    PreferenceVersion int64
    PolicyVersion    string
    TemplateVersion  string
}

type Suppression struct {
    Blocked bool
    Version int64
    Reason  string
}

type Store interface {
    SuppressionFor(ctx context.Context, recipientID string, channel Channel) (Suppression, error)
    Append(ctx context.Context, decisionID, state, reason string, at time.Time) error
}

type Sender interface {
    Submit(ctx context.Context, attempt Attempt) (providerMessageID string, err error)
}

func Dispatch(ctx context.Context, store Store, sender Sender, a Attempt, now time.Time) error {
    suppression, err := store.SuppressionFor(ctx, a.RecipientID, a.Channel)
    if err != nil {
        return err
    }
    if suppression.Blocked || suppression.Version > a.PreferenceVersion {
        reason := suppression.Reason
        if reason == "" {
            reason = "NEWER_CONSENT_STATE"
        }
        return store.Append(ctx, a.DecisionID, "blocked", reason, now)
    }

    providerID, err := sender.Submit(ctx, a)
    if err != nil {
        _ = store.Append(ctx, a.DecisionID, "failed", "SUBMIT_ERROR", now)
        return err
    }
    if providerID == "" {
        return errors.New("provider returned an empty message identifier")
    }
    return store.Append(ctx, a.DecisionID, "submitted", providerID, now)
}
```

One detail is intentionally unresolved: whether failure to append `submitted` should trigger an immediate retry. I'm not sure there is a universal answer. It depends on whether the provider supports an idempotency key that survives ambiguous responses. Without that guarantee, an automatic retry can create a duplicate compliance notice; the safer response may be an `unknown` transition, a reconciliation job, and a page tied to an explicit service objective.

## Idempotency must span planning, sending, and callbacks

Retries lie.

A queue's at-least-once delivery is only one source of duplication. Cohort imports can repeat an event, workers can lose acknowledgments, providers can accept a request before the client sees the response, and callbacks can arrive twice or out of order. Use different keys for different boundaries: an event key prevents the same business event from creating two plans; a decision key identifies one recipient-policy evaluation; an attempt key identifies a channel submission; and a provider event key deduplicates callbacks. Reusing one key everywhere looks tidy but hides which boundary actually failed.

For the example above, an event key could be derived from the district, notice type, academic term, and source revision. A decision key then adds the recipient and policy version. The attempt key adds the channel and template version. Hashing those canonical fields makes retries reproducible, provided canonicalization is specified and covered by golden tests. Never derive a key from the rendered body or a timestamp. Both can change without changing the business intent.

Retries need a budget and a deadline. A compliance notice due by a fixed time should not sit in exponential backoff after its usefulness has expired. Record `next_attempt_at`, attempt count, and deadline; page on exhausted or overdue cohorts, not on every individual rejection. A bad phone number is work for data quality. A sudden rise in `UNKNOWN_SUPPRESSION_STATE` is work for the on-call engineer. Those are different queues and should produce different pages.

## Integration effort belongs in the failure model

The shortest initial integration is often one SDK call in a request handler. It is also the design most likely to couple consent reads, rendering, provider latency, and application retries into a path nobody can explain at 03:00. Count integration effort across the lifecycle: schema migrations, local contract tests, secret rotation, callback authentication, replay tooling, dashboards, pages, retention, and provider replacement. Lines of setup code are a poor proxy.

Count all of it.

Use a transactional outbox when notice creation must commit with application state. A relay can turn each outbox row into a plan, while separate channel workers own provider concerns. This adds a table and a worker, but it removes the dangerous gap where the application commits the compliance event and fails before enqueueing it. If the source system already offers a durable event log with replay and stable identifiers, duplicating it with another outbox may be unnecessary; document which log owns replay before choosing either design.

Provider adapters should expose the same small contract: submit with an attempt key, normalize acceptance into `submitted`, authenticate callbacks, and preserve raw provider event identifiers. Product-specific features may still matter, but they belong behind capability flags and contract tests. For example, an adapter that cannot guarantee request idempotency should declare that boundary so the orchestrator uses reconciliation rather than blind retries. Don't let a generic interface pretend away a real semantic difference.

Cost belongs in this review, but not as a per-message beauty contest. Include engineering time for callback handling, retention storage, support escalation, number validation, and operating a second channel. The decision should change when those costs change. Your mileage may vary, especially when an institution already has a reviewed messaging contract and established operational runbooks.

## What should page the engineer carrying this system?

Page the threat.

Page on threats to the compliance outcome: notices approaching their deadline with no terminal delivery state, sustained inability to read consent, a callback authentication failure across a cohort, or divergence between submitted attempts and reconciled provider records. Ticket or dashboard lower-urgency conditions such as isolated invalid destinations and ordinary user opt-outs. Delivery percentage alone is actively misleading because a high rate can coexist with unauthorized sends, while a lower rate may prove that suppression is working.

The first dashboard panel should reconcile counts across the pipeline: source events, decisions, blocked routes by reason, submitted attempts, terminal outcomes, and unresolved attempts. Every count must drill into decision IDs. The second panel should show age, because fifty unresolved attempts from one minute ago are different from one unresolved mandatory notice past its deadline. The third should show policy and preference versions so a rollout can be correlated with changed behavior.

Then test the ugly transitions. Run table-driven policy tests for channel combinations and notice classes; race an opt-out against a queued attempt; replay the same event 100 times; deliver callbacks in reverse order; rotate callback secrets with an overlap window; and restore the ledger into a clean environment to prove the audit record is usable. A test that only asserts the email adapter was called misses the question compliance will ask: what policy allowed that call?

This architecture is not suitable for every message. Keep direct, low-complexity delivery for disposable internal alerts where consent, deadlines, and evidentiary records are irrelevant. Use the orchestrated path when the organization must prove why it contacted a learner or guardian, and judge it by the page it fires when that proof is at risk.

## Sources

- https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms
- https://resend.com/docs/introduction
