# 5 Failure Boundaries Explained for User Reminder Queue Retries and Dead Letters

Treat every shipment notification as a recoverable state transition, not as a one-shot email or webhook job. The deciding constraint is that a retry can happen after the downstream system accepted a request but before the worker recorded success, so the queue must preserve intent, the consumer must be idempotent, and operators must be able to distinguish delayed work from permanently rejected work.

**Short answer:** choose a queue by testing five boundaries: scheduled release, worker ownership, retry timing, duplicate suppression, and dead-letter recovery. Keep the notification intent in durable storage, assign one stable delivery key per subscriber and channel, retry only failures that may recover, and make replay use the same consumer path as first delivery. If a page cannot name which boundary failed, the alert is probably describing a dashboard rather than an incident.

## How should a user reminder queue handle retries and dead letters?

A health shipment update might fan out to a patient, a caregiver, and a clinic, with email and webhook subscriptions carrying different delivery expectations. One carrier event can therefore create several independent intents. Combining them into one job makes partial success dangerous: an email may be accepted while a clinic webhook returns HTTP 429, and retrying the whole bundle risks another email.

The page should identify a user-visible risk and an actionable boundary. Queue depth alone does neither. A growing ready queue can be normal after a planned batch, while a flat queue can hide consumers that acknowledged work before recording delivery state. I would alert on the age of the oldest eligible intent against the notification objective, sustained exhaustion of retry budgets, and dead-letter arrivals grouped by failure class. The page should say which channel, which shipment-event cohort, and which transition stopped.

No mystery alerts.

HTTP 429 deserves its own handling because it means the recipient is rate limiting requests; the response may include `Retry-After`. A worker should honor a valid delay rather than hammer the endpoint with a locally convenient backoff. Timeouts and 5xx responses are ambiguous and may recover, so they can consume a bounded retry budget. A malformed destination, an invalid payload that will remain invalid, or a policy rejection should not spin through the same schedule. The exact classification belongs in a small, reviewed policy table, not scattered across worker branches.

| Outcome | State transition | Operator meaning |
| --- | --- | --- |
| Accepted | Mark the delivery complete | No retry for this delivery key |
| HTTP 429 | Reschedule, honoring valid `Retry-After` | Recipient is applying backpressure |
| Timeout or 5xx | Reschedule with bounded, jittered delay | Result is uncertain or dependency may recover |
| Permanent rejection | Move to dead letter with reason | Human or data correction is required |
| Retry budget exhausted | Move to dead letter, preserving history | Automation has stopped intentionally |

## Five boundaries define the queue

The first boundary is time. A reminder or shipment update scheduled for later must remain durable until it becomes eligible; visibility timeout is not a scheduling model. The second is ownership. Two workers may inspect the same eligible row, but only one should acquire it. PostgreSQL documents `FOR UPDATE ... SKIP LOCKED` and explicitly notes that its inconsistent view can be useful for multiple consumers accessing a queue-like table. That makes it a viable building block, provided the transaction that claims work is short and sending occurs outside the lock.

The third boundary is retry policy. Persist `next_attempt_at`, attempt count, and the last normalized failure class. Do not sleep inside a worker while holding ownership. Releasing capacity matters during downstream throttling, and a persisted schedule survives process restarts.

The fourth is idempotency. Use a stable key such as `shipment-event-id + subscriber-id + channel`; do not generate a new key on every attempt. Enforce uniqueness where delivery state is stored. This prevents two workers from creating two logical deliveries, but it cannot prove that an external email or webhook was not accepted during a lost response. When the receiver supports an idempotency key, send the stable key. Otherwise, accept that the final network boundary is ambiguous and design reconciliation around recorded evidence rather than promises of exactly-once delivery.

The fifth is quarantine. A dead-letter queue is not an attic. It needs the original intent, delivery key, attempt history, failure class, timestamps, and a controlled replay transition. Payloads may contain sensitive health or contact data, so store references or minimized fields where possible and apply the same access and retention controls used for the source record.

This is a real trade-off.

These boundaries also shape the selection decision. A broker, a database-backed queue, and a managed task service can all work if they provide delayed availability, atomic claiming, bounded redelivery, inspectable attempt metadata, and controlled replay. The useful comparison is whether those properties remain clear during an incident, including who owns retention and how a poisoned message is isolated. A database-backed queue has the advantage of atomic coordination with application rows and familiar query tools, but its limitation is contention and cleanup pressure on the primary database; it is a poor fit when notification traffic can overwhelm transactional work. A dedicated broker separates that load and may offer stronger delivery controls, at the cost of another stateful system and a consistency boundary between intent creation and publication. A managed task service removes some operational ownership, while service limits, retention policy, access controls, and export or replay mechanics become external constraints. Feature counts do not answer that.

Pick the boundary you can operate.

## Implement the smallest safe consumer

The consumer below keeps transport details behind interfaces. It reserves one due intent, creates or finds the stable delivery record, sends with the same key on every attempt, and records one of three outcomes. The storage methods must make each state change atomic; the remote call deliberately sits between short transactions, because no ordinary database transaction can include an arbitrary email service or subscriber webhook.

```go
package delivery

import (
	"context"
	"errors"
	"time"
)

type Intent struct {
	ID          string
	DeliveryKey string
	Channel     string
	Attempt     int
}

type Result struct {
	Retryable  bool
	RetryAfter time.Duration
	Reason     string
}

type Store interface {
	ClaimDue(context.Context, time.Time) (Intent, error)
	AlreadyDelivered(context.Context, string) (bool, error)
	MarkDelivered(context.Context, string, time.Time) error
	Reschedule(context.Context, string, int, time.Time, string) error
	DeadLetter(context.Context, string, string, time.Time) error
}

type Sender interface {
	Send(context.Context, Intent) (Result, error)
}

var ErrNoWork = errors.New("no eligible intent")

func ConsumeOne(ctx context.Context, store Store, sender Sender, now time.Time) error {
	intent, err := store.ClaimDue(ctx, now)
	if err != nil {
		return err
	}

	done, err := store.AlreadyDelivered(ctx, intent.DeliveryKey)
	if err != nil {
		return err
	}
	if done {
		return nil
	}

	result, sendErr := sender.Send(ctx, intent)
	if sendErr == nil {
		return store.MarkDelivered(ctx, intent.DeliveryKey, now)
	}

	nextAttempt := intent.Attempt + 1
	if result.Retryable && nextAttempt < 8 {
		delay := result.RetryAfter
		if delay <= 0 {
			delay = cappedBackoff(nextAttempt)
		}
		return store.Reschedule(ctx, intent.ID, nextAttempt, now.Add(delay), result.Reason)
	}

	return store.DeadLetter(ctx, intent.ID, result.Reason, now)
}

func cappedBackoff(attempt int) time.Duration {
	delay := time.Second << min(attempt, 8)
	return min(delay, 5*time.Minute)
}
```

Eight attempts and a five-minute cap are example policy values, not universal recommendations. Set them from the maximum tolerable notification age, downstream guidance, and the time responders need to intervene. Add jitter in production so a recovered endpoint does not receive a synchronized wave; keep the deterministic function in unit tests.

Policy is workload-specific.

There is a deliberate uncomfortable edge here. If `Send` succeeds and `MarkDelivered` fails, the intent will return. The stable key gives an idempotent receiver a chance to suppress the duplicate. For email transports without equivalent semantics, record the provider's acceptance identifier when available and route uncertain outcomes through reconciliation. Marking complete before the send merely trades duplicates for silent loss, which is the worse failure for a time-sensitive shipment update.

## Verify failure behavior before rollout

Test state transitions, not just the happy-path function. Start with one shipment event, three subscribers, and two channels, producing six distinct delivery keys. Then inject a timeout after remote acceptance, repeated 429 responses with a valid delay, a permanent payload rejection, a worker crash immediately after claim, and a database error after send. The assertions should cover eventual eligibility, unchanged delivery keys, attempt history, one terminal state per intent, and dead-letter evidence sufficient to replay or close the case.

Deploy with sending disabled first and inspect generated intents against source subscriptions. Next, enable a narrow cohort and compare intent counts, accepted deliveries, retries, and terminal failures as a conservation check. A dashboard is useful for exploration, but the release gate should be a query or automated check that can name missing and duplicated keys. Sample payload references as well; operational visibility should not become an uncontrolled copy of patient data.

For rollback, stop new claims while allowing in-flight calls to finish, then disable intent creation or return traffic to the previous consumer. Do not purge ready or dead-letter records. A schema change should remain backward-readable for at least the deployment window, because old workers and replay tools may overlap. Re-enable claims only after confirming that leases can expire safely and both versions derive the same delivery key.

The final drill is manual replay. Select a dead-lettered intent by reason, correct the underlying data or policy, move it back to scheduled state without changing its delivery key, and observe the ordinary consumer complete it. **If replay requires a special sender, the recovery path is untested production code.**

## Choose for the incident you need to resolve

The best queue is the one whose failure boundaries your team can prove under load and explain from a page at 3 a.m. Require durable scheduling, short atomic claims, visible retry state, stable idempotency keys, deliberate dead-letter transitions, and replay through the normal consumer. Then evaluate operational burden: database contention, broker partitioning, service limits, retention, access control, and the team's ability to inspect a single delivery without exposing unrelated data.

Write the postmortem test before adopting the mechanism: “A clinic endpoint throttled shipment updates; which page fired, which deliveries waited, which were ambiguous, and how were they replayed without changing identity?” If the design cannot answer those questions with stored state, a longer feature list will not rescue it.

## References

- [MDN: 429 Too Many Requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429)
- [PostgreSQL `SELECT` documentation](https://www.postgresql.org/docs/current/sql-select.html)
