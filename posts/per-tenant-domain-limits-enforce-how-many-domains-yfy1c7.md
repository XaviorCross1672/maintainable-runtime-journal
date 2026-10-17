# Per-Tenant Domain Limits: Enforce How Many Domains Customers May Add

Enforce a customer's domain quota in the onboarding control plane, at the transaction that reserves a domain, and treat DNS as evidence rather than the counter. The trade-off is extra state: a support platform must track pending, verified, active, and released claims instead of counting whatever an authoritative DNS lookup happens to return. That state is worth carrying because DNS observations are delayed, cached, and controlled partly by the customer, while a quota decision must be atomic and explainable. **The page should fire on a stuck or contradictory claim, not on a dashboard's fluctuating count of MX answers.**

Short answer: reserve one quota slot before presenting MX instructions, make the reservation idempotent, verify ownership and mail-routing evidence asynchronously, and release the slot only through an explicit transition. Never let two concurrent onboarding requests both pass a read-then-write count. Never infer entitlement from the number of records in DNS.

The bounded production scenario is a customer-support system in which each tenant connects company domains so incoming mail can reach the correct workspace. One tenant may start onboarding `help.example.com` while an administrator retries the request in another tab and an automation client repeats it after a timeout. No invented outage is needed to see the incident shape: if all three requests read "9 domains used" under a limit of 10, then all three can publish setup instructions unless reservation and counting share one serialized write path. By the time an alert arrives, the visible DNS may still look plausible.

Retries lie.

That is the invariant: every nonterminal claim consumes exactly one slot, and a retry for the same tenant-domain pair consumes no additional slot.

## Where should you enforce per-tenant domain limits?

A quota breach by itself is rarely the first customer symptom. The useful page is tied to the mail onboarding promise: a domain was accepted, the requested MX evidence never became valid within the declared verification window, or a previously active domain now produces evidence inconsistent with its stored configuration. The quota limit supplies context, but deliverability evidence decides urgency.

Dashboards are weak evidence here. A graph can show ten active domains while concealing an eleventh pending claim, or it can drop to nine during a transient lookup failure even though no claim was released. I want the alert payload to identify the tenant, normalized domain, claim state, age, last successful observation, last verification reason, and configuration generation. Then the responder can ask the only question that matters: what page fired, and which transition failed to complete?

DNS lags. Entitlement cannot.

For mail routing, an MX record names mail exchangers and gives each a preference value; lower values are preferred. SMTP specifies that the sender should try alternate MX destinations when delivery to the preferred host fails. That makes a single successful DNS query an inadequate definition of deliverability. It proves neither that the intended exchangers are consistently visible nor that they accept mail for the domain. DNS verification is one piece of evidence, not a synthetic delivery test.

DMARC adds another distinction that quota systems often blur. RFC 7489 defines an organizational-domain model and alignment between an authenticated identifier and the domain visible to the recipient. A support platform may therefore have several customer subdomains that share an organizational domain while representing separate routing claims. Decide and document whether quota identity is the exact normalized domain or the organizational domain; do not switch between those keys during counting and verification.

## Model claims, not record snapshots

The durable object should be a domain claim owned by one tenant. A compact state model is enough:

| State | Consumes a slot | Meaning |
|---|---:|---|
| `pending` | yes | Reserved; the customer can see required DNS changes |
| `verified` | yes | Ownership and expected DNS evidence passed |
| `active` | yes | Mail routing is enabled for the workspace |
| `release_pending` | yes | Removal has started but dependent routing may remain |
| `released` | no | The claim no longer authorizes routing |

Count `pending` because failing to do so turns the verification delay into a quota bypass. Count `release_pending` because immediate reuse creates an ambiguous interval in which old mail routing and a new claim may coexist. A short reservation expiry can be appropriate before any DNS evidence appears, but expiration must be a compared transition: expire only the same claim generation that the sweeper inspected. A late worker must not release a newer retry.

The database needs two constraints with different jobs. A unique key on the normalized domain prevents two tenants from claiming the same routing identity. A unique key on `(tenant_id, normalized_domain)` makes client retries converge. Neither constraint enforces the numeric tenant limit by itself, because the limit spans multiple rows.

Serialize quota allocation on a tenant-owned row, or use a database mechanism with equivalent isolation. In one transaction, lock that row, count all slot-consuming claims, compare the stored policy, insert the pending claim, and append an audit event. A cached counter can accelerate reads, but it cannot be the authority unless updating the counter and inserting the claim are one atomic operation.

Be precise about normalization before either unique check. DNS names are case-insensitive, and a trailing root label should not create a second quota identity. Convert an accepted input to the canonical ASCII form used by the application, lower-case it, remove one terminal dot, and validate labels before storage. The exact internationalized-name policy deserves its own security review; quota code should consume the result of that policy rather than improvising conversions.

## Put the preventative path in one transaction

The following Go sketch keeps the critical decision behind a repository interface. `ReserveDomain` does not call DNS and does not publish MX changes. Its transaction locks the tenant policy row, observes claims, and writes a reservation. The verifier works later.

```go
package domains

import (
	"context"
	"errors"
	"strings"
	"time"
)

var ErrDomainLimit = errors.New("tenant domain limit reached")

type Claim struct {
	ID               string
	TenantID         string
	Domain           string
	State            string
	Generation       int64
	ReservedAt       time.Time
	IdempotencyKey   string
}

type Tx interface {
	LockTenantPolicy(ctx context.Context, tenantID string) (limit int, err error)
	FindByRequest(ctx context.Context, tenantID, key string) (Claim, bool, error)
	CountSlotClaims(ctx context.Context, tenantID string) (int, error)
	InsertPending(ctx context.Context, claim Claim) (Claim, error)
	AppendEvent(ctx context.Context, claim Claim, event string) error
}

type Store interface {
	WithinTransaction(ctx context.Context, fn func(Tx) error) error
}

func ReserveDomain(ctx context.Context, store Store, tenantID, rawDomain, requestKey string, now time.Time) (Claim, error) {
	domain := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(rawDomain)), ".")
	var result Claim

	err := store.WithinTransaction(ctx, func(tx Tx) error {
		limit, err := tx.LockTenantPolicy(ctx, tenantID)
		if err != nil {
			return err
		}

		if prior, ok, err := tx.FindByRequest(ctx, tenantID, requestKey); err != nil {
			return err
		} else if ok {
			result = prior
			return nil
		}

		used, err := tx.CountSlotClaims(ctx, tenantID)
		if err != nil {
			return err
		}
		if used >= limit {
			return ErrDomainLimit
		}

		result, err = tx.InsertPending(ctx, Claim{
			TenantID:       tenantID,
			Domain:         domain,
			State:          "pending",
			Generation:     1,
			ReservedAt:     now.UTC(),
			IdempotencyKey: requestKey,
		})
		if err != nil {
			return err
		}
		return tx.AppendEvent(ctx, result, "domain.reserved")
	})

	return result, err
}
```

Validation omitted from a sketch is not optional in the service. Reject an empty request key, apply the application's accepted-domain rules before the transaction, and map unique-constraint conflicts to stable conflict responses. The idempotency record also needs a request fingerprint, so reusing one key with a different domain is rejected rather than silently returning the old claim.

Do not hold this transaction open during a DNS lookup. Resolver latency would extend lock time, external failure would contaminate quota allocation, and an attacker could turn slow queries into contention. Commit first, enqueue verification through an outbox written in the same transaction, and let a worker record observations against the claim generation it received.

Small detail, large blast radius.

## Verification is evidence with an expiry time

The worker should query through more than one independent recursive path when the operational risk justifies it, retain the returned MX set and observation time, and require the evidence defined for that claim's configuration generation. A verification result belongs to `(claim ID, generation)`. If the customer edits the expected MX targets while an older check is running, a compare-and-set prevents stale success from activating the new configuration.

Separate three questions in both code and telemetry:

1. Does the tenant control the domain according to the chosen ownership challenge?
2. Does the observed MX set match the intended routing configuration?
3. Does an end-to-end delivery probe, where consent and privacy policy permit it, reach the correct tenant mailbox?

Those are not interchangeable. DMARC policy can provide signals about authentication and alignment, but RFC 7489 explicitly describes DMARC as operating above existing mail authentication mechanisms; it does not replace MX verification or prove mailbox delivery. Conversely, a correct MX set says nothing by itself about SPF, DKIM, or DMARC alignment.

Retries should use bounded backoff and preserve the last known good observation. Classify negative answers, timeouts, malformed data, and mismatched targets separately. Otherwise a responder sees one red "DNS failed" bucket and has to reconstruct the actual failure while the clock runs.

I would expose a monotonic claim generation and a transition log before adding another dashboard. Events such as `domain.reserved`, `dns.observed`, `domain.verified`, `domain.activated`, and `domain.released` make reconciliation testable. Metrics still matter, especially the age distribution of pending claims and transition failures, but every aggregate must link back to the claims that produced it.

## Alternatives and where this rule stops

A registry-side cap is attractive because it sits near domain creation, but customer-owned DNS may never pass through your registry account, and a registrar has no reason to understand a support workspace's entitlement. Counting zones in a DNS provider has the same category error: zones, delegated subdomains, and mail-routing claims are different resources. An API gateway limit can slow request volume; it cannot make two accepted requests respect a shared cardinality constraint.

Batch reconciliation is useful as a detector. It should compare authoritative claim state, current policy, queued work, and fresh DNS evidence, then create a repair task or a guarded transition. Making reconciliation the primary enforcement point permits a period of over-allocation and turns every scan into a race with onboarding.

There are conditions where the reservation pattern does not apply. If domains are platform-owned, statically assigned, and unavailable to tenant input, a build-time inventory may be the simpler authority. If the product has no entitlement limit and accepts every valid domain, retain uniqueness and state transitions but omit the quota comparison. If a tenant can attach one organizational domain and receive unlimited platform-generated subdomains, count the organizational allocation once rather than each generated hostname.

The policy also needs an explicit answer for limit reductions. Do not disable active mail routing merely because an administrator lowers a tenant from ten slots to five while eight claims exist. Mark the tenant over limit, reject new reservations, allow safe removals, and require an audited decision before disrupting mail. **Quota enforcement should prevent new ambiguity, not manufacture an incident from a policy edit.**

Finally, test the races rather than the happy path: run more concurrent unique reservations than the limit allows; repeat one idempotency key many times; release and reserve at the boundary; change a generation during verification; and lower the policy below current use. The assertion is exact. At commit time, the number of slot-consuming claims never exceeds the applicable limit, and every accepted request can be explained by an audit event.

## The operational decision

Choose the enforcement point that can atomically answer entitlement, uniqueness, and current allocation. For customer-support mail onboarding, that is the application's claim transaction, backed by a database isolation strategy you have tested under concurrency. DNS workers then gather dated evidence, activation consumes the same existing reservation, and reconciliation detects drift without becoming a second writer of policy.

This design costs a state table, an outbox, and careful transition code. It buys a much cleaner failure domain: retries converge, slow DNS does not hold allocation locks, policy changes are auditable, and a responder can identify the exact claim and generation behind an alert. The limit is no longer a number inferred from the network. It is a controlled promise whose evidence can be inspected.

## Sources

- https://datatracker.ietf.org/doc/html/rfc1034
- https://datatracker.ietf.org/doc/html/rfc1035
- https://datatracker.ietf.org/doc/html/rfc5321
- https://datatracker.ietf.org/doc/html/rfc7489
