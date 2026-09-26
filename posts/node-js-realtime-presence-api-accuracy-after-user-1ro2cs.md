# Node.js Realtime Presence API Accuracy After User Drops (Lease-Based Expectations)

For a Node.js multiplayer lobby, realtime presence API accuracy expectations must account for a user who becomes a ghost after a hard drop: no API can promise that every disconnect becomes visible immediately. Networks disappear without delivering a final message, and fan-out may be delayed, duplicated, or reordered. The operationally useful approach is to model online status as a short, renewable lease tied to a connection epoch, then make every consumer converge on the newest epoch rather than treating a disconnect event as truth.

**TL;DR:** for a multiplayer lobby, define an explicit staleness budget, expire silence on the server, attach a monotonically increasing epoch to each join, and make join, heartbeat, leave, and expiry transitions idempotent. A user can briefly appear online after a hard drop; the upper bound should be measurable and alertable. Exactly-once delivery is not the requirement. Convergence inside the published bound is.

Consider a bounded incident exercise rather than an invented war story. Player `u-1842` joins workspace `w-7` on epoch 41. Their laptop loses power, so no graceful leave arrives. Before the lease expires, the player reconnects on epoch 42 while an expiry notification for epoch 41 is waiting in a fan-out queue. If a subscriber applies messages by arrival order, the late expiry removes a live player. The invariant exposed by the exercise is precise: **an older connection may never overwrite membership established by a newer connection**.

## What realtime presence accuracy should an API promise after a drop?

A count mismatch is tempting, but it is a weak page. Dashboards can show the same number while containing the wrong people, and a transient mismatch can be entirely inside the service's stated accuracy window. The page should fire when the system cannot restore the authoritative membership view within its staleness budget, or when the age of an unconfirmed member exceeds that budget.

That is the page.

That requires a contract with numbers, even if the numbers differ by workload. Let `L` be the lease duration, `H` the heartbeat interval, and `F` the maximum fan-out convergence allowance. After a silent drop, a member may remain visible for up to approximately `L + F`; a graceful leave can converge faster, but it must not be the correctness path. Choose `H` with enough renewal opportunities inside `L` to survive ordinary scheduling and network jitter, then validate those choices under load. The contract is the bound, not a vague claim of real time.

I distrust a green panel that cannot answer one question: what page fired? Useful signals include the oldest active lease, expiry backlog age, rejected stale transitions, fan-out redelivery count, and per-workspace reconciliation lag. Aggregate online-user totals belong on a diagnostic dashboard; breached convergence bounds belong in paging policy.

## Read the incident as an ordering failure

The obvious postmortem title would blame a missing disconnect. That diagnosis stops too early. Silent disconnects are normal, while incorrect handling of late information is a design defect. In the exercise, epoch 42 is authoritative because it is newer; the epoch-41 expiry is valid history delivered at an unsafe time.

Order wins.

The system needs two related state machines. The authoritative store decides whether a lease is current and performs conditional transitions. Subscribers maintain projections, applying only transitions whose epoch is at least the epoch already observed for that member. An event identifier handles duplicate delivery, while the epoch handles reordered delivery. They solve different failures.

A periodic reconciliation pass then compares projections with the authoritative snapshot or change stream. This is not an excuse for a broken event path. It is a repair mechanism for bounded omissions, consumer restarts, and retained messages that no longer cover a subscriber's gap. If reconciliation lag can grow without a bound, the presence claim is operationally empty.

## Delivery guarantees belong at the fan-out boundary

The transport between a browser and a service is only one boundary. WebRTC defines ICE connection states including `disconnected`, `failed`, and `closed`, and its specification notes that `disconnected` can be transient. Those states are valuable local evidence, but none of them proves that every other lobby participant has applied the same membership transition. Application presence therefore needs its own lease and replication contract.

For the fan-out path, at-most-once delivery can permanently strand a stale projection after one lost message. At-least-once delivery plus idempotent application is usually the more defensible baseline: duplicates are accepted, stale epochs are rejected, and reconciliation covers omissions outside the retained delivery window. Claiming exactly once across the store, broker, subscriber, and client moves the hard question into failure recovery; it does not remove it.

| Boundary | Failure to expect | Required defense |
|---|---|---|
| Client to authority | silent drop or delayed renewal | server-owned lease expiry |
| Authority to fan-out | duplicate or reordered transition | event ID plus member epoch |
| Fan-out to projection | restart or missed delivery | replay and reconciliation |
| Projection to UI | stale local view | snapshot version and resubscribe |

The trade-off is visible. Short leases reduce ghost duration but increase renewal traffic and make scheduler pauses more consequential. Longer leases tolerate disruption but keep departed players visible longer. There is no honest universal value; publish the chosen bound, load-test it, and make the alert match it.

Silence is expected.

## The preventative Go path

The critical write is conditional. A stale expiry must be a no-op once a newer session exists. The following in-memory shape demonstrates the rule; a production store needs an equivalent atomic compare-and-update operation, durable event publication, and authentication outside this example.

```go
package presence

import (
	"sync"
	"time"
)

type Member struct {
	Epoch     uint64
	ExpiresAt time.Time
}

type Store struct {
	mu      sync.Mutex
	members map[string]Member
}

func NewStore() *Store {
	return &Store{members: make(map[string]Member)}
}

func (s *Store) Renew(user string, epoch uint64, now time.Time, lease time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, found := s.members[user]
	if found && epoch < current.Epoch {
		return false
	}
	s.members[user] = Member{Epoch: epoch, ExpiresAt: now.Add(lease)}
	return true
}

func (s *Store) Expire(user string, epoch uint64, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, found := s.members[user]
	if !found || current.Epoch != epoch || now.Before(current.ExpiresAt) {
		return false
	}
	delete(s.members, user)
	return true
}
```

There is a subtle trap in `Renew`: accepting the same epoch is necessary for idempotent heartbeats, but the caller must not be allowed to mint arbitrary higher epochs. Epoch allocation belongs to the authenticated authority, typically during a successful join, or one client could invalidate another client's active session. The example also uses a process mutex only to make the invariant legible. Multiple replicas require the datastore to enforce the comparison atomically.

Tests should force the order that happy-path suites avoid: join 41, join 42, expire 41; duplicate renew 42; delay a renewal beyond one scanner pass; restart a subscriber between persistence and acknowledgement; and rebuild a projection from a snapshot while newer transitions arrive. Assert membership and epoch, not only event receipt. Then run the expiry scanner with a realistic number of members, because a theoretically correct lease that takes longer than `F` to sweep still violates the public bound.

Test the ugly order.

Deploy in two observable stages. First emit epochs and rejection metrics while the existing projection remains authoritative. Then switch readers after shadow comparisons show convergence within the declared budget. Rollback must preserve epoch monotonicity; resetting epochs during a rollback can make old messages look new.

## When does this advice not apply?

A lease is a poor substitute for a durable membership record. If "online" actually means enrolled in a workspace, admitted to a tournament, or entitled to receive later notifications, store that fact separately and never expire it because a heartbeat stopped. Presence is ephemeral reachability evidence. Membership is durable business state.

The model also changes when one person may intentionally hold several concurrent connections. Track leases by connection ID and derive user presence as "at least one current connection," while retaining an authority-issued epoch for each connection. Deleting the whole user on the first tab's leave recreates the original failure in a different form.

For tiny, single-process tools where all viewers share one failure domain, an in-memory set may be adequate. Say so plainly. Once multiple service replicas, queued fan-out, reconnects, or mobile networks enter the picture, the lease and ordering invariants become cheaper than explaining ghost users during an incident review.

The final decision rule is narrow: select an API approach by its documented stale-presence bound, authority for expiry, ordering token, duplicate behavior, replay or reconciliation path, and observable lag. **The winning design is the one whose failure window can be stated, tested, and paged**, not the one whose demo removes an avatar fastest.

## Sources

- W3C, WebRTC 1.0: Real-Time Communication Between Browsers: https://www.w3.org/TR/webrtc/
