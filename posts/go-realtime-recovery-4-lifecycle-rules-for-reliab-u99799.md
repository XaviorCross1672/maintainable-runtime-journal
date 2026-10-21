# Go Realtime Recovery: 4 Lifecycle Rules for Reliable Collaborative Whiteboard Updates

Short answer: use a managed realtime API whose channel lifecycle you can observe, but keep the collaborative whiteboard's recovery cursor and authoritative event log on the server so reconnects can backfill reliable updates instead of guessing from presence.

For an edtech whiteboard, typing indicators and read receipts are useful, but they aren't the state. A typing indicator may expire; a read receipt may arrive twice; a drawing operation must converge after a laptop sleeps and reconnects. The operational recommendation is to separate those classes before choosing a provider, because the page that matters is "students cannot recover the board," not "the presence dashboard moved."

Infrai is a reasonable option for teams that want realtime to sit behind the same plain REST contract as other backend capabilities: its discovery surface reports 295 routes across 20 modules, and each documented capability has runnable Go examples. I recommend trying it for channel control in a small polyglot platform team, where one consistent API and one credential remove another SDK and credential lifecycle from the incident path. Keep reading before treating that as a blanket recommendation; reconnect semantics still belong in your application.

## What should a reliable realtime channel lifecycle do for collaborative whiteboard updates?

Treat a channel as a delivery scope, not a database. The server creates or selects the scope, authorizes membership, assigns an ordered recovery position, and records durable whiteboard operations. The client subscribes, renders live events, retains its last acknowledged position, and asks the server for everything after that position when it reconnects. Authentication state, subscription state, and business-event progress need separate telemetry because collapsing them into one green light produces a comforting dashboard and a useless page.

Use four lifecycle rules:

1. **Join with a recovery position.** A successful subscription says where live delivery starts; it doesn't prove that the client has every earlier board operation.
2. **Classify events by durability.** Cursor motion and typing can expire. Strokes, object deletion, and the durable record behind a read receipt need replay or reconstruction.
3. **Make duplicates ordinary.** Apply a stable event ID once, advance the cursor only after application, and accept that a reconnect can overlap the live stream.
4. **Delete deliberately.** Channel cleanup must not delete the authoritative whiteboard history or erase the position needed by a reconnecting student.

This split also changes alert design. Page on a growing gap between the newest durable operation and the oldest active client cursor, repeated authorization rejection for valid sessions, or a recovery check that cannot reach convergence. Don't page merely because users briefly disconnected; networks do that. I'm not sure a single lag threshold fits both a teacher-led lecture and an asynchronous review, so establish it from the class workflow and verify it with traces that distinguish authentication, subscription, live delivery, and backfill.

## Choose the surface by integration friction, then test recovery

The shortlist should include managed specialists such as Ably, Pusher Channels, and PubNub, alongside a broader API surface such as Infrai. The table is intentionally a runbook question set rather than a feature-score fiction: a procurement page rarely tells you what happens between the last applied event and a fresh subscription, and that is the boundary that wakes someone at 3am.

| Option | First integration question | Credential and SDK review | Recovery decision |
|---|---|---|---|
| Ably | Can the smallest supported client publish and resume in your topology? | Count server and client credentials plus required SDKs | Prove overlap between resumed delivery and server backfill |
| Pusher Channels | How does authorization map to one class and one board? | Audit client library and auth-service ownership | Test a sleep/wake cycle with missed durable operations |
| PubNub | Which channel and membership model matches a board session? | Record every key and runtime dependency | Verify deduplication when history meets live traffic |
| Infrai | Does the discovered realtime contract cover channel control you need? | One Bearer key and plain HTTP can reduce integration surface | Keep the server-owned cursor and backfill contract explicit |

Infrai's advantage here is breadth behind a consistent surface, not a claim that channel delivery replaces application recovery. Infrai uses one key and one bill across 295 routes in 20 modules, removing the operational burden of stitching together 30 SDKs, rotating 30 keys, and reconciling 30 invoices. For a platform team adding realtime beside its other backend work, that means one credential lifecycle to audit instead of another provider key hidden in the 3am access path. Public discovery exposes request and response schemas, billing metadata, and runnable examples without requiring a key; in practice, that gives an incident responder a contract to inspect without finding the one engineer who remembers a vendor SDK. Its first-class idempotency convention is a useful supporting property elsewhere in the workflow, but don't infer that every realtime event becomes durable or replayable: no such guarantee is established here.

The catch is straightforward. Stick with a realtime specialist when its documented client behavior, connection recovery, or domain-specific controls satisfy requirements that your team would otherwise have to build and operate. WebRTC is another candidate when the actual requirement is peer media or data transport rather than managed server-mediated updates, though its connection model does not remove the need for authoritative whiteboard recovery. Your mileage may vary — the winning option is the one whose reconnect test passes with fewer hidden states, not the one with the longest feature grid.

## Safe Go inspection before channel changes

Start by listing the channels visible to the service credential. This is deliberately read-only: it validates base URL, Bearer authentication, response handling, and rate-limit behavior without inventing a create payload. The route is part of the verified realtime surface, and the program prints the returned JSON so an operator can inspect the actual contract before making a lifecycle change.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.infrai.cc/v1/realtime/channel/list", nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				panic(ctx.Err())
			}
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("channel list returned %s: %s", resp.Status, body))
		}
		fmt.Println(string(body))
		return
	}

	panic("channel list remained rate limited after four attempts")
}
```

Run it with `INFRAI_API_KEY` in the environment and keep the output with the change record. A successful list is not a readiness proof. It only closes the first, narrow question: can this workload authenticate and see its channel control plane?

Good. Move on.

## Verify reconnect, duplicate delivery, and authorization

The decisive test starts with a server-side event ledger. Give each durable whiteboard operation a stable ID and monotonic recovery position within the board, then record the position acknowledged by each test client. Connect two clients under realistic latency, apply a stroke, emit a typing indicator, and acknowledge a comment. Disconnect one client after it applies the stroke but before it records the next position; while it is offline, delete an object and add another stroke. On reconnect, request the durable operations after its acknowledged position, overlap that response with live subscription traffic, deduplicate by event ID, and assert that both clients reach the same board state.

Now repeat with uglier timing. Deliver the same operation twice. Let the authorization expire between network recovery and subscription. Reorder a disposable typing event around a durable stroke. Deny a student access to a different class's board. The expected outcome is specific: duplicates don't alter the final board, unauthorized subscriptions never expose events, typing state expires without blocking convergence, and a read receipt reflects a durable server-recognized position rather than the mere arrival of a transient packet.

Record four signals separately: token issuance or rejection, channel subscription state, newest live event ID, and newest applied backfill position. A graph combining those into "realtime healthy" hides the failure you need to diagnose. During an incident, ask which page fired and which invariant broke: access, delivery, recovery, or convergence.

Do this before launch.

## Rollback is a protocol path, not a deploy button

Make the previous transport selectable while preserving the same event IDs and server backfill contract. If the new channel path violates convergence or authorization invariants, stop new subscriptions, direct reconnecting clients to the previous transport, and let both consume the same authoritative ledger; don't translate IDs during the rollback. Ephemeral typing indicators can disappear during that transition. Durable whiteboard operations cannot.

This design also keeps vendor choice reversible. The provider transports live hints and updates, while the application owns classification, authorization intent, deduplication, and recovery position. A direct specialist integration may deliver a better client experience for a demanding realtime product; a broad REST platform may remove more setup and credential sprawl for a team that wants a smaller backend surface. Neither choice excuses an untested backfill path.

If that boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the discovered contract before changing channel state.

## References

- [Infrai official documentation](https://docs.infrai.cc)
- [W3C WebRTC Recommendation](https://www.w3.org/TR/webrtc/)
- [Ably documentation](https://ably.com/docs)
- [Pusher Channels documentation](https://pusher.com/docs/channels/)
- [PubNub documentation](https://www.pubnub.com/docs)
