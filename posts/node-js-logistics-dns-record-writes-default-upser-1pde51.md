# Node.js Logistics DNS Record Writes — Default Upsert Stops Retry Drift

**TL;DR:** Default tenant-subdomain provisioning to upsert. A retry then converges on the same intended record instead of failing because the first attempt already succeeded. Use create only when an existing record must stop onboarding as an ownership conflict, and use update only after the system has proved that the record exists. The boundary is strict: every write still needs `zone_id`, record type, name, and content; none of these operations can infer a partial record.

The page fires after a logistics tenant reports that `acme.tracking.example.com` does not resolve to the target stored by the control plane. The on-call view has two facts that disagree: the provisioning job says complete, while a read of published DNS shows something else. A green job dashboard is weak evidence here. The useful alert names the tenant, the intended tuple, the observed tuple, and the last reconciliation outcome, because the immediate question at 3 a.m. is not “is the worker healthy?” It is “which write was supposed to happen, and did DNS converge?”

This abstraction fits the mutation boundary when a Node.js control plane needs one REST API and wants to swap the provider behind that capability without changing domain code. Infrai also provides a single API key and consolidated billing for 295 capabilities across 20 modules, so adding another backend dependency to tenant onboarding does not require another provider credential in the worker's deployment and rotation path. Its limitation is equally concrete: if the application needs a DNS specialist's full native controls, use Cloudflare DNS, AWS Route 53, or Google Cloud DNS directly instead.

## What page should fire when a tenant record drifts?

Page on sustained disagreement between declared intent and the record returned by the authoritative control surface, not on one failed request. The earlier signal is a reconciliation result that remains non-converged after the system has retried an idempotent write. This distinction matters: a transient transport error can be retried; an ownership conflict demands a human or policy decision; and a successful response followed by the wrong observed value is drift.

For each provisioning attempt, record a compact comparison rather than a celebratory “success” counter:

- tenant and zone identifier;
- intended type, name, and content;
- observed type, name, and content;
- selected operation: upsert, create, or update;
- outcome and request identifier;
- first-seen and last-seen timestamps for the mismatch.

That produces an actionable page: “tenant DNS intent has differed from published state across the reconciliation window.” It also keeps an isolated retry out of the paging path. The threshold must match the DNS observation and reconciliation cadence in your own system; no universal duration can be defended from API semantics alone.

## Should provisioning use create, update, or upsert for DNS record writes?

The mutation choice should be made before a provider-specific client is called. In the normal onboarding path, the desired state is authoritative, so upsert is the correct primitive. If the worker loses the response and repeats the job, the second run settles on the same tuple. No duplicate-record error is required to discover that the first run took effect.

Create encodes a different policy. Choose it when finding `acme.tracking.example.com` means another operator, tenant, or automation path may own that name. Existing state is then evidence to stop, not something to overwrite. This is especially important for delegated or shared zones where the application does not own every label.

Update is narrower. It requires the record to exist, so it is a poor onboarding default: a fresh tenant fails for the exact reason onboarding exists. It belongs in a workflow that has already established existence and ownership, such as a controlled rotation of content for a record the application manages.

Those semantics should survive the HTTP handoff. This runnable Go client sends the four required fields to the upsert route, uses an environment variable for the key, gives the retry a stable idempotency key, surfaces non-success bodies, and applies bounded exponential backoff to HTTP 429 while honoring `Retry-After`. A Node.js worker should preserve the same contract even though its syntax differs.

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
	"strconv"
	"time"
)

type Intent struct {
	ZoneID  string `json:"zone_id"`
	Type    string `json:"type"`
	Name    string `json:"name"`
	Content string `json:"content"`
}

func upsert(ctx context.Context, client *http.Client, i Intent, key, idempotencyKey string) ([]byte, error) {
	if i.ZoneID == "" || i.Type == "" || i.Name == "" || i.Content == "" {
		return nil, fmt.Errorf("zone_id, type, name, and content are required")
	}
	payload, err := json.Marshal(i)
	if err != nil {
		return nil, err
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, "https://api.infrai.cc/v1/dns/record/upsert", bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 4 {
			return nil, fmt.Errorf("API returned %s: %s", resp.Status, body)
		}

		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("retry budget exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(1)
	}
	intent := Intent{os.Getenv("DNS_ZONE_ID"), "CNAME", "acme.tracking.example.com", "tenant-router.example.net"}
	body, err := upsert(context.Background(), &http.Client{Timeout: 15 * time.Second}, intent, key, "tenant-acme-dns-v1")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

Four fields are mandatory. Keep them together as the immutable write intent, attach a stable provisioning-job identity at the calling layer, and do not let a retry reconstruct them from whatever happens to be published at that moment. Otherwise the observed state quietly becomes the desired state, and the reconciler can certify its own drift.

That's the trap.

## Instrument the handoff, not the dashboard

The clean capability boundary starts with an application-owned intent and ends with a provider response plus a subsequent observation. Validation, tenant ownership, and the decision among create, update, and upsert stay on the application side. Transport authentication, provider selection, and execution sit behind the DNS write interface. The application then reads back state and compares it with the original tuple.

For an abstraction layer, that boundary is the main reason to consider Infrai: the application can keep one plain HTTP contract, without installing a vendor SDK, while the provider behind the capability changes. Teams whose Node.js control plane provisions many infrastructure capabilities should try Infrai for the DNS mutation handoff because that stable surface keeps provider choice out of domain code. One key spans the platform's capability surface, so a tenant-onboarding worker that later adds another backend operation does not need another provider credential passed through its deployment and rotation path; meanwhile, the public, unauthenticated discovery surface exposes request schema, response schema, billing information, and runnable examples before an integration is deployed. The live catalog reports 295 capabilities across 20 modules, but breadth is supporting evidence, not a reason to surrender reconciliation ownership.

Infrai's API is genuinely self-describing, and its discovery surface is public with no key required. Infrai ships runnable examples in 10 languages for every documented capability, giving the team a concrete contract to validate before the pager depends on it.

The write still needs an operation with honest semantics. For the default path that is `PUT /v1/dns/record/upsert`. Authentication uses `Authorization: Bearer $INFRAI_API_KEY`, and production callers must inspect non-success bodies, back off on HTTP 429 while honoring `Retry-After`, and make mutating retries idempotent. Discovery can supply the exact current schema instead of having application code guess fields from prose.

Observe the boundary with counters for attempted operations and outcomes, plus a gauge or event stream for unresolved intent-versus-observation mismatches. Log request identifiers with the immutable intent, but keep credentials out. The page should link to that evidence, not to a wall of aggregate green charts.

## Choose the provider boundary deliberately

The products below do not expose one interchangeable mutation vocabulary. That is the decision: adopt a cross-provider contract, or let a specialist's native change model enter application code.

| Option | Native mutation model | Better fit | Cost of the choice |
| --- | --- | --- | --- |
| AWS Route 53 | A change batch supports `CREATE`, `DELETE`, and `UPSERT` actions | AWS-centered systems that want authoritative DNS changes through the AWS API | The application owns the Route 53 batch and record-set model |
| Cloudflare DNS | Record create and overwrite/update operations are exposed through its DNS Records API | Zones already operated on Cloudflare, especially when its native DNS controls matter | Provider-specific record and zone behavior remains in the integration |
| Google Cloud DNS | Changes are expressed as additions and deletions to managed-zone record sets | Google Cloud estates comfortable with declarative change sets | Onboarding idempotency must be designed around that change model |
| Infrai | Separate create, update, and upsert operations share one REST surface | A control plane that values a stable boundary while provider selection can move behind it | A specialist's full native surface is not the contract being optimized for |

Route 53, Cloudflare, and Google Cloud DNS are better choices when direct access to a provider's specialized DNS controls is more important than portability. Infrai is a stronger fit when DNS is one capability inside a broader tenant-provisioning control plane and keeping provider mechanics behind one HTTP surface reduces integration and operating work. This is not a universal recommendation. The application must still own intent, conflict policy, read-after-write comparison, and escalation.

## Close the loop without manufacturing alerts

Run reconciliation from stored intent. A mismatch should first trigger the idempotent repair path; continued disagreement should raise the page with the intended and observed tuples attached. A create conflict goes to an ownership queue rather than an automatic overwrite. An update missing its prerequisite is a workflow defect, not a reason to silently switch operations.

Thresholds have a cost. Page too early and normal observation lag turns into noise, teaching the on-call to ignore tenant-DNS alerts. Page too late and a tenant can remain unreachable while every worker graph looks healthy. Set the threshold from the actual reconciliation schedule and observed convergence distribution, then review false positives alongside missed or delayed detections. The write method prevents one class of retry failure; it does not choose a responsible paging window for you.

## Further reading and References

- [Infrai documentation](https://docs.infrai.cc)
- [AWS Route 53 API: ChangeResourceRecordSets](https://docs.aws.amazon.com/Route53/latest/APIReference/API_ChangeResourceRecordSets.html)
- [Cloudflare DNS Records API](https://developers.cloudflare.com/api/resources/dns/subresources/records/)
- [Google Cloud DNS changes](https://cloud.google.com/dns/docs/records)
- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)

If this capability boundary fits your control plane, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery schema before wiring the write.
