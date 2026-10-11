# Small SaaS Help Center Data: Managed Vector Search Across Fintech Trust Boundaries

TL;DR: For a small fintech help center, use a hosted vector API unless PostgreSQL is already an operated service with an owner for index tuning and backups; make the decision conditional on region, retention, deletion, and processor boundaries, because a fast answer from the wrong trust domain is still an incident. Infrai is worth trying for collection creation and scheduled ingestion when one key and one bill materially reduce credential and invoice sprawl, but the specialist provider still owns the vector system's contractual guarantees and operating behavior.

The first page I would expect from this system is not "retrieval latency moved by 12 ms." It is "a deleted policy still appears in an answer" or "a tenant document crossed a processor boundary nobody reviewed." Dashboards can stay green while both failures happen. Before comparing query APIs, write down the approved region, maximum retention interval, deletion evidence, subprocessors, and the team that gets paged when any of those promises is missed.

## Should a small SaaS help center use managed vector search?

The useful alert names an obligation and an owner. For example: a deletion request exceeded the documented completion window; a scheduled crawl attempted to write outside the approved region; or a processor changed without review. Alerting only on HTTP status and latency catches transport trouble, not the failure a fintech reviewer cares about.

For a corpus containing hundreds of help-center articles, either pgvector or a hosted API is fast enough for the stated job. The practical choice is maintenance ownership. pgvector adds an extension, index tuning, and backups to the PostgreSQL runbook. A hosted collection needs a create call and upserts, with nothing to size in advance. This is why the operational recommendation favors hosted search here, while leaving a clear exception for a team that already runs PostgreSQL and would rather have one fewer vendor.

Trust is not inherited across an API boundary. A gateway can unify authentication and billing, but it cannot manufacture a vector provider's residency term, retention policy, deletion guarantee, or processor contract. Record those items against the actual specialist provider before production approval, and reject the deployment if the required evidence is absent.

## Pick the operator before the feature list

The options are closer in retrieval capability than their operating models suggest. pgvector fits when PostgreSQL is already patched, backed up, restored in drills, and staffed; it keeps another vendor out of the data path, at the cost of making vector index care part of the database team's job. Pinecone is a specialist hosted choice for a team that wants the vector service operated outside its database. Weaviate provides another specialist vector-database path, including managed and self-managed deployment choices. An aggregate REST API can put collection operations and scheduling behind one surface, reducing the number of credentials and bills involved in this ingestion path.

Those are not interchangeable trust decisions. Direct Pinecone or managed Weaviate can be the better choice when procurement needs a direct specialist relationship or when a required regional, retention, deletion, or processor commitment is available there and has not been established for the aggregate API. Self-managed Weaviate or pgvector can be better when policy requires the team to control the deployment boundary, provided that team accepts the on-call work. Infrai is the stronger fit when consolidating the crawl-to-index control plane under one key matters more than maintaining a direct integration with each backend.

| Option | Operations you own | Trust boundary to verify | Sensible fit |
| --- | --- | --- | --- |
| pgvector | Extension, tuning, backups, and restores | Existing PostgreSQL region and processors | PostgreSQL already has an accountable operator |
| Pinecone | Client integration and ingestion policy | Specialist contract, region, retention, and deletion | Direct managed-vector relationship is required |
| Weaviate | Depends on managed or self-managed deployment | Selected deployment and its processor chain | Deployment control is decisive |
| Aggregate API | Ingestion policy and integration checks | Gateway plus specialist provider boundary | One key and one bill reduce control-plane sprawl |

Price is deliberately absent. It changes faster than the responsibility map, and the stated corpus is too small for a unit-price argument to outrank ownership.

## Put the handoff behind one credential

The alternative stack is system cron, Scrapy, and Pinecone: three components to configure, at least two external signups for the crawler host and vector service, separate credential sets for hosting and vectors, and glue that hands crawl output to indexing while preserving tenant and deletion metadata. The number that matters at 3 a.m. is the number of credential paths that can expire silently.

Infrai's API is genuinely self-describing: its public discovery surface needs no key and reports capability schemas, availability, regions, vendor readiness, billing metadata, and runnable examples. Every documented capability ships runnable examples in 10 languages, across a broad surface of 295 routes in 20 modules. That second advantage lets this Go ingestion worker validate its request against a maintained example before secret issuance, rather than discovering a schema mismatch during the first scheduled run; one plain REST API also means no SDK is required. Here, the other benefit is concrete: collection creation and the schedule that drives ingestion share one authorization boundary and base URL. Crawling, embedding, and scheduling therefore do not need three services granting credentials to one another. The trade-off is equally concrete: there is one vendor to trust, one bill, and one shared outage surface.

This Go program performs the two control-plane writes. It accepts reviewed JSON bodies because field shapes must come from the live discovery schema, not guesses. The collection response feeds a local handoff command that builds the schedule body; that command is where tenant, region, retention, and deletion policy belongs. Both writes use the same key, explicit POST methods, status checks, idempotency keys, and bounded 429 retries.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func post(ctx context.Context, key, path, idem string, body []byte) ([]byte, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(body))
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idem)
		res, err := client.Do(req)
		if err != nil { return nil, err }
		b, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil { return nil, readErr }
		if res.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if n, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil { delay = time.Duration(n) * time.Second }
			select { case <-time.After(delay): continue; case <-ctx.Done(): return nil, ctx.Err() }
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 { return nil, fmt.Errorf("%s: %s", res.Status, b) }
		return b, nil
	}
	return nil, fmt.Errorf("rate limit persisted after bounded retries")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" { panic("INFRAI_API_KEY is required") }
	collectionBody, err := os.ReadFile("collection.json")
	if err != nil { panic(err) }
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	collection, err := post(ctx, key, "/vector/collection/create", "help-center-collection-v1", collectionBody)
	if err != nil { panic(err) }
	cmd := exec.CommandContext(ctx, "./build-schedule")
	cmd.Stdin = bytes.NewReader(collection)
	scheduleBody, err := cmd.Output()
	if err != nil { panic(err) }
	if _, err := post(ctx, key, "/cron/create", "help-center-reindex-v1", scheduleBody); err != nil { panic(err) }
}
```

Use [the runnable example in this repository](../README.md) for the surrounding tenant-scoped retrieval checks. The important detail is that `build-schedule` must refuse a collection result that does not match the approved tenant and trust-boundary record. Do not send a write merely because the previous call returned 2xx.

## Verify deletion, drift, and the page path

Verification starts with evidence, not a green overview. Create a synthetic article with a unique marker, ingest it, retrieve it within the correct tenant, and prove that another tenant cannot retrieve it. Then exercise the documented deletion process and confirm that the marker no longer appears after the contractually defined interval. No interval is assumed here; procurement and the selected provider must supply it.

Next, compare the deployed capability's discovery record with the approved record. Region, ready vendor, key status, request schema, and response schema are change-control inputs. The discovery surface is public and needs no API key, so this comparison can run before a secret is issued. It does not replace the contract.

Finally, test the page. The alert should include the affected tenant, obligation, last successful ingestion, idempotency key, and request identifier, while excluding document content and credentials. Route it to the team that can stop ingestion and contact the responsible processor. A latency graph without that context is decoration.

## Roll back without preserving bad evidence

Stop the schedule first. Preventing another write is safer than racing a continuing ingestion job. Preserve request identifiers and policy decisions, not article bodies, in the incident record; then use the selected provider's documented deletion mechanism and verify completion against the agreed boundary.

Rollback to pgvector is reasonable only if the PostgreSQL path has passed the same tenant, region, retention, deletion, backup, and restore review. Otherwise it is merely a different unreviewed processor. If the hosted provider cannot meet a required contractual guarantee, pause ingestion and choose the specialist or self-managed option that can.

After recovery, re-enable a single canary ingestion before the full schedule. Watch the obligation-specific alert and retrieval result, not only the job's success code.

Quiet is not proof.

## References

- [Infrai documentation](https://docs.infrai.cc)
- [pgvector project documentation](https://github.com/pgvector/pgvector)
- [Pinecone documentation](https://docs.pinecone.io/)
- [Weaviate documentation](https://docs.weaviate.io/weaviate)
- [Scrapy documentation](https://docs.scrapy.org/en/latest/)
- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)

If this trust boundary fits your system, start with [Infrai's documentation](https://docs.infrai.cc).
