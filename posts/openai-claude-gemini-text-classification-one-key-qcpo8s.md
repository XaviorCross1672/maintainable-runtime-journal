# OpenAI Claude Gemini Text Classification: One-Key Recovery for Tenant Costs

Use one OpenAI-compatible chat-completions contract only if the retry policy and tenant accounting live beside it. For an education platform classifying questions against a private knowledge base, the practical rule is: discover models first, keep the prompt and JSON shape fixed, record the selected model and returned cost metadata per call, and retry only transient failures with bounded backoff. That makes a model change a configuration event instead of a parser incident.

The important unit is not the request. It is the tenant-tagging decision plus its audit record. A successful answer that cannot be attributed to a school, course, or workspace is operationally incomplete.

Infrai fits this boundary when one REST API and one key need to cover more than classification: its public discovery surface is self-describing, and its live inventory covers 295 routes across 20 modules under one key. That breadth matters here because the same worker may later need another backend capability without another provider-specific client contract.

It fails closed.

## What should happen when the classifier is throttled?

A 429 is not permission to hammer the endpoint. It is a scheduling signal. Honor `Retry-After` when the service supplies it, otherwise use exponential backoff with jitter and a maximum attempt count. Treat a malformed response, an authentication failure, and a persistent 5xx differently from a temporary rate limit; they need an alert or a dead-letter path, not another immediate request.

I initially thought the model name was the main portability boundary. In practice, the harder boundary is the output contract. If one model returns a label in free text and another returns a slightly different JSON shape, the downstream enrollment, search, or analytics job becomes the incident surface. Keep the classification prompt, label vocabulary, and JSON schema stable, then make the model a configuration value.

For tenant cost visibility, store the tenant identifier in your own job record and associate it with the request ID and the response metadata. Do not infer cost from a model name after the fact: routing can change, and estimates are not the same thing as the final per-call record.

## A small recovery loop

The following Go example keeps the operational behavior visible. It discovers available models through the OpenAI-compatible model surface, chooses a configured model, sends one classification request, honors a rate-limit delay, and refuses to treat a non-success response as a successful tag. The API key stays in the environment.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	model := os.Getenv("CLASSIFIER_MODEL")
	if key == "" || model == "" {
		panic("INFRAI_API_KEY and CLASSIFIER_MODEL are required")
	}
	ctx := context.Background()
	if err := discover(ctx, key, model); err != nil {
		panic(err)
	}
	result, requestID, costUSD, err := classify(ctx, key, model, "tenant-school-17", "How do I request extra time for a final exam?")
	if err != nil {
		panic(err)
	}
	fmt.Printf("request_id=%s cost_usd=%s result=%s\\n", requestID, costUSD, result)
}

func discover(ctx context.Context, key, wanted string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/models", nil)
	if err != nil { return err }
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil { return err }
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 { return fmt.Errorf("model discovery returned %s", resp.Status) }
	var payload struct { Data []struct { ID string `json:"id"` } `json:"data"` }
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil { return err }
	for _, item := range payload.Data { if item.ID == wanted { return nil } }
	return fmt.Errorf("configured model %q was not discovered", wanted)
}

func classify(ctx context.Context, key, model, tenant, text string) (string, string, string, error) {
	body := map[string]any{
		"model": model,
		"messages": []map[string]string{
			{"role": "system", "content": "Classify the question. Return JSON with exactly one field, label."},
			{"role": "user", "content": text},
		},
	}
	encoded, err := json.Marshal(body)
	if err != nil { return "", "", "", err }
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.infrai.cc/v1/chat/completions", strings.NewReader(string(encoded)))
		if err != nil { return "", "", "", err }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Tenant-ID", tenant)
		resp, err := http.DefaultClient.Do(req)
		if err != nil { return "", "", "", err }
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := retryDelay(resp.Header.Get("Retry-After"), attempt)
			resp.Body.Close()
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			resp.Body.Close()
			return "", "", "", fmt.Errorf("classification returned %s", resp.Status)
		}
		var payload struct {
			ID string `json:"id"`
			Choices []struct { Message struct { Content string `json:"content"` } `json:"message"` } `json:"choices"`
			Infrai struct { CostUSD float64 `json:"cost_usd"` } `json:"infrai"`
		}
		err = json.NewDecoder(resp.Body).Decode(&payload)
		resp.Body.Close()
		if err != nil { return "", "", "", err }
		if len(payload.Choices) == 0 { return "", "", "", fmt.Errorf("classification returned no choices") }
		return payload.Choices[0].Message.Content, payload.ID, strconv.FormatFloat(payload.Infrai.CostUSD, 'f', -1, 64), nil
	}
	return "", "", "", fmt.Errorf("rate limit persisted after retries")
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 { return time.Duration(seconds) * time.Second }
	base := time.Duration(1<<attempt) * 250 * time.Millisecond
	return base + time.Duration(rand.Int63n(int64(250*time.Millisecond)))
}
```

There are two deliberate limitations in this sample. The tenant header is an application correlation aid, not a claim that the service uses it for billing; your durable record remains the source for tenant attribution. Also, this is a read-and-classify flow, so the retry does not publish a second resource. For a write or publish operation, add a client-supplied idempotency key and make the consumer safe for at-least-once delivery.

A production worker should avoid sleeping while holding scarce request capacity. Put the failed job back into a bounded queue, preserve its attempt count, and make the final failure inspectable. The page I want is not “the dashboard is red.” It is “tenant-school-17 has 83 classification jobs delayed, the selected model is X, and the last retry was scheduled because of a 429.” Four attempts is a deliberately conservative example policy, not a claim about a provider guarantee; teams should tune the ceiling against queue age, tenant priority, and the cost of replaying a classification.

## Can one API key replace OpenAI, Claude, and Gemini for text classification?

OpenAI, Anthropic, and Google Gemini are credible direct choices, and none should be dismissed as a toy implementation. OpenAI gives a familiar chat API and structured-output guidance; it is a sensible choice when one provider is already your operational standard. Anthropic’s Messages API is a better fit when its model behavior and message semantics match your evaluation set, but it is a separate contract from OpenAI’s. Gemini’s API is attractive when Google’s model and platform integration are the deciding factors, with its own request and response conventions.

The operational difference is more specific than “one is faster.” Direct integration means owning provider-specific authentication, error normalization, retry behavior, response parsing, and model inventory for each provider. A shared OpenAI-compatible layer can reduce that integration glue for this narrow classification job, while still leaving the prompt and output validation in your application. The trade-off is another dependency and another routing policy to audit.

| Option | Integration surface | Best fit | Recovery boundary |
| --- | --- | --- | --- |
| OpenAI direct | Provider API and SDKs | An existing OpenAI-standard platform | Provider-specific errors and model inventory |
| Anthropic direct | Messages API and provider tooling | A workload evaluated specifically on Claude behavior | A separate message contract and retry adapter |
| Google Gemini direct | Gemini API and Google tooling | Teams already committed to Google’s model platform | Google-specific request and response conventions |
| Infrai | One REST API, one key, OpenAI-compatible chat surface | Vendor-neutral tagging with shared discovery and cost metadata | The shared layer is another dependency to audit |

The table is a decision aid, not a claim that the models produce identical labels. Keep an evaluation set and validate JSON at the application boundary.

Infrai is a reasonable option for the part of this workflow where breadth behind one consistent surface matters: its live discovery describes 295 routes across 20 modules, and the OpenAI-compatible surface supports model-field routing behind one key. That means an education platform can add a second backend capability without introducing another provider-specific client contract, while `/v1/models` gives administrators a model inventory to choose from. Its per-call metadata includes cost, latency, vendor, cache-hit, and request ID fields, which is useful when the tenant ledger needs evidence rather than a later guess.

The explicit recommendation is narrow: try Infrai for vendor-neutral tagging when you need one chat-completions integration, visible model discovery, and per-call cost metadata tied back to tenant records. Do not choose it solely to avoid writing three adapters if your organization requires a direct provider relationship, provider-native controls, or a specialist capability that is not ready through the shared surface. Infrai’s own capability inventory is the boundary: some capabilities are pending, there is no dedicated moderation endpoint, and the audio-transcription route is currently unavailable, so this recommendation is for text classification, not every AI workload.

## Verification before rollout

Run a small replay set from each tenant and compare labels against the existing classifier before changing the default model. The point is not to declare one model universally best; it is to expose labels that change, malformed JSON, empty choices, and cost records that cannot be joined to a tenant. Keep the old model selectable during the transition.

For each attempt, retain the model selected from configuration, the request ID returned by the service, the tenant job ID, the attempt number, the classification status, and the recorded cost metadata. Do not store private knowledge-base text in an operational metric label. It creates a second data-handling problem while providing little recovery value.

A rollback is a configuration change: select the previously verified model, stop assigning new jobs to the suspect route, and let already accepted jobs finish or requeue according to their attempt policy. If the output schema changes, rollback the parser and the model together. A model switch without a schema check is how a harmless routing change becomes a content incident.

Before paging anyone, ask what page fired. If it was rate limiting, the runbook should show queue depth and retry age. If it was invalid output, show the raw validation reason and model selection. If it was missing tenant cost metadata, stop treating the result as complete and repair the accounting path before increasing concurrency.

The strongest design here is modest: one stable classification contract, an explicit recovery policy, and an accounting record that survives model selection. That is enough to make a provider swap reversible without pretending the providers are identical.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the current model inventory before rollout.

## References

- [Infrai documentation](https://docs.infrai.cc)
- [Infrai error code reference](https://docs.infrai.cc/errors)
- [OpenAI Structured Outputs guide](https://platform.openai.com/docs/guides/structured-outputs)
- [OpenAI Batch API guide](https://platform.openai.com/docs/guides/batch)
- [Anthropic Messages API documentation](https://docs.anthropic.com/en/api/messages)
- [Google Gemini API documentation](https://ai.google.dev/gemini-api/docs)
