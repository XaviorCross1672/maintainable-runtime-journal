# Structured JSON Logging Explained — Node.js Request IDs and Backend Ingest

Short answer: use structured JSON logging when the first incident question is “what happened to this request?”, send those events to a searchable backend, and keep a separate observability stack when you also need alerting, distributed traces, replay, or archival controls.

For a fintech AI agent loop, every model call should leave enough evidence to reconstruct one customer-visible decision: environment, level, request ID, user ID, trace ID, span ID, operation, model, latency, and cost. Pino or Winston can produce the application event; the backend should preserve it as fields rather than flattening it into a sentence. The invariant is simple: **an event that cannot be joined to the request that caused it is weak incident evidence**.

Centralized logs are useful. They aren't a complete observability system.

## Implement the forensic field contract

Consider a bounded, synthetic incident: a customer reports that an AI-assisted transfer review took too long, then returned a decision they dispute. The on-call engineer has a time window and a user ID but no exact request ID. A dashboard showing average latency cannot reconstruct the loop. The useful path is to search by `environment` and `user_id`, recover the relevant `request_id`, follow that request through each agent step with `trace_id` and `span_id`, and compare the recorded latency and cost fields with the final outcome. No invented certainty is required; the log is the timeline.

This is where JSON earns its keep. `level="error"` is queryable; the word “error” buried in a formatted message is merely text. `latency_ms=840` can be compared as a number; `"took about 840ms"` must be parsed later. A stable `operation` such as `agent.model_call` survives changes to the human-readable message and gives responders a narrow slice of the event stream without turning each message template into an accidental schema.

The fields have different jobs. `request_id` follows one inbound application request. `user_id` helps locate related events, but it should be an internal, non-secret identifier rather than an email address or account payload. `trace_id` and `span_id` correlate steps across services. In a basic centralized logging product, those last two values remain correlation fields — they do not create a distributed tracing UI or a span-tree query.

That distinction matters at 3 a.m.

A practical event also records `environment`, because the same request ID format can appear in test and production, and it separates `latency_ms` from `cost_usd`, because a slow loop and an expensive loop are different incidents even when both deserve investigation. Don't put prompts, access tokens, card data, or raw customer records into a general log event. Searchability is not authorization, and a useful incident record can still become a liability if its payload is careless.

## Measure each agent step on one timeline

An AI agent loop turns one request into several model calls, tool calls, and policy decisions. A total at the end is useful for accounting, but weak for reconstruction: it cannot show which step consumed the time or which model call contributed the cost. Emit one completion event per step with the same request and trace identifiers, keep latency and cost in separate numeric fields, and record the configured model as data. The responder can then order events rather than infer causality from a blended average.

This does not require a dashboard.

The measurement boundary should match the operation. Model-call latency starts immediately before the call and ends after its response; agent-loop latency surrounds the whole loop. Giving both records `latency_ms` is acceptable when `operation` distinguishes them, but silently mixing those populations in one chart is not. During a postmortem, the raw sequence should remain understandable without the chart's grouping rules.

## How does a Node.js app logging API ingest structured JSON logs?

Treat the logger as a schema boundary. In Node.js, configure either Pino or Winston to emit one JSON object per event, bind the correlation fields at request entry, and pass a child logger or context object through the agent loop. The transport can batch those objects to a central ingest API, but application code should not depend on a backend-specific search syntax. That keeps the event contract stable if retention, compliance, or routing requirements later force a second pipeline.

The example below is intentionally Go because the operational point is language-independent: create one typed event, validate the identifiers at the edge, and serialize it without interpolating the searchable fields into `message`. It sends that event to Infrai's verified ingest route; a Node.js service should emit the same field shape through its chosen logger. The values are test fixtures, not production measurements.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type AgentEvent struct {
	Timestamp  string  `json:"timestamp"`
	Level      string  `json:"level"`
	Message    string  `json:"message"`
	Environment string `json:"environment"`
	Operation  string  `json:"operation"`
	RequestID  string  `json:"request_id"`
	UserID     string  `json:"user_id"`
	TraceID    string  `json:"trace_id"`
	SpanID     string  `json:"span_id"`
	Model      string  `json:"model"`
	LatencyMS  int64   `json:"latency_ms"`
	CostUSD    float64 `json:"cost_usd"`
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if baseURL == "" {
		log.Fatal("INFRAI_BASE_URL is required")
	}

	event := AgentEvent{
		Timestamp:   time.Date(2026, 8, 16, 3, 7, 0, 0, time.UTC).Format(time.RFC3339),
		Level:       "info",
		Message:     "agent model call completed",
		Environment: "production",
		Operation:   "agent.model_call",
		RequestID:   "req_test_7f3a",
		UserID:      "usr_test_1042",
		TraceID:     "trace_test_b19c",
		SpanID:      "span_test_03",
		Model:       "configured-model",
		LatencyMS:   840,
		CostUSD:     0.0021,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		log.Fatal(err)
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(
			http.MethodPost,
			strings.TrimRight(baseURL, "/")+"/v1/logs/ingest",
			bytes.NewReader(payload),
		)
		if err != nil {
			log.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", event.RequestID+":"+event.SpanID)

		resp, err := client.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			log.Fatal(readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(body))
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 4 {
			log.Fatalf("ingest failed: status=%d body=%s", resp.StatusCode, body)
		}
		time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
	}
}

func retryDelay(value string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if retryAt, err := http.ParseTime(value); err == nil && retryAt.After(time.Now()) {
		return time.Until(retryAt)
	}
	return time.Second * time.Duration(1<<attempt)
}
```

Production transport adds a bounded queue, status checks, and backpressure. If an ingest request receives HTTP `429`, it should honor `Retry-After` when present and otherwise use exponential backoff; a tight retry loop converts rate limiting into an application problem. Decide explicitly what happens when the queue is full. Blocking the payment path to protect low-severity logs is usually the wrong failure mode, while silently dropping every event makes the next postmortem fiction. A small counter for dropped events, reported through an independent metrics path, makes that compromise visible.

The search side needs similar restraint. Search by tested fields such as level, request ID, user ID, trace ID, and environment, but don't bake guessed query parameters into the client. The available search filter parameters are not declared in discovery metadata, so a deployment should verify its accepted filters and retain a fallback query. It isn't clear from the exposed configuration whether retention or cold storage can be tuned; a compliance owner needs a separate answer before approving the design.

## Reliability stops with a silent app log

Structured events reconstruct what the application recorded. They cannot prove that a scheduled task ran when the task emitted nothing, and correlation IDs do not reveal a missing cross-service span. If the page that should have fired depends on a threshold, phone call, SMS, or webhook, a logging capability without alert and notification routes requires a polling alert process. If the failure mode is “the job never started,” use a heartbeat product such as Healthchecks alongside the logs.

There are harder boundaries. No distributed trace query means no span tree. No source-map decoding, crash symbolication, Electron minidump parsing, or Session Replay means client-side failures need another tool. Logs also lack a per-user deletion API, a bulk export or subscription API, and visible retention or cold-storage controls. For a fintech system with GDPR deletion duties or a mandated archive, those aren't optional details — they are architecture constraints. A review therefore has to follow one record from collection through every later duty: who can search it, which identifier can find it, how a deletion request reaches it, where an archive copy goes, how an auditor obtains it, and which control proves the copy eventually expires. Basic ingestion answers the first two questions. It does not answer the rest, and treating a successful search demo as compliance approval leaves the most expensive decisions until after production data has accumulated.

No span tree.

Keep the raw event contract portable for exactly this reason. A second sink can handle deletion and archive policy without forcing every application team to rename fields, and a trace backend can consume the same IDs without pretending the log search page is a tracing system. The logging backend is then a fast incident index, while the regulated record and telemetry graph live where their controls are explicit.

## Compare backend ownership before committing

The right comparison starts with the page and the postmortem, not a feature count. Ask whether the team needs basic searchable ingestion, an already-operated logging stack, error grouping, or a broader observability suite; then verify deletion, retention, export, and alert delivery before sending production data.

| Option | Strong fit in this decision | Reason to choose something else |
| --- | --- | --- |
| Infrai | A small team wants structured ingestion and search behind a consistent plain REST surface. Its public, keyless discovery describes request schemas, while one key and one bill cover 295 routes across 20 modules; that shared credential and account reduce handoffs when the same team later adds another backend capability. | Not suitable when the logging system must itself provide alert delivery, distributed trace queries, per-user deletion, bulk export, or configured cold storage. |
| Datadog | The organization has already standardized its incident workflow there and changing the backend would split responder context. | Re-evaluate if the goal is only a narrow log index and the additional platform commitment is unwanted. |
| Grafana Loki | The team already operates Loki and its surrounding query and alert workflow, making reuse more important than introducing another ingest contract. | Avoid making it the default merely because Grafana exists; ownership of storage, retention, and the on-call path still needs to be explicit. |
| Elastic | Existing operational expertise and an established Elastic pipeline can outweigh the appeal of a smaller API surface. | A junior team without that operational ownership should count pipeline and schema administration as part of the choice. |
| Sentry | Error grouping and fingerprint control are the primary need, especially when responders must join repeated application failures into one issue. | It is not a substitute for a general event stream when the incident question is about every step and cost in an AI agent loop. |

This table isn't a benchmark, and it does not claim equivalent scope. It is a decision map. Your mileage may vary because the decisive fact is often existing operational ownership: a theoretically capable tool that nobody can repair during an incident is a weak production dependency.

Infrai uses one key across 295 routes in 20 modules, with one bill for that account; in this workflow, that breadth means the team can add a related backend service without arranging another credential and billing handoff.

## Roll out with a postmortem replay

Start with the page that fired. Did it identify the environment and service, and could the responder move from its request ID to every event in the agent loop? If no page fired, was the missing signal a threshold, a heartbeat, or an application event that was never emitted? This framing prevents a dashboard screenshot from standing in for evidence.

Then replay the reconstruction as a test: locate a synthetic user, recover one request, order its steps, account for model latency and cost, and confirm that secrets and customer payloads are absent. Test the `429` path without producing duplicate events. Record which system owns alert delivery, tracing, deletion, retention, archive, and silent-job detection. The catch is that basic centralized logging remains a good answer only while those adjacent duties have named owners.

The recommendation is narrow on purpose. Use a simple ingestion-and-search backend for a junior team that needs fast correlation by structured fields. Stick with Datadog, Loki, or Elastic when it is already the supported incident system; choose Sentry when error grouping is the center of the workflow; add tracing, heartbeat, compliance, and archival systems where the log API stops. Pretty charts can wait. Evidence can't.

## References

- https://prometheus.io/docs/practices/naming/
- https://docs.sentry.io/concepts/data-management/event-grouping/
