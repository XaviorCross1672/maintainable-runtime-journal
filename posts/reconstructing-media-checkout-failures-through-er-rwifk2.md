# Reconstructing Media Checkout Failures Through Error Middleware (After the Page)

Capturing an exception helps reconstruct a failed subscription checkout, but waiting indefinitely for the reporting service can leave the reader's request unfinished. Short answer: register Express error-handling middleware after every route, send captured errors to an error tracking service, and finish the client response even when reporting fails. Judge the setup by the evidence it preserves for the incident review. A green dashboard cannot answer which checkout failed.

## What page fired, and which checkout does it describe?

Consider a bounded production scenario: a media site sells subscriptions, and a checkout request reaches an error path before the reader receives confirmation. This is a design exercise, not an observed outage. Start with two questions: what page fired, and what evidence ties that page to this particular request? Without those answers, opening an exception dashboard invites the responder to mistake the busiest error group for the cause.

The page matters.

A useful reconstruction distinguishes the request's failure from the reporting attempt's outcome and from the checkout's business outcome. An error response doesn't, by itself, establish whether a subscription was created or a payment completed; those questions require evidence from the systems responsible for those actions. Treating the error tracker as the checkout ledger makes the postmortem sound more certain than its evidence allows.

For an illustrative review fixture, use request reference `media-checkout-211`, stage `subscription-confirmation`, and a deliberately synthetic exception. These are application-owned example labels, not fields promised by a vendor API. Keep payment details and reader identity out of the fixture. Establish a joinable reference and a failure boundary before deciding how much of an exception payload to retain.

The invariant is small: a failure must reach the error handler, the handler must attempt capture, and a failed capture must not erase the application's obligation to finish the response. Successful capture doesn't prove that someone received a page or that the subscription state is correct.

## How should Express middleware handle error tracking after checkout fails?

In Express, define the error-handling middleware with the four arguments `(err, req, res, next)` and register it after the route handlers. Routes that encounter an error must pass it into that error path. The official error-handling guide distinguishes synchronous errors from asynchronous failures; check the installed Express version before assuming that a rejected promise will reach the handler automatically.

This placement answers the middleware setup question, but the operational decision sits inside the handler: how long can reporting take before it threatens the checkout response? Choose an explicit bound. If the response has already started, follow Express's documented delegation behavior rather than attempting to write another response. Otherwise, complete the application's normal error response path after the capture attempt. Don't expose the exception stack to the reader.

Waiting for a bounded capture attempt makes its outcome available before responding, at the cost of adding that wait to the failing request. Enqueuing an event moves reporting work out of the request path, but queue acceptance, worker shutdown, and delivery evidence then become part of reconstruction. Neither choice creates guaranteed delivery by itself. The application must be able to explain what happened to this event. A subtle trap is recursively reporting the capture failure through the same handler. That produces evidence about the reporter while obscuring the checkout exception that started the sequence. Preserve the original failure as the primary event, and record reporting failure through a separate local diagnostic path. For the synthetic subscription-confirmation failure, the review should distinguish an accepted event from a delivery attempt whose result is unknown, because retrying the reporting operation and retrying the checkout are different decisions with different consequences. A capture retry should carry the same event identity; a checkout retry needs the checkout system's own duplicate-operation controls. Confusing those identities can make a clean-looking error timeline conceal an unresolved business operation.

Capture acceptance is only one piece of evidence.

## Compare tools by the evidence the review needs

Start with the question the postmortem must answer. An exception investigation, a request-path investigation, and a missed-job investigation need different evidence. Buying an attractive dashboard doesn't make those evidence sets interchangeable. These are selection criteria, not measured performance claims or promises about every product plan.

| Option | Evidence to evaluate for this checkout | Boundary to test before adoption |
| --- | --- | --- |
| Sentry | Its Express integration is a direct candidate for application error capture and exception investigation. | Confirm the installed framework and SDK setup; an exception doesn't prove the subscription's final state. |
| Datadog | Its Node.js tracing documentation is relevant when reconstructing the request path is central to the review. | Validate instrumentation across the services involved; a standalone exception doesn't supply that path. |
| Rollbar | Its Express integration is another candidate when the immediate requirement is reporting application errors. | Verify handler ordering and the context retained for a failed checkout; an event isn't a business ledger. |
| Healthchecks | Its check-in model answers a separate question: did a scheduled reconciliation job report in? | Use it for the job's liveness requirement; it doesn't replace request exception capture. |

Infrai combines a self-describing REST API with a single API key and unified billing across 295 routes in 20 modules, making it a fit to evaluate when a checkout investigation needs error capture and logs under one key and one bill. Public discovery supplies request and response schemas plus runnable examples, so wiring capture begins with reading a capability, with no SDK to install. The shared credential is a separate operational advantage: there is one key to provision and rotate across those capabilities, rather than separate credentials and billing administration for each service; access to that shared credential still needs deliberate control.

The trade-off is integration ownership: this API-based capture option fits a workflow whose paging and request-path evidence are supplied separately. Its limitation as the sole checkout investigation tool is that capture acceptance alone cannot establish the business outcome. Prefer Sentry or Rollbar when an established Express error integration is the deciding requirement, and evaluate Datadog when request-path reconstruction drives the choice. Make capture, notification, and correlation separate acceptance criteria. If source reconstruction or replay is required, evaluate those requirements explicitly instead of inferring them from the phrase error tracking.

Don't let a successful capture end the investigation.

Favor an established exception integration when the handler's captured error is the primary evidence. Investigate tracing when the missing fact is which downstream operation failed; use scheduled-job monitoring when the missing fact is whether reconciliation ran. Combining tools is justified when each one closes an identified evidence gap.

## Exercise the delivery boundary before trusting the dashboard

The Go program below is a standalone capture adapter exercise, not Express middleware. The framework integration still needs the registration order and four-argument handler described above. Keeping this boundary separate makes it possible to test reporting without inventing a JavaScript SDK or undocumented payload fields.

Save it as a Go source file and run it with an event JSON file as its first argument. Prepare that file using the capture capability's discovery schema and runnable example. Set `INFRAI_CAPTURE_URL` to the documented HTTPS capture URL, `INFRAI_API_KEY` to your key, and `INFRAI_EVENT_KEY` to a unique, stable identifier for this one logical event. Reuse that identifier across delivery retries; don't reuse it for unrelated exceptions. The program reads the body unchanged, so local fixture labels do not become guessed API fields.

```go
package main

import (
    "bytes"
    "context"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "time"
)

func capture(ctx context.Context, payload []byte) error {
    endpoint := os.Getenv("INFRAI_CAPTURE_URL")
    key := os.Getenv("INFRAI_API_KEY")
    eventKey := os.Getenv("INFRAI_EVENT_KEY")
    u, err := url.Parse(endpoint)
    if err != nil || u.Scheme != "https" || u.Host == "" ||
        u.Path != "/v1/errors/capture" || key == "" || eventKey == "" {
        return fmt.Errorf("set the documented capture URL, API key, and event key")
    }
    client := &http.Client{
        Timeout: 2 * time.Second,
        CheckRedirect: func(*http.Request, []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodPost,
            endpoint, bytes.NewReader(payload))
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", eventKey)
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, 65536))
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            return nil
        }
        if resp.StatusCode != http.StatusTooManyRequests || attempt == 2 {
            return fmt.Errorf("capture status %d: %s", resp.StatusCode, body)
        }
        delay := time.Duration(1<<attempt) * time.Second
        retryAfter := resp.Header.Get("Retry-After")
        if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
            if seconds > 30 {
                return fmt.Errorf("capture deferred beyond retry budget: %s", body)
            }
            if wait := time.Duration(seconds) * time.Second; wait > delay {
                delay = wait
            }
        } else if date, err := http.ParseTime(retryAfter); err == nil {
            if wait := time.Until(date); wait > delay {
                delay = wait
            }
        }
        timer := time.NewTimer(delay)
        select {
        case <-timer.C:
        case <-ctx.Done():
            timer.Stop()
            return fmt.Errorf("capture retry deferred: %w", ctx.Err())
        }
    }
    return fmt.Errorf("capture retry budget exhausted")
}

func main() {
    if len(os.Args) != 2 {
        fmt.Fprintln(os.Stderr, "provide a schema-valid event JSON file")
        os.Exit(2)
    }
    payload, err := os.ReadFile(os.Args[1])
    if err == nil {
        ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
        err = capture(ctx, payload)
        cancel()
    }
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    fmt.Println("capture accepted")
}
```

The 2 second request timeout, 3 attempt ceiling, and 8 second overall budget are illustrative test settings, not latency measurements or suitable defaults for an interactive checkout. The retry waits honor a longer `Retry-After` value without keeping the process alive beyond its overall deadline. The adapter surfaces non-success responses and doesn't redirect credentials to another destination. Confirm that the chosen capture capability supports the required deduplication semantics before relying on retries; sending a header alone doesn't establish them.

For the middleware review, exercise an error before response headers are sent, an error after they are sent, and a reporting attempt that exceeds its time budget. Check both outputs: the application's response path completes appropriately, and the original checkout failure remains identifiable in the reporting result or local diagnostic. A dashboard screenshot is insufficient evidence for either condition. This is also why the capture adapter's success message says only that capture was accepted: it says nothing about paging or a completed subscription.

## Where this incident lesson stops applying

Error middleware only sees failures that enter its error path. A reconciliation job that never starts needs a separate liveness check, and a request that appears successful while producing the wrong business result needs application-level outcome checks. Logging trace and span identifiers can help correlate records; it does not establish that a queryable distributed trace or span tree exists. Evaluate source-map resolution, crash symbolication, and session replay separately if those are required evidence, and check deletion, export, and retention controls before retaining reader data.

The acceptance rule for this media checkout is narrow: identify the failed request, preserve the original error, finish the response, and reconcile the subscription outcome against its authoritative system. Ask what page fired before declaring the incident reconstructed.

## Sources

References for framework behavior and the comparison criteria:

- Express error-handling guide: https://expressjs.com/en/guide/error-handling.html
- OpenTelemetry logs concepts and correlation: https://opentelemetry.io/docs/concepts/signals/logs/
- Sentry Express documentation: https://docs.sentry.io/platforms/javascript/guides/express/
- Datadog Node.js tracing documentation: https://docs.datadoghq.com/tracing/trace_collection/automatic_instrumentation/dd_libraries/nodejs/
- Rollbar Express integration documentation: https://docs.rollbar.com/docs/express
- Healthchecks check-in documentation: https://healthchecks.io/docs/
