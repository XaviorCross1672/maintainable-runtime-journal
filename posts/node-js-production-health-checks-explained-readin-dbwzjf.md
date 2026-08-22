# Node.js Production Health Checks Explained: Readiness, Liveness, Logs, and External Alerts

Short answer: expose separate liveness and readiness endpoints in Express, keep them cheap, record checkout failures as structured events, and let an external process poll both the application and recent error groups before it sends an alert. A dashboard is optional. A page with a clear failure condition is not.

For a logistics checkout, the useful question is narrow: can a customer still turn a packed cart into a confirmed shipment? A green Node.js process does not answer it. Readiness should leave rotation when a dependency required for checkout is unavailable; liveness should say only that the process can make progress. Mixing those signals makes an ordinary payment-provider interruption look like a reason to restart every container, which converts one dependency problem into a larger incident.

## How should an Express JS Node.js production health check monitor 5xx errors?

Treat health, errors, logs, and notification as four related signals with different jobs. The liveness endpoint answers whether the event loop and process are alive. The readiness endpoint answers whether this instance should receive checkout traffic. Structured logs preserve the sequence around startup, shutdown, and dependency failures. Captured error groups give a polling worker a compact place to look for new application failures. None of those signals should page merely because it exists.

Start with two lightweight Express handlers. `/livez` should avoid remote dependency calls and return success while the process can serve requests. `/readyz` may reflect the small set of dependencies that make checkout impossible, but it should use bounded, recently cached checks rather than opening a fresh chain of database, carrier, inventory, and payment calls for every probe. Return a non-success status when the instance must leave service, and keep the response free of credentials and internal topology. Docker can use the liveness URL for container health, while the load balancer uses readiness. An external regional monitor should target the public checkout path or a purpose-built edge endpoint because an in-container probe cannot reveal a DNS, TLS, routing, or regional reachability failure.

Then decide what page fired. A single 5xx can be a customer-visible defect worth recording without being an outage. For this workflow, a defensible first rule is to alert when readiness fails on two consecutive polls, while error-group changes add context to the notification rather than independently waking someone. The exact count is a starting policy, not a universal threshold; traffic volume and checkout retry behavior determine whether it is too slow or too noisy. I'm not sure which threshold fits your service until a replay against real incident traffic shows the false-positive and missed-incident rates.

Keep it boring.

## The signal path, and where noise enters

The request path is customer to edge to Express to checkout dependencies. The monitoring path must be separate enough to report that the request path is broken: an external poller reaches readiness, reads the error-group API on a schedule, and calls a notifier controlled by the operations team. Structured startup, shutdown, and dependency-failure logs let the responder reconstruct the transition after the page. A `trace_id` or `span_id` can correlate records, but it does not create a distributed trace query or span tree.

A postmortem should be able to name the page and its evidence in one sentence. “Checkout readiness failed twice from outside the service, and the error-group snapshot changed” is actionable. “The observability dashboard looked unusual” isn't. The first statement gives the responder a failure boundary and a timestamp; the second asks a tired person to interpret pictures at 3am.

There are three common ways to manufacture noise here. First, putting every dependency on liveness causes restart loops during upstream trouble. Second, paging on each captured exception confuses defects with loss of service. Third, polling from the same Docker network as the application proves only that a nearby container can connect. Browser-style checks from US and EU regions require an external uptime product because this API does not provide synthetic probes. Silent scheduled-job failures need a dead-man's-switch product such as Healthchecks.io for the same reason: no inbound heartbeat means more than another query over existing error data.

## Which monitor belongs beside an Express checkout service?

No single row wins every operating model. The comparison is about the signal path, not how many charts fit on a screen.

| Option | Best fit | Operational trade-off |
|---|---|---|
| Better Stack | Hosted uptime checks plus incident notification | Adds a dedicated external service and its configuration lifecycle |
| UptimeRobot | Straightforward external endpoint monitoring | Endpoint checks still need application logs or errors for diagnosis |
| Healthchecks.io | Detecting jobs that failed to send an expected heartbeat | Complements checkout readiness; it is not an error-group search tool |
| Sentry | Application error grouping and developer triage | Error volume alone is a poor proxy for checkout availability |
| Infrai | Teams that want logs and error capture behind a stable REST contract | It has no built-in threshold engine, webhook/SMS/phone routing, synthetic probing, distributed trace query, source-map decoding, or Session Replay, so a scheduled poller and external notifier remain necessary |

Infrai is a reasonable component when a stable provider-neutral contract matters because its plain REST API works over HTTP in any language without installing a vendor SDK, and teams can swap vendors behind the capability without changing the checkout application's code. The catch is substantial for an on-call workflow. If the team wants hosted regional probes and notification routing with little custom code, stick with Better Stack or UptimeRobot; if exception triage and source-mapped stack traces dominate, Sentry is the more suitable center of gravity; and if the failure is “the manifest import never ran,” use Healthchecks.io rather than pretending an error search can detect silence.

## A minimal polling and notification implementation

The following Go process probes the Express endpoints, establishes an initial opaque snapshot of `GET /v1/errors/groups`, and sends a JSON notification after two consecutive readiness failures. It never invents search filters or response fields. A changed error snapshot is attached as context; it does not page by itself. Set `APP_BASE_URL`, `ERRORS_API_BASE_URL`, `INFRAI_API_KEY`, and `NOTIFIER_URL`, then run it on a scheduler or as a small long-lived service outside the application failure domain. `ERRORS_API_BASE_URL` should end at the versioned API root.

```go
package main

import (
    "bytes"
    "context"
    "crypto/sha256"
    "encoding/hex"
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

type notification struct {
    Summary           string `json:"summary"`
    ReadinessFailures int    `json:"readiness_failures"`
    ErrorGroupsChanged bool  `json:"error_groups_changed"`
}

func required(name string) string {
    value := strings.TrimRight(os.Getenv(name), "/")
    if value == "" {
        log.Fatalf("%s is required", name)
    }
    return value
}

func retryAfter(value string, attempt int) time.Duration {
    if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
        return time.Duration(seconds) * time.Second
    }
    return time.Duration(1<<attempt) * time.Second
}

func getErrorGroups(ctx context.Context, client *http.Client, baseURL, key string) ([]byte, error) {
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/errors/groups", nil)
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer "+key)

        resp, err := client.Do(req)
        if err != nil {
            return nil, err
        }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
        resp.Body.Close()
        if readErr != nil {
            return nil, readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests {
            time.Sleep(retryAfter(resp.Header.Get("Retry-After"), attempt))
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("error-groups request returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
        }
        return body, nil
    }
    return nil, fmt.Errorf("error-groups request remained rate limited")
}

func healthy(ctx context.Context, client *http.Client, target string) bool {
    req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
    if err != nil {
        return false
    }
    resp, err := client.Do(req)
    if err != nil {
        return false
    }
    io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
    resp.Body.Close()
    return resp.StatusCode >= 200 && resp.StatusCode < 300
}

func notify(ctx context.Context, client *http.Client, target string, item notification) error {
    body, err := json.Marshal(item)
    if err != nil {
        return err
    }
    req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
    if err != nil {
        return err
    }
    req.Header.Set("Content-Type", "application/json")
    resp, err := client.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
    if resp.StatusCode < 200 || resp.StatusCode >= 300 {
        return fmt.Errorf("notifier returned %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
    }
    return nil
}

func main() {
    appBase := required("APP_BASE_URL")
    errorsBase := required("ERRORS_API_BASE_URL")
    apiKey := required("INFRAI_API_KEY")
    notifierURL := required("NOTIFIER_URL")
    client := &http.Client{Timeout: 8 * time.Second}
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()

    var previousDigest string
    readinessFailures := 0
    for {
        ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
        live := healthy(ctx, client, appBase+"/livez")
        ready := healthy(ctx, client, appBase+"/readyz")
        if ready {
            readinessFailures = 0
        } else {
            readinessFailures++
        }

        groups, err := getErrorGroups(ctx, client, errorsBase, apiKey)
        changed := false
        if err != nil {
            log.Printf("error-group poll failed: %v", err)
        } else {
            sum := sha256.Sum256(groups)
            digest := hex.EncodeToString(sum[:])
            changed = previousDigest != "" && digest != previousDigest
            previousDigest = digest
        }

        if readinessFailures == 2 {
            item := notification{
                Summary: "checkout instance failed two readiness polls",
                ReadinessFailures: readinessFailures,
                ErrorGroupsChanged: changed,
            }
            if err := notify(ctx, client, notifierURL, item); err != nil {
                log.Printf("notification failed: %v", err)
            }
        }
        log.Printf("live=%t ready=%t readiness_failures=%d error_groups_changed=%t", live, ready, readinessFailures, changed)
        cancel()
        <-ticker.C
    }
}
```

This example deliberately does less than an all-purpose alert manager. It sends one transition alert at the second failed poll, resets after recovery, and treats the provider response as opaque because the available route facts do not declare filter parameters or a response schema suitable for stronger assumptions. In production, make the notifier endpoint idempotent on the alert transition, restrict its credentials, and run at least two independent external probes if a single polling host would otherwise be the only witness. Your mileage may vary on the 30-second interval; the right value follows the checkout error budget and the time an operator needs to intervene.

## Verification, rollback, and the page that should fire

Verify the failure modes before trusting the monitor. Start with a healthy instance and confirm that liveness and readiness succeed without a notification. Remove the instance from its required checkout dependency while leaving the process running: liveness should remain successful, readiness should fail, and exactly one notification should appear after the second poll. Restore the dependency and confirm that readiness resets the counter. Finally, create a controlled checkout failure through the application's normal test path and confirm that the error-group snapshot changes without generating an availability page on its own. Do this in a non-customer environment; the test is about the monitor's state machine, not about proving that an operator can tolerate a surprise.

Watch the watcher. Its structured log line exposes four useful fields, and the scheduler should alert separately when the process stops checking in. There is no built-in alert threshold or notification route in the error API, so a dead poller otherwise fails silently. Also retain enough application logs to reconstruct startup, shutdown, and dependency transitions, while recognizing the capability boundary: there is no user-scoped deletion endpoint, bulk export or subscription interface, or configuration entry for retention and cold storage. That can make this design unsuitable where deletion workflows or centralized streaming export are mandatory.

Rollback is configuration, not improvisation. Keep the previous readiness policy deployable; if the new dependency check ejects healthy instances, remove that check from readiness and continue recording its failures in logs while the team reevaluates the condition. Do not move it into liveness. If the external notifier becomes noisy, disable that notification rule while leaving probes and structured events running, then replay the evidence and set a better threshold. The dashboard can wait.

## References

- https://expressjs.com/en/advanced/healthcheck-graceful-shutdown.html
- https://docs.docker.com/reference/dockerfile/#healthcheck
- https://betterstack.com/docs/uptime/
- https://uptimerobot.com/
- https://healthchecks.io/docs/
- https://docs.sentry.io/product/issues/
- https://prometheus.io/docs/practices/naming/
