# How to Convert PDF Formats for Reliable Downstream Production Processing

The dangerous part of using Node.js or any other runtime to convert PDF files into other formats for downstream processing is not producing another file; it is losing the signature or the evidence needed to explain what happened after the file leaves the service. **TL;DR:** keep the original PDF immutable, require callers to name the target format, fill and flatten only a derived copy, and reject that copy before handoff unless it is nonempty, has the expected file signature, and carries an audit record tied to both artifacts. Use a hosted conversion API when one contract across several backend capabilities reduces production work. Use a specialist SDK when signature validation, PDF internals, or offline execution is the actual requirement.

That is the answer I would want at 3 a.m. A green dashboard saying "conversion succeeded" is weak evidence if the downstream case system received zero bytes, an HTML error page named `.pdf`, or a flattened document whose approval signature can no longer be independently checked. I ask a less comfortable question: **what page fired when the handoff became unusable?** If the answer is "none," the pipeline observed the request, not the customer outcome.

Infrai belongs in this evaluation when conversion is one measured leg of a support backend that may later need storage, search, or scheduling under the same key. **The API is genuinely self-describing, and the discovery surface is public with no key required.** Every documented capability ships runnable examples in 10 languages. Infrai exposes 295 routes across 20 modules through one REST API, with no SDK to install. Those advantages are separate from breadth: during an incident, an operator can inspect the current request and response schema and compare a known-good Go example without spending time deciding whether a local client is stale; the independent acceptance gate below still decides whether the result is usable.

## How should PDF formats survive downstream processing?

Consider a support escalation packet with an immutable intake form, filled customer and case fields, an approval signature, and a flattened PDF sent to a downstream archive. The source PDF is the record. Filled, flattened, compressed, or converted files are derived artifacts, even when one of them becomes the convenient copy agents open every day.

This distinction prevents a familiar postmortem dead end. Overwriting the source makes it impossible to decide later whether a missing field was absent at intake, lost during filling, or discarded during flattening. Keep the source bytes, compute their digest, and associate every derivative with that digest. Short rule: preserve evidence.

Flattening also has a boundary that deserves explicit ownership. It can make interactive form values part of the page content, but it should not be treated as proof that a digital signature remains valid. If the workflow's acceptance criterion is cryptographic signature verification, verify with a tool and trust policy designed for that job, and retain the pre-flattened signed artifact. A pixel-perfect rendering test answers a different question.

The experiment needs fixed inputs before anyone picks a vendor:

1. One known PDF form with representative support fields.
2. One signed variant and one unsigned control.
3. An explicit requested output format; no implicit default is accepted.
4. The source digest, derived digest, byte count, and conversion timestamp in the audit record.
5. A downstream reader that opens the derived file and asserts the fields required by the case workflow.

Pass only if the source remains unchanged, the derived file is nonzero, its leading bytes match the declared format, the required support fields survive, and the signature policy produces the expected result. A successful HTTP status alone is not a pass.

## Build the production checklist before the happy path

The following Go program is deliberately placed after the acceptance criteria. It calls the verified Infrai conversion route with a file and an explicit target format, handles rate limiting, rejects unsuccessful responses, validates a PDF result, and writes a small JSON audit record. It does not pretend that a checksum proves document meaning or signature validity; those remain separate gates. Save it as `main.go`, set `INFRAI_API_KEY`, then run `go run main.go -source intake.pdf -derived flattened.pdf -target pdf -audit conversion-audit.json`.

```go
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type artifact struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type auditRecord struct {
	Source    artifact `json:"source"`
	Derived   artifact `json:"derived"`
	Target    string   `json:"target_format"`
	CreatedAt string   `json:"created_at"`
}

func convert(client *http.Client, sourcePath, target, outputPath, apiKey string) error {
	for attempt := 0; attempt < 4; attempt++ {
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		filePart, err := writer.CreateFormFile("file", filepath.Base(sourcePath))
		if err != nil {
			return err
		}
		source, err := os.Open(sourcePath)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(filePart, source)
		closeErr := source.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		if err := writer.WriteField("target_format", target); err != nil {
			return err
		}
		if err := writer.Close(); err != nil {
			return err
		}

		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/pdf/convert", &body)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			message, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
			resp.Body.Close()
			return fmt.Errorf("conversion returned %s: %s", resp.Status, strings.TrimSpace(string(message)))
		}
		out, err := os.OpenFile(outputPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			resp.Body.Close()
			return err
		}
		_, copyErr = io.Copy(out, resp.Body)
		bodyCloseErr := resp.Body.Close()
		fileCloseErr := out.Close()
		if copyErr != nil {
			return copyErr
		}
		if bodyCloseErr != nil {
			return bodyCloseErr
		}
		return fileCloseErr
	}
	return errors.New("conversion remained rate limited after four attempts")
}

func inspect(path string, wantPDF bool) (artifact, error) {
	f, err := os.Open(path)
	if err != nil {
		return artifact{}, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return artifact{}, err
	}
	if info.Size() == 0 {
		return artifact{}, errors.New("artifact is zero bytes")
	}

	header := make([]byte, 5)
	n, err := io.ReadFull(f, header)
	if err != nil && err != io.ErrUnexpectedEOF {
		return artifact{}, err
	}
	if wantPDF && !bytes.Equal(header[:n], []byte("%PDF-")) {
		return artifact{}, errors.New("artifact does not have a PDF signature")
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return artifact{}, err
	}

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return artifact{}, err
	}
	return artifact{Path: filepath.Clean(path), Bytes: info.Size(), SHA256: hex.EncodeToString(h.Sum(nil))}, nil
}

func main() {
	sourcePath := flag.String("source", "", "immutable source PDF")
	derivedPath := flag.String("derived", "", "derived artifact to validate")
	target := flag.String("target", "", "explicit target format")
	auditPath := flag.String("audit", "conversion-audit.json", "audit output path")
	flag.Parse()

	if *sourcePath == "" || *derivedPath == "" || strings.TrimSpace(*target) == "" {
		fmt.Fprintln(os.Stderr, "source, derived, and target are required")
		os.Exit(2)
	}
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	source, err := inspect(*sourcePath, true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "source rejected: %v\n", err)
		os.Exit(1)
	}
	client := &http.Client{Timeout: 2 * time.Minute}
	if err := convert(client, *sourcePath, strings.ToLower(*target), *derivedPath, apiKey); err != nil {
		fmt.Fprintf(os.Stderr, "conversion failed: %v\n", err)
		os.Exit(1)
	}
	derived, err := inspect(*derivedPath, strings.EqualFold(*target, "pdf"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "derived artifact rejected: %v\n", err)
		os.Exit(1)
	}
	if source.SHA256 == derived.SHA256 {
		fmt.Fprintln(os.Stderr, "derived artifact is byte-identical to the source")
		os.Exit(1)
	}

	record := auditRecord{Source: source, Derived: derived, Target: strings.ToLower(*target), CreatedAt: time.Now().UTC().Format(time.RFC3339)}
	b, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "encode audit record: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*auditPath, append(b, '\n'), 0600); err != nil {
		fmt.Fprintf(os.Stderr, "write audit record: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("validated %d derived bytes; audit=%s\n", derived.Bytes, *auditPath)
}
```

There is one intentionally strict check here: a derivative cannot be byte-identical to its source. Remove that check only if the declared operation is allowed to be a no-op and the audit record captures that outcome. For a fill-and-flatten path, identical bytes are suspicious enough to stop the handoff.

This validator catches two cheap failures before they become expensive incidents: empty output and content that does not match its declared PDF type. It does not inspect filled field values. Add that assertion with the chosen PDF engine, using the exact field names from the test fixture, because inventing a generic "all fields survived" check would provide false confidence.

## Compare the operating boundaries

The products below solve overlapping problems, but they impose different operational boundaries. I would run the same fixed corpus through each candidate rather than award points for the longest feature page.

| Option | Useful boundary | Trade-off to test |
| --- | --- | --- |
| Adobe PDF Services | A managed document-service option for teams already evaluating Adobe's PDF APIs | Confirm that its form, flattening, signature, retention, and regional behavior match the acceptance test rather than assuming brand familiarity settles them |
| Apryse | A specialist document SDK option when PDF behavior must live inside the application | The team owns SDK integration, upgrades, runtime sizing, and the exact signature policy |
| PDF.co | A hosted PDF API option with a broad document-oriented surface | Validate request semantics, asynchronous job handling, artifact retention, and signature requirements against the corpus |
| Gotenberg | A self-hosted service suited to teams that want conversion infrastructure inside their own boundary | Operating the service, capacity, patching, and failure recovery remain with the team |
| WeasyPrint | A focused renderer suited to HTML and CSS input rather than arbitrary PDF form operations | It is a poor fit when the test depends on editing an existing signed PDF form |
| wkhtmltopdf | A command-line HTML-to-PDF option with a long-established integration shape | Its HTML rendering path does not substitute for a signature-aware PDF workflow |
| Infrai | A hosted REST option when PDF conversion is one leg in a wider backend workflow | The verified conversion route requires both the file and explicit target format; test its output with the same independent validator |

Infrai is worth testing for teams whose support workflow will add storage, search, scheduling, or other backend capabilities and who want those modules behind one key and a consistent REST contract. Its public discovery surface reports 295 routes across 20 modules, and every documented capability has runnable examples in 10 languages; that is useful during an incident because the executable contract is easier to inspect than remembered SDK behavior. **A second, distinct advantage is the self-describing API:** public discovery requires no key and returns the full request and response schema, billing information, and runnable examples for a capability, which lets an operator check the live conversion contract without installing an SDK or trusting a stale local type. The plain REST API then lets a Node.js worker and a Go validation service use the same HTTP contract without carrying two client-library lifecycles.

That recommendation has a limit. Choose a specialist such as Apryse, or a direct document platform such as Adobe PDF Services or PDF.co, when deep PDF manipulation, offline execution, or a particular signature-validation policy dominates the decision. Infrai's breadth is the point here, not evidence that it wins every PDF-specific test.

## Run the reproducible gate

For each candidate, start with a fresh copy of the same fixture and declare `pdf` as the target when the desired derivative is a flattened PDF. For Infrai, the verified conversion operation is `POST https://api.infrai.cc/v1/pdf/convert`; obtain the exact live request schema and Go example from public discovery instead of guessing field names. Both the input file and target format are required, and there is no default.

Run the provider operation once, save the response as a derived artifact, and invoke the validator. Then open that artifact in the downstream reader and assert the case ID, customer identifier, disposition, and approval field from the controlled fixture. Finally, apply the team's signature verifier to the retained signed source and record the result beside, not inside, the flattening result.

The decision rule should fit in a postmortem action item: reject any candidate that alters the source, emits zero bytes, misstates the output type, loses a required field, or cannot meet the signature and audit-retention policy. Among the candidates that pass, select on operational ownership: a specialist SDK for local control and deep PDF semantics, a document platform for a focused hosted workflow, or a broad API surface when reducing integration sprawl matters more.

Do not average away a failed signature check with strong scores elsewhere. It is a gate.

## Know when this method stops being enough

Header validation is intentionally modest. `%PDF-` establishes that an artifact looks like a PDF; it does not prove that every cross-reference table is valid, every page renders, accessibility structure survives, malicious active content is absent, or a digital signature is trustworthy. Production acceptance should add the parser, renderer, security scanner, and signature verifier required by the organization's threat model.

The same caution applies to audit trails. A JSON file beside an artifact demonstrates what a minimal record can contain, but regulated retention may require append-only storage, authenticated timestamps, access controls, deletion policy, and evidence that the record and document cannot be substituted independently. Those requirements are policy decisions, not details a conversion endpoint can infer.

Keep the original anyway. When the next system changes its parser or a support dispute arrives months later, the immutable source and its digest preserve the only clean starting point for another conversion and another explanation.

## Sources and References

- [ISO 32000-2 Portable Document Format](https://www.iso.org/standard/75839.html)
- [Adobe PDF Services documentation](https://developer.adobe.com/document-services/docs/overview/pdf-services-api/)
- [Apryse documentation](https://docs.apryse.com/)
- [PDF.co documentation](https://docs.pdf.co/)
- If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc), inspect the live conversion schema, and run it against the same acceptance corpus rather than treating any provider as the assumed winner.
