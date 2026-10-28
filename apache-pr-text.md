# apache/tika PR text (drafts for Kristian, nothing posted)

Written 2026-10-28, refreshed 12:20, from the fork branches at these heads:

1. TIKA-4766, refresh of apache #2961: `TIKA-4766-document-contract-minimal` @ 230268723, against apache main edbfbf34b
2. TIKA-4795, new PR: `TIKA-4795-parseBytes` @ 23f3b400d, against layer 1
3. TIKA-4771 + TIKA-4951, new PR: `claude/project-thread-llxu8u` (fork #4) @ 560518a3b, against layer 2

The first line of each block is the title. Replaces the stale apache-pr-drafts.md.

---

## 1. TIKA-4766 (replace the #2961 title and body; Davide pastes this, written in his voice as the PR author)

```
TIKA-4766: typed Document contract for tika-grpc (experimental TikaV2Service)

This adds an experimental second gRPC service, TikaV2Service, next to the existing Tika service on the same port. Its FetchAndParse calls (unary, server streaming, bidi) return a typed Document instead of the v1 fields map: content type, source origin, parse status, the common Dublin Core fields as real types (timestamps, lists, ints), and every other metadata key in a tagged `extra` list that keeps all values. Extracted text is not in this change; that comes later.

v1 is not touched on the wire. tika.proto is byte-identical to main, and buf breaking against main is clean. The one v1 behaviour change is that a fetch failure's error_message is now cut to its first line, same as v2, unless parse-context.exception-reporting is set, so stack traces stop reaching callers by default. If you'd rather keep v1 exactly as it was, we can drop that part.

Where things live:

tika-grpc-api holds the v2 protos (document.proto, tika_v2.proto) and the generated classes, so clients can depend on the contract without the server. tika-grpc-mapper turns Tika Metadata into the Document (DocumentBuilder plus per-format DocumentTransformers). A value is typed only when Tika's Property declares the type or the pipes fork reports it (TIKA-4969); otherwise it is a string. tika-grpc gets TikaGrpcV2ServerImpl, registered beside the v1 impl.

Most of the recent work went into error handling on v2. A busy pool, a pipes runtime or fetcher that failed to start, and an unknown fetcher close the call with RESOURCE_EXHAUSTED, UNAVAILABLE or NOT_FOUND, so client retry policies keyed on status codes work. A document that timed out, ran out of memory or crashed its worker is still a normal reply with a STATUS_FAILED Document and the reason in errors, because that's a fact about the document, not the server. Errors are one line, at most 1024 characters, unless exception-reporting is configured. A cancelled or expired call stops waiting right away, but a parse already running on a worker is allowed to finish and its result dropped; interrupting it restarted the worker JVM in per-client mode. grpc.cancelRunningParse=true brings the interrupt back for anyone who wants it.

The v2 protos pass buf lint STANDARD with no exceptions. emitter_id is reserved on the v2 requests since v2 returns the Document and never emits. There is also a new .github/workflows/buf.yml, which runs buf lint on both proto modules and buf breaking against the PR base. v1 gets its own tika-grpc/buf.yaml that turns off the six naming rules it can't meet without renaming its wire API, each with a comment. The point is to keep v1 frozen and put changes in v2.

Testing: `mvn clean verify -pl tika-grpc-api,tika-grpc-mapper,tika-grpc` passes (57 tika-grpc tests, 37 mapper tests). The tika-e2e-tests HandlerTypeTest gets a PDF case that checks the typed fields and the v1 reply through a live server.

Interaction with TIKA-5001: this conflicts with Tim's step 2 (#3440) and step 3 (#3441) in TikaGrpcServerImpl and CHANGES.txt, and with #3441 in TikaGrpcServerTest. Fetcher lookup in both the v1 and v2 parse paths now goes through one method, resolveFetcherId, so when getFetcher starts returning a lease there is a single place to release it. Whichever lands second has to rebase. If Tim's go first, we'll do the merge here and adjust the tests.

Good places to start reviewing: document.proto, then DocumentBuilder and MetadataTagger in the mapper, then the status handling in TikaGrpcV2ServerImpl.

ParseBytes (TIKA-4795) and the external parser work (TIKA-4771/TIKA-4951) are separate PRs stacked on this one. Kristian (krickert) co-authored this one and did most of the recent rounds: the v2 status codes and cancel handling, the buf setup, and the resolveFetcherId change for TIKA-5001.
```

---

## 2. TIKA-4795 (new PR, base = the #2961 branch until it merges)

```
TIKA-4795: ParseBytes, parse bytes the caller already has

Depends on #2961 (TIKA-4766). Until that merges, the diff here includes it; the commits for this change start at "add ParseBytes to the experimental TikaV2 service".

Davide (dpol1) started this and wrote the first version of the RPC and the spool. I added the admission control, the failure handling and the tests on top.

TikaV2Service.ParseBytes takes the bytes in the request and returns the same Document that FetchAndParse does. Nothing is fetched. The use case is a crawler that already downloaded a page: it wants a parse of exactly what it captured, not a second fetch that might get a redirect, a different cookie, or a different page. The request carries a correlation_id (returned as Document.id), an optional resource_name for detection, and source_uri, effective_uri and base_uri, which Tika echoes in Document.origin and never dereferences. The proto change is additive: the new RPC, its request and response, and four new SourceOrigin fields.

The bytes go to pipes the way a tika-server request body does: inline up to pipes.maxInlineBytes, above that through a temp file read by a file-system fetcher registered under a reserved "__" id. The fetcher RPCs treat "__" ids as unknown and SaveFetcher refuses them. Since TIKA-5001 step 1 that spool fetcher comes from the config file and never enters the config store.

Limits, since this is the obvious way to hurt a server. parseBytesMaxContentBytes defaults to 256 MiB, because 100 MB+ WARCs from crawls are normal; over it is RESOURCE_EXHAUSTED. If maxInboundMessageBytes is not set, the gRPC inbound limit becomes the content limit plus 1 MiB. parseBytesMaxConcurrent (2x pipes.numClients by default) caps requests in progress, and one over the cap gets RESOURCE_EXHAUSTED before its bytes are spooled. The server warns at startup if that many full-size requests could outgrow the heap or the spool directory's free space. Empty content is INVALID_ARGUMENT.

Two things ParseBytes does that FetchAndParse can't, because it holds the bytes. When a parse fails before recording a digest or a content type, the server fills origin.sha256 itself and detects the type from magic bytes and the name; for gzip it looks inside, so a .warc.gz is application/warc+gz. The small change for that is in tika-core's GZipSpecializationDetector, which now inflates at most 64 bytes of its prefix. It skips container detection since this runs in the server JVM. Second, if the caller sets truncated=true, a success is reported as STATUS_PARTIAL with an error saying so. A PDF cut before its trailer parses "successfully" and silently loses its Info dictionary; Tika can't see that, the crawler can. pipes_status keeps the raw value.

Testing: `mvn clean verify -pl tika-grpc-api,tika-grpc-mapper,tika-grpc` passes (122 tika-grpc, 37 mapper tests). They cover parity with FetchAndParse on the same file, the inline and spooled paths, the size and concurrency limits, cancel, the fallback digest and type, and truncation. buf lint and breaking are clean against #2961. Davide also ran it from StormCrawler with his demo (https://github.com/dpol1/tika-demo): one GET per URL, the client's SHA-256 matching origin.sha256, and Tika's fields reaching the indexer.

To review first: ParseBytesRequest in tika_v2.proto, admitParseBytes and the permit in TikaGrpcServerImpl, and ParseBytesSpool.

Not in here: cancelling the forked parse itself when a caller goes away (it finishes and is dropped, same as FetchAndParse in #2961).
```

---

## 3. TIKA-4771 + TIKA-4951 (new PR, base = the ParseBytes branch)

```
TIKA-4771, TIKA-4951: external parser adapters for tika-grpc ParseBytes

Depends on the ParseBytes PR (TIKA-4795), which depends on #2961. The commit for this change is the top one.

This lets ParseBytes send the same bytes to external gRPC parsers, written in any language, and fold what they return into Tika's own typed Document. TIKA-4771 is the envelope and composition; TIKA-4951 (FastWARC) is the first parser it was built for. Unlike the first prototype, there's no gRPC reflection and no JSON request building. Each parser comes with an adapter jar that has the parser's generated stubs and one ExternalParserAdapter, found with ServiceLoader. Drop the jar on the classpath, add an entry under grpc.externalParsers, and it's used for the content types the adapter names.

New pieces:

tika-grpc-adapter-api is a small module with the SPI: ExternalParserAdapter (producer id, content types, a unary MethodDescriptor, a key prefix, options check, build request, map reply) and AdapterContribution, an immutable result built through a builder that applies the size limits as the adapter writes to it. document.proto gains Document.extensions (40), one ExtensionResult per parser with status (OK, EMPTY, FAILED, SKIPPED, PARTIAL), warnings, timing, and reported/kept/truncated counts for children, and Document.embedded (41), a flat EmbeddedDocument list for things like WARC records. EmbeddedDocument is deliberately not a Document: no id and no nesting. In the mapper, AdapterMerge applies a contribution to Tika's Document and DocumentComposer puts it together. In tika-grpc, the compose package loads adapters, opens channels (TLS by default, mTLS optional, plaintext only when asked) and runs them after Tika's parse.

The trust model is the thing to look at closely. An adapter may add child documents it authors itself and parent keys under its own prefix (warc: for FastWARC), and a parent key only goes in where Tika recorded nothing under that name. It never changes Tika's core fields, status, origin, id or Tika's own children. Prefixes that overlap Tika's namespaces (tk:, X-TIKA, dc:, Content-, pdf: and the rest, taken from tika-core's metadata classes) or each other are refused at startup, ignoring case. An adapter that fails, throws or times out gives a STATUS_FAILED slot with a one-line reason, and Tika's Document still comes back untouched. Adapters run in the server JVM, so they're trusted code; the limits bound what the server keeps from them, not what they do. That's written down in the README.

Limits: maxReplyBytes per parser (128 MiB by default, separate from the server's inbound limit), per reply at most 100,000 children and 64 MiB of embedded documents, plus caps on keys and value length, and one externalParsersMaxAddedBytes budget (96 MiB) across all parsers. Those defaults are sized for a 256 MiB WARC. When a cap cuts the child list, the slot is marked truncated with the counts, so nobody takes a partial list for the whole archive. The ParseBytes admission permit is held until the composed reply is sent, so parseBytesMaxConcurrent covers the external calls too. Each call's deadline is the earlier of the client's deadline and the entry's timeoutMs, and a client cancel stops the remaining calls.

Out of scope on purpose: streaming external methods (unary only until it's measured), FetchAndParse (the server never holds the fetched bytes), Tika's own embedded documents in Document.embedded, and any mode where an adapter replaces Tika's parse.

The FastWARC adapter is not in this PR. It's one implementation of the SPI and doesn't belong in Tika; it lives on its own branch as the working example: https://github.com/ai-pipestream/tika/pull/6. I ran it against fastwarc-grpc 1.1.0 on my machine with a 50-record WARC, both .warc and .warc.gz (and an unnamed .warc.gz, routed by its bytes): 50 embedded records matching FastWARC's own output, slot OK, and with FastWARC stopped, Tika's Document came back intact with a FAILED slot.

Testing: `mvn clean verify -pl tika-grpc-api,tika-grpc-mapper,tika-grpc-adapter-api,tika-grpc` passes (168 tika-grpc, 60 mapper, 10 adapter-api tests). The tika-grpc tests use a test-only adapter over a small records.proto in src/test/proto, so nothing depends on FastWARC, plus a WARC compose test that goes through a real forked worker. buf lint and breaking are clean against the ParseBytes branch.

Where to start: ExternalParserAdapter and AdapterContribution, then AdapterMerge (the merge rules), then ExternalParsers in tika-grpc.
```

---

## Branch state (12:20)

All earlier mismatches are fixed: adapter defaults in code with a test, ParseBytes CHANGES/proto/README numbers, PR 3 CHANGES.txt entry, tika-grpc-adapter-api in tika-bom, README example raises both limits together. FastWARC fork #6 = 620aa358d (11 tests). Text is final pending Kristian's wording.
