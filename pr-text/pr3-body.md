Depends on the ParseBytes PR. ParseBytes can now call external gRPC parsers through drop-in adapter jars (a new tika-grpc-adapter-api SPI, found with ServiceLoader, no reflection) and merge what they return into Tika's Document as embedded documents (Document.embedded) and prefixed parent keys, with one ExtensionResult per parser.

Adapters can add their own child documents and keys under their own prefix. They never change Tika's fields, status or children, and a failing adapter never fails the request. They run in the server JVM, so they're trusted code.

Design, trust model, limits and how to write an adapter: tika-grpc/docs/external-parser-adapters.md. How all three PRs fit together: tika-grpc/docs/architecture.md. Start with ExternalParserAdapter, AdapterContribution and AdapterMerge.

The FastWARC adapter is the working example and isn't part of this PR: https://github.com/ai-pipestream/tika/pull/6. I ran it against fastwarc-grpc 1.1.0 with a 50-record WARC (all records matched FastWARC's own output).

Tested with `mvn clean verify -pl tika-grpc-api,tika-grpc-mapper,tika-grpc-adapter-api,tika-grpc` (168 tika-grpc, 60 mapper, 10 adapter-api tests), using a test-only adapter so nothing depends on FastWARC.
