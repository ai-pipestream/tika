# tika-grpc-adapter-fastwarc

An external parser adapter for tika-grpc (TIKA-4951): it calls
[fastwarc-grpc](https://github.com/ai-pipestream)'s `fastwarc.v1.WarcService/ParseArchive`
from ParseBytes and maps each WARC record into Tika's Document as an embedded
document, keyed as Tika's own `WARCParser` keys a record.

This adapter is its own project built on the adapter SPI (`tika-grpc-adapter-api`).
It is not part of upstream Tika or of the default build: in this fork it builds only
with the `fastwarc-adapter` profile:

```
mvn clean verify -Pfastwarc-adapter -pl tika-grpc-adapter-api,tika-grpc-adapter-fastwarc
```

It could as well live in its own repository; it needs nothing from Tika but the SPI jar.

## Requirements

- **fastwarc-grpc 1.1.0 or later.** The adapter compiles the `fastwarc.v1` protos of
  1.1.0 (`src/main/proto`); earlier releases do not serve `ParseArchive` with this
  request shape.
- This jar on tika-grpc's classpath, and an entry in the `grpc` section:

```json
"externalParsers": [
  {"producerId": "fastwarc", "target": "localhost:50060", "plaintext": true,
   "options": {"verifyDigests": true}}
]
```

Options: `verifyDigests` and `quirksMode`, both booleans, passed to FastWARC.

## Mapping

| FastWARC | Key on the record |
|---|---|
| WARC headers | `warc:<header name>` |
| `record_id` | `resourceName` |
| `record_date` | `dcterms:created` |
| `stream_pos` | `warc:stream-pos` |
| response status line | `warc:http:status`, `warc:http:status:reason` |
| request line | `warc:http:method`, `warc:http:target` |
| HTTP headers | `warc:http:<header name>` |
| HTTP content type and charset | `Content-Type`, `warc:payload-content-type` |

The parent gets `warc:record-count`, the number of records FastWARC returned, but only
when FastWARC read the whole archive: an unrecoverable error leaves it out, since the
archive may hold more. Records are `/record-1`, `/record-2`, ... in FastWARC's order.
Payloads are not requested, so a record has no `Content-Length` of its own; the HTTP
header is kept as `warc:http:Content-Length`.

## Malformed records

A malformed record costs that record, not the archive:

- a record that cannot be mapped is left out, with a warning counting such records;
- a header name that is not an RFC 9110 token is skipped, with a warning;
- a status line splits on any run of spaces or tabs, and needs a three-digit code, else
  no status is set and a warning says so;
- a charset that is not a token is not added to the content type, with a warning; one
  already in the content type (any case or spacing) is left as it is;
- a WARC-Date out of range is left out, with a warning.

FastWARC's own recoverable errors become warnings. An unrecoverable one after some
records makes the slot `STATUS_PARTIAL` and keeps them; one before any record fails it.
