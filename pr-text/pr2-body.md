Depends on #2961. Adds TikaV2Service.ParseBytes: the caller sends the bytes it holds and gets the same Document back, with nothing fetched. It's for crawlers that want a parse of exactly what they captured. The proto change is additive.

Content is capped (256 MiB by default) and concurrency is capped, with RESOURCE_EXHAUSTED before anything is spooled. When a parse fails, ParseBytes still fills in the digest and content type, and a caller can mark content as truncated. There's a small change to tika-core's GZipSpecializationDetector.

Design and reasoning: tika-grpc/docs/parse-bytes.md. Start with ParseBytesRequest, admitParseBytes and ParseBytesSpool.

Tested with `mvn clean verify -pl tika-grpc-api,tika-grpc-mapper,tika-grpc` (122 tika-grpc tests) and from StormCrawler with https://github.com/dpol1/tika-demo.

Davide (dpol1) started this and wrote the first version.
