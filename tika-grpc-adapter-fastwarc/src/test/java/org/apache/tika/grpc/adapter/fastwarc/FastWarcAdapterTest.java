/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tika.grpc.adapter.fastwarc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import org.apache.tika.grpc.adapter.AdapterContribution;
import org.apache.tika.grpc.adapter.AdapterRequest;
import org.apache.tika.grpc.adapter.ChildDraft;
import org.apache.tika.grpc.adapter.ContributionLimits;
import org.apache.tika.grpc.adapter.ExternalParserAdapter;
import org.apache.tika.grpc.adapter.fastwarc.v1.HeaderBlock;
import org.apache.tika.grpc.adapter.fastwarc.v1.HeaderField;
import org.apache.tika.grpc.adapter.fastwarc.v1.ParseArchiveRequest;
import org.apache.tika.grpc.adapter.fastwarc.v1.ParseArchiveResponse;
import org.apache.tika.grpc.adapter.fastwarc.v1.ParsedRecord;
import org.apache.tika.grpc.adapter.fastwarc.v1.RecordError;
import org.apache.tika.grpc.adapter.fastwarc.v1.RecordMetadata;
import org.apache.tika.grpc.adapter.fastwarc.v1.WarcServiceGrpc;
import org.apache.tika.metadata.HttpHeaders;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;

class FastWarcAdapterTest {

    private static HeaderField field(String name, String value) {
        return HeaderField.newBuilder()
                .setName(ByteString.copyFromUtf8(name))
                .setValue(ByteString.copyFromUtf8(value))
                .build();
    }

    private static final ParsedRecord RESPONSE = ParsedRecord.newBuilder()
            .setMetadata(RecordMetadata.newBuilder()
                    .setRecordId("<urn:uuid:2>")
                    .setRecordDate(Timestamp.newBuilder().setSeconds(1_800_000_000L))
                    .setStreamPos(4096)
                    .setWarcHeaders(HeaderBlock.newBuilder()
                            .addFields(field("WARC-Type", "response"))
                            .addFields(field("WARC-Target-URI", "https://example.com/"))
                            .addFields(field("Content-Type", "application/http; msgtype=response")))
                    .setHttpHeaders(HeaderBlock.newBuilder()
                            .setStatusLine(ByteString.copyFromUtf8("HTTP/1.1 404 Not Found"))
                            .addFields(field("Server", "nginx"))
                            .addFields(field("Bad Name\r\nX", "smuggled")))
                    .setHttpContentType("text/html")
                    .setHttpCharset("utf-8"))
            .build();

    private static AdapterRequest request(ByteString content) {
        return new AdapterRequest(content, new Metadata(), "crawl.warc", "application/warc",
                JsonNodeFactory.instance.objectNode().put("quirksMode", true),
                ContributionLimits.DEFAULTS);
    }

    @Test
    void aRecordIsKeyedAsTikasWarcParserKeysIt() {
        AdapterContribution contribution = new FastWarcAdapter().map(
                ParseArchiveResponse.newBuilder().addRecords(RESPONSE).build(),
                request(ByteString.EMPTY));

        assertEquals(List.of("1"), contribution.parentKeys().get("warc:record-count"));
        assertEquals(1, contribution.parentKeys().size(), "only warc: keys on the parent");
        assertEquals(1, contribution.children().size());
        ChildDraft draft = contribution.children().get(0);
        assertEquals("/record-1", draft.path());
        Metadata record = draft.metadata();
        assertEquals("<urn:uuid:2>", record.get(TikaCoreProperties.RESOURCE_NAME_KEY));
        assertEquals("response", record.get("warc:WARC-Type"));
        assertEquals("https://example.com/", record.get("warc:WARC-Target-URI"));
        assertEquals("application/http; msgtype=response",
                record.get("warc:record-content-type"));
        assertEquals("404", record.get("warc:http:status"));
        assertEquals("Not Found", record.get("warc:http:status:reason"));
        assertEquals("nginx", record.get("warc:http:Server"));
        assertEquals("text/html; charset=utf-8", record.get(HttpHeaders.CONTENT_TYPE));
        assertEquals("text/html; charset=utf-8", record.get("warc:payload-content-type"));
        assertEquals("4096", record.get(FastWarcAdapter.STREAM_POS));
        assertEquals(1_800_000_000_000L, record.getDate(TikaCoreProperties.CREATED).getTime());
        assertNull(record.get(HttpHeaders.CONTENT_LENGTH), "no payload was asked for");
        assertNull(record.get("warc:http:Bad Name\r\nX"));
        assertEquals(List.of("record 1: skipped 1 headers whose names are not tokens"),
                contribution.warnings());
        assertEquals(AdapterContribution.Outcome.OK, contribution.outcome());
    }

    /** A request record's first line is a request line, not a status line. */
    @Test
    void aRequestRecordKeepsItsMethodAndTargetNotAStatus() {
        ParsedRecord request = ParsedRecord.newBuilder()
                .setMetadata(RecordMetadata.newBuilder()
                        .setWarcHeaders(HeaderBlock.newBuilder()
                                .addFields(field("WARC-Type", "request"))
                                .addFields(field("Content-Type",
                                        "application/http; msgtype=request")))
                        .setHttpHeaders(HeaderBlock.newBuilder()
                                .setStatusLine(ByteString.copyFromUtf8("GET /x?q=1 HTTP/1.1"))
                                .addFields(field("Host", "example.com"))))
                .build();

        AdapterContribution contribution = new FastWarcAdapter().map(
                ParseArchiveResponse.newBuilder().addRecords(request).build(),
                request(ByteString.EMPTY));

        Metadata record = contribution.children().get(0).metadata();
        assertEquals("GET", record.get("warc:http:method"));
        assertEquals("/x?q=1", record.get("warc:http:target"));
        assertEquals("example.com", record.get("warc:http:Host"));
        assertNull(record.get("warc:http:status"));
        assertNull(record.get("warc:http:status:reason"));
        assertEquals(List.of(), contribution.warnings());
    }

    @Test
    void recoverableErrorsWarnAndOthersEndThePartWay() {
        AdapterContribution partial = new FastWarcAdapter().map(
                ParseArchiveResponse.newBuilder()
                        .addRecords(RESPONSE)
                        .addErrors(RecordError.newBuilder().setStreamPos(10).setRecoverable(true)
                                .setMessage("bad digest"))
                        .addErrors(RecordError.newBuilder().setStreamPos(20)
                                .setMessage("truncated"))
                        .build(), request(ByteString.EMPTY));

        assertTrue(partial.warnings().contains("record at byte 10: bad digest"),
                partial.warnings().toString());
        assertEquals(AdapterContribution.Outcome.PARTIAL, partial.outcome());
        assertEquals("record at byte 20: truncated", partial.reason());
        assertEquals(1, partial.children().size(), "the record before the error is kept");

        AdapterContribution failed = new FastWarcAdapter().map(
                ParseArchiveResponse.newBuilder()
                        .addErrors(RecordError.newBuilder().setMessage("not a WARC"))
                        .build(), request(ByteString.EMPTY));
        assertEquals(AdapterContribution.Outcome.FAILED, failed.outcome());
    }

    @Test
    void optionsAreCheckedAtStartup() {
        FastWarcAdapter adapter = new FastWarcAdapter();
        adapter.validateOptions(JsonNodeFactory.instance.objectNode()
                .put("verifyDigests", true).put("quirksMode", false));
        adapter.validateOptions(JsonNodeFactory.instance.objectNode());
        assertThrows(IllegalArgumentException.class, () -> adapter.validateOptions(
                JsonNodeFactory.instance.objectNode().put("verifyDigest", true)));
        assertThrows(IllegalArgumentException.class, () -> adapter.validateOptions(
                JsonNodeFactory.instance.objectNode().put("quirksMode", "yes")));
    }

    @Test
    void theRequestAsksForHeadersNotPayloads() {
        ByteString bytes = ByteString.copyFromUtf8("WARC/1.1");

        ParseArchiveRequest request = new FastWarcAdapter().buildRequest(request(bytes));

        assertEquals(bytes, request.getArchive());
        assertTrue(request.getConfig().getParseHttp());
        assertTrue(request.getConfig().getIncludeHeaders());
        assertTrue(request.getConfig().hasIncludePayload());
        assertFalse(request.getConfig().getIncludePayload());
        assertTrue(request.getConfig().getQuirksMode());
        assertFalse(request.getConfig().getVerifyDigests());
    }

    /**
     * The adapter's method, request and reply over a real gRPC call: an in-process server
     * stands in for fastwarc-grpc, so the method's full name and the message encodings are
     * the ones that cross the wire.
     */
    @Test
    void aParseArchiveCallRoundTripsThroughTheAdapter() throws Exception {
        FastWarcAdapter adapter = new FastWarcAdapter();
        assertEquals("fastwarc.v1.WarcService/ParseArchive",
                adapter.method().getFullMethodName(), "the path fastwarc-grpc serves");
        AtomicReference<ParseArchiveRequest> seen = new AtomicReference<>();
        String name = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new WarcServiceGrpc.WarcServiceImplBase() {
                    @Override
                    public void parseArchive(ParseArchiveRequest request,
                                             StreamObserver<ParseArchiveResponse> reply) {
                        seen.set(request);
                        reply.onNext(ParseArchiveResponse.newBuilder()
                                .addRecords(RESPONSE)
                                .addErrors(RecordError.newBuilder().setStreamPos(77)
                                        .setRecoverable(true).setMessage("bad digest"))
                                .build());
                        reply.onCompleted();
                    }
                }).build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        try {
            ByteString archive = ByteString.copyFromUtf8("WARC/1.1\r\n");
            AdapterRequest request = request(archive);
            ParseArchiveResponse reply = ClientCalls.blockingUnaryCall(channel,
                    adapter.method(), CallOptions.DEFAULT, adapter.buildRequest(request));
            AdapterContribution contribution = adapter.map(reply, request);

            assertEquals(archive, seen.get().getArchive());
            assertTrue(seen.get().getConfig().getQuirksMode());
            assertFalse(seen.get().getConfig().getIncludePayload());
            assertEquals(AdapterContribution.Outcome.OK, contribution.outcome());
            assertEquals(List.of("1"), contribution.parentKeys().get("warc:record-count"));
            Metadata record = contribution.children().get(0).metadata();
            assertEquals("404", record.get("warc:http:status"));
            assertEquals("<urn:uuid:2>", record.get(TikaCoreProperties.RESOURCE_NAME_KEY));
            assertTrue(contribution.warnings().contains("record at byte 77: bad digest"));
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void theJarDeclaresTheAdapterForServiceLoader() {
        List<String> found = new ArrayList<>();
        for (ExternalParserAdapter<?, ?> adapter : ServiceLoader.load(ExternalParserAdapter.class)) {
            found.add(adapter.producerId());
        }
        assertEquals(List.of(FastWarcAdapter.PRODUCER_ID), found);
    }

    private static ParsedRecord withHttp(String statusLine, String contentType, String charset) {
        HeaderBlock.Builder http = HeaderBlock.newBuilder();
        if (statusLine != null) {
            http.setStatusLine(ByteString.copyFromUtf8(statusLine));
        }
        RecordMetadata.Builder metadata = RecordMetadata.newBuilder().setHttpHeaders(http);
        if (contentType != null) {
            metadata.setHttpContentType(contentType);
        }
        if (charset != null) {
            metadata.setHttpCharset(charset);
        }
        return ParsedRecord.newBuilder().setMetadata(metadata).build();
    }

    private static AdapterContribution mapOne(ParsedRecord record) {
        return new FastWarcAdapter().map(
                ParseArchiveResponse.newBuilder().addRecords(record).build(),
                request(ByteString.EMPTY));
    }

    @Test
    void aStatusLineSplitsOnAnySpacingAndNeedsAThreeDigitCode() {
        Metadata tabs = mapOne(withHttp("HTTP/1.1\t200   OK", null, null))
                .children().get(0).metadata();
        assertEquals("200", tabs.get("warc:http:status"));
        assertEquals("OK", tabs.get("warc:http:status:reason"));

        AdapterContribution bad = mapOne(withHttp("HTTP/1.1 2OO <script>", null, null));
        Metadata record = bad.children().get(0).metadata();
        assertNull(record.get("warc:http:status"));
        assertNull(record.get("warc:http:status:reason"));
        assertEquals(List.of("record 1: HTTP status line has no three-digit status code"),
                bad.warnings());
    }

    @Test
    void aCharsetIsAddedOnlyWhenItIsATokenAndNotAlreadyThere() {
        assertEquals("text/html; Charset = ISO-8859-1", mapOne(
                withHttp(null, " text/html; Charset = ISO-8859-1 ", "utf-8"))
                .children().get(0).metadata().get(HttpHeaders.CONTENT_TYPE));

        AdapterContribution injected = mapOne(
                withHttp(null, "text/html", "utf-8\r\nSet-Cookie: x"));
        assertEquals("text/html", injected.children().get(0).metadata()
                .get(HttpHeaders.CONTENT_TYPE));
        assertEquals(List.of("record 1: charset is not a token, left out"),
                injected.warnings());

        assertNull(mapOne(withHttp(null, "  ", "utf-8")).children().get(0).metadata()
                .get(HttpHeaders.CONTENT_TYPE), "a blank content type is left out");
    }

    @Test
    void aWarcDateOutOfRangeIsLeftOutNotFatal() {
        ParsedRecord record = ParsedRecord.newBuilder()
                .setMetadata(RecordMetadata.newBuilder()
                        .setRecordDate(Timestamp.newBuilder().setSeconds(Long.MAX_VALUE)))
                .build();

        AdapterContribution contribution = mapOne(record);

        assertEquals(1, contribution.children().size());
        assertNull(contribution.children().get(0).metadata().get(TikaCoreProperties.CREATED));
        assertEquals(List.of("record 1: WARC-Date out of range, left out"),
                contribution.warnings());
    }

    /** The count says how many records the archive has, so only a complete read sets it. */
    @Test
    void theRecordCountIsLeftOutWhenFastWarcStoppedPartWay() {
        AdapterContribution partial = new FastWarcAdapter().map(
                ParseArchiveResponse.newBuilder()
                        .addRecords(RESPONSE)
                        .addErrors(RecordError.newBuilder().setStreamPos(20)
                                .setMessage("truncated"))
                        .build(), request(ByteString.EMPTY));

        assertNull(partial.parentKeys().get("warc:record-count"));
        assertEquals(1, partial.reportedChildren());
    }
}
