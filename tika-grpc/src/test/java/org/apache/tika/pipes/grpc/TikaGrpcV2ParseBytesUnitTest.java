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
package org.apache.tika.pipes.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.grpc.v2.ParseBytesReply;
import org.apache.tika.grpc.v2.ParseBytesRequest;
import org.apache.tika.grpc.v2.SourceOrigin;
import org.apache.tika.metadata.HttpHeaders;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.pipes.api.FetchEmitTuple;
import org.apache.tika.serialization.config.JsonConfigHelper;

/**
 * ParseBytes contract coverage at the unit tier: the v1 collaborator is mocked, so no
 * forked worker and no gRPC server are involved.
 */
class TikaGrpcV2ParseBytesUnitTest {

    private TikaGrpcServerImpl v1;
    private TikaGrpcV2ServerImpl v2;

    @BeforeEach
    void setUp() {
        v1 = Mockito.mock(TikaGrpcServerImpl.class);
        Mockito.when(v1.denyPerRequestConfig(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(false);
        // The TIKA-4848 gate lives on v1; a mock would answer null and close the call.
        Mockito.when(v1.buildRequestParseContext(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any())).thenReturn(new ParseContext());
        v2 = new TikaGrpcV2ServerImpl(v1);
    }

    static final class RecordingObserver implements StreamObserver<ParseBytesReply> {
        final List<ParseBytesReply> replies = new ArrayList<>();
        final List<Throwable> errors = new ArrayList<>();
        int completions = 0;

        @Override
        public void onNext(ParseBytesReply reply) {
            replies.add(reply);
        }

        @Override
        public void onError(Throwable t) {
            errors.add(t);
        }

        @Override
        public void onCompleted() {
            completions++;
        }
    }

    private static Metadata htmlMetadata() {
        Metadata primary = new Metadata();
        primary.set(HttpHeaders.CONTENT_TYPE, "text/html");
        return primary;
    }

    private TikaGrpcServerImpl.ParseBytesOutcome outcome() {
        return new TikaGrpcServerImpl.ParseBytesOutcome(
                htmlMetadata(), "PARSE_SUCCESS", 5L);
    }

    /** runParseBytes consumes a stream, so the fixtures hand it one. */
    private static InputStream bytes(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }

    private static ParseBytesRequest.Builder requestWithContent() {
        return ParseBytesRequest.newBuilder()
                .setContent(ByteString.copyFromUtf8("<html><body>u</body></html>"));
    }

    /**
     * INV-NO-INTERNAL-ID: with no correlation id, nothing in the reply is derived from
     * the internal spool name. Document.id is the caller's key or nothing (see
     * document.proto); it is never minted by the server.
     */
    @Test
    void noReplyFieldIsDerivedFromTheSpoolName() throws Exception {
        Mockito.when(v1.runParseBytes(Mockito.any(), Mockito.anyLong(), Mockito.any(), Mockito.any()))
                .thenReturn(outcome());
        RecordingObserver observer = new RecordingObserver();

        v2.parseBytes(requestWithContent().build(), observer);

        assertEquals(1, observer.replies.size());
        assertFalse(observer.replies.get(0).toString().contains("parse-bytes-"),
                "no reply field may be derived from the internal spool name");
    }

    /** An opaque id echoes verbatim: whitespace is a value, not absence. */
    @Test
    void whitespaceCorrelationEchoesVerbatim() throws Exception {
        Mockito.when(v1.runParseBytes(Mockito.any(), Mockito.anyLong(), Mockito.any(), Mockito.any()))
                .thenReturn(outcome());
        RecordingObserver observer = new RecordingObserver();

        v2.parseBytes(requestWithContent().setCorrelationId("  ").build(), observer);

        assertEquals(1, observer.replies.size(), "exactly one reply expected");
        assertEquals("  ", observer.replies.get(0).getCorrelationId(),
                "the reply echoes the opaque id verbatim");
        assertEquals("  ", observer.replies.get(0).getDocument().getId(),
                "Document.id carries the same verbatim id");
    }

    /** The caller's provenance and the byte count reach origin untouched. */
    @Test
    void originCarriesTheCallersProvenanceAndTheByteCount() throws Exception {
        Mockito.when(v1.runParseBytes(Mockito.any(), Mockito.anyLong(), Mockito.any(), Mockito.any()))
                .thenReturn(outcome());
        RecordingObserver observer = new RecordingObserver();
        ParseBytesRequest request = requestWithContent()
                .setSourceUri("http://h/a")
                .setEffectiveUri("http://h/b")
                .setBaseUri("http://h/")
                .setTruncated(true)
                .build();

        v2.parseBytes(request, observer);

        assertEquals(1, observer.replies.size());
        SourceOrigin origin = observer.replies.get(0).getDocument().getOrigin();
        assertEquals(request.getContent().size(), origin.getByteSize(),
                "byte_size is the size of what the caller sent");
        assertEquals("http://h/a", origin.getSourceUri());
        assertEquals("http://h/b", origin.getEffectiveUri());
        assertEquals("http://h/", origin.getBaseUri());
        assertTrue(origin.getTruncated(), "the caller's truncation flag comes back as is");
    }

    /**
     * INV-TERMINAL: an interrupted round trip still closes the RPC. runParseBytes returns
     * null when the calling thread was interrupted before a reply could be built; with no
     * terminal signal the call stays open until the client's deadline, or forever if the
     * client set none. No Document is invented either: without a pipes result there is no
     * status the server could honestly report.
     */
    @Test
    void interruptedRoundTripStillTerminatesTheRpc() throws Exception {
        Mockito.when(v1.runParseBytes(Mockito.any(), Mockito.anyLong(), Mockito.any(), Mockito.any()))
                .thenReturn(null);
        RecordingObserver observer = new RecordingObserver();

        v2.parseBytes(requestWithContent().build(), observer);

        assertTrue(observer.replies.isEmpty(),
                "no Document may be invented without a pipes result");
        assertEquals(0, observer.completions,
                "an interrupted round trip is not a successful completion");
        assertEquals(1, observer.errors.size(),
                "exactly one terminal signal must close the call");
        assertEquals(Status.Code.UNAVAILABLE,
                Status.fromThrowable(observer.errors.get(0)).getCode(),
                "the interrupt comes from the server going down, so the client may retry");
    }

    /**
     * INV-BOUND-REJECTED-ONCE (a): the cap is checked where the declared size arrives,
     * so it costs nothing to exercise. A four-byte stream with a size declared past the
     * bound reaches the check without allocating 64 MiB anywhere.
     */
    @Test
    void contentAboveTheBoundIsRejectedBeforeAnythingIsAllocated(@TempDir Path tmp)
            throws Exception {
        TikaGrpcServerImpl real = realImpl(tmp);
        try {
            assertThrows(TikaGrpcServerImpl.ParseBytesTooLargeException.class, () ->
                    real.runParseBytes(bytes("<x/>"),
                            TikaGrpcConfig.DEFAULT_PARSE_BYTES_MAX_CONTENT_BYTES + 1L,
                            "a.html", null));

            try (var files = Files.list(real.parseBytesDir)) {
                assertTrue(files.findAny().isEmpty(),
                        "a rejected request must not have created a spool file");
            }
        } finally {
            real.postShutdown();
        }
    }

    /**
     * The content cap is configuration, not a constant: a configured
     * {@code parseBytesMaxContentBytes} governs the check in place of the 64 MiB
     * default. A tiny configured cap rejects a declared size the default would accept,
     * still without allocating or spooling anything.
     */
    @Test
    void theContentCapComesFromTheConfig(@TempDir Path tmp) throws Exception {
        TikaGrpcServerImpl real = realImpl(tmp, 1024L);
        try {
            assertThrows(TikaGrpcServerImpl.ParseBytesTooLargeException.class, () ->
                    real.runParseBytes(bytes("<x/>"), 1025L, "a.html", null),
                    "content above the configured cap must be refused");

            try (var files = Files.list(real.parseBytesDir)) {
                assertTrue(files.findAny().isEmpty(),
                        "a rejected request must not have created a spool file");
            }
        } finally {
            real.postShutdown();
        }
    }

    /**
     * INV-BOUND-REJECTED-ONCE (b): oversize maps to RESOURCE_EXHAUSTED, not
     * INVALID_ARGUMENT. The request is well formed; the server declines to accept that
     * much, which is a different answer from "your argument is wrong".
     */
    @Test
    void oversizeIsReportedAsResourceExhausted() throws Exception {
        Mockito.when(v1.runParseBytes(Mockito.any(), Mockito.anyLong(), Mockito.any(),
                        Mockito.any()))
                .thenThrow(new TikaGrpcServerImpl.ParseBytesTooLargeException(
                        "content exceeds ParseBytes bound of 1 bytes"));
        RecordingObserver observer = new RecordingObserver();

        v2.parseBytes(requestWithContent().build(), observer);

        assertTrue(observer.replies.isEmpty(), "no reply may accompany a refusal");
        assertEquals(1, observer.errors.size(), "exactly one terminal signal");
        assertEquals(Status.Code.RESOURCE_EXHAUSTED,
                Status.fromThrowable(observer.errors.get(0)).getCode(),
                "oversize is a resource decision, not a malformed argument");
    }

    // The spool file no longer travels with the outcome: PayloadRouter.Routed owns it and
    // runParseBytes closes it right after the parse. The lifecycle invariants live in the
    // real-impl tests below (success, failure and interrupted paths all leave the spool
    // directory empty).

    // ------------------------------------------------------------------
    // INV-SPOOL-LIFECYCLE on the failure paths: stubbed-worker layer — a real
    // TikaGrpcServerImpl whose pipes round trip is stubbed, so no worker ever starts.
    // ------------------------------------------------------------------

    private static TikaGrpcServerImpl realImpl(Path tmp) throws Exception {
        return realImpl(tmp, null);
    }

    private static TikaGrpcServerImpl realImpl(Path tmp, Long parseBytesMaxContentBytes)
            throws Exception {
        return realImpl(tmp, parseBytesMaxContentBytes, null);
    }

    /** {@code sharedStore} non-null: a "file" config store at that path, shareable between impls. */
    private static TikaGrpcServerImpl realImpl(Path tmp, Long parseBytesMaxContentBytes,
                                               Path sharedStore) throws Exception {
        Path config = tmp.resolve("unit-config.json");
        Map<String, Object> replacements = new HashMap<>();
        replacements.put("JAVA_PATH", Paths.get(System.getProperty("java.home"), "bin", "java"));
        replacements.put("FETCHER_BASE_PATH", tmp.toAbsolutePath());
        replacements.put("PLUGIN_ROOTS", Paths.get("target").toAbsolutePath().resolve("plugins"));
        JsonConfigHelper.writeConfigFromResource("/tika-pipes-test-config.json",
                TikaGrpcServerTest.class, replacements, config);
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = (ObjectNode) mapper.readTree(config.toFile());
        // Below every payload these tests send: the spool branch is the one under test,
        // and with the 10 MiB default a four-byte payload would never reach it.
        ((ObjectNode) root.get("pipes")).put("maxInlineBytes", 1);
        if (sharedStore != null) {
            ((ObjectNode) root.get("pipes")).put("configStoreType", "file");
            // Through Jackson, not string concatenation: a Windows path would not survive
            // as a JSON literal otherwise.
            ((ObjectNode) root.get("pipes")).put("configStoreParams", mapper.createObjectNode()
                    .put("path", sharedStore.toAbsolutePath().toString()).toString());
        }
        if (parseBytesMaxContentBytes != null) {
            ObjectNode grpc = root.has("grpc") && root.get("grpc").isObject()
                    ? (ObjectNode) root.get("grpc")
                    : root.putObject("grpc");
            grpc.put("parseBytesMaxContentBytes", parseBytesMaxContentBytes);
        }
        Files.writeString(config,
                mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root));
        return new TikaGrpcServerImpl(config.toAbsolutePath().toString());
    }

    /** INV-SPOOL-LIFECYCLE: a failed pipes round trip must not leak the spool file. */
    @Test
    void spoolIsDeletedWhenThePipesRoundTripThrows(@TempDir Path tmp) throws Exception {
        TikaGrpcServerImpl real = realImpl(tmp);
        try {
            TikaGrpcServerImpl spied = Mockito.spy(real);
            Mockito.doThrow(new RuntimeException("worker unavailable"))
                    .when(spied).executeTuple(Mockito.any(), Mockito.anyLong());

            assertThrows(RuntimeException.class, () ->
                    spied.runParseBytes(bytes("<x/>"), 4, "a.html", null));

            assertSpooled(spied);
            try (var files = Files.list(spied.parseBytesDir)) {
                assertTrue(files.findAny().isEmpty(),
                        "a failed round trip must not leave a spool file behind");
            }
        } finally {
            // package-private cleanup: closes the never-started pipes client and removes
            // the temp dir + effective config the constructor created.
            real.postShutdown();
        }
    }

    /** Same invariant on the interrupted (null-outcome) path. */
    @Test
    void spoolIsDeletedWhenTheOutcomeIsNull(@TempDir Path tmp) throws Exception {
        TikaGrpcServerImpl real = realImpl(tmp);
        try {
            TikaGrpcServerImpl spied = Mockito.spy(real);
            Mockito.doReturn(null)
                    .when(spied).executeTuple(Mockito.any(), Mockito.anyLong());

            assertNull(spied.runParseBytes(bytes("<x/>"), 4, "a.html", null));

            assertSpooled(spied);
            try (var files = Files.list(spied.parseBytesDir)) {
                assertTrue(files.findAny().isEmpty(),
                        "an interrupted round trip must not leave a spool file behind");
            }
        } finally {
            real.postShutdown();
        }
    }

    /** The tuple the stubbed worker received named the spool fetcher, not the inline one. */
    private static void assertSpooled(TikaGrpcServerImpl spied) throws Exception {
        ArgumentCaptor<FetchEmitTuple> tuple = ArgumentCaptor.forClass(FetchEmitTuple.class);
        Mockito.verify(spied).executeTuple(tuple.capture(), Mockito.anyLong());
        assertEquals(spied.parseBytesFetcherId,
                tuple.getValue().getFetchKey().getFetcherId(),
                "the payload must have gone through the spool for this test to mean anything");
    }

    /**
     * INV-DRAIN: TikaGrpcServer.stop() calls shutdown() before the gRPC server drains and
     * postShutdown() after it. A request still in flight, or admitted in that window, must
     * find its spool directory; only postShutdown() releases it, once the worker is gone.
     */
    @Test
    void spoolSurvivesShutdownAndIsReleasedByPostShutdown(@TempDir Path tmp) throws Exception {
        TikaGrpcServerImpl real = realImpl(tmp);
        Path dir = real.parseBytesDir;
        try {
            TikaGrpcServerImpl spied = Mockito.spy(real);
            Mockito.doReturn(new TikaGrpcServerImpl.FetchParseOutcome("k", "PARSE_SUCCESS", null,
                            Map.of(), htmlMetadata(), 1L))
                    .when(spied).executeTuple(Mockito.any(), Mockito.anyLong());

            spied.shutdown();

            assertNotNull(spied.runParseBytes(bytes("<x/>"), 4, "a.html", null),
                    "a request admitted while the server drains must still be served");
            assertTrue(Files.isDirectory(dir),
                    "shutdown() runs before the drain, so it must leave the spool directory");
        } finally {
            real.postShutdown();
        }
        assertFalse(Files.exists(dir), "postShutdown() is where the spool directory goes");
    }

    /** INV-CTOR-ROLLBACK: a constructor that throws leaves nothing behind for nobody to close. */
    @Test
    void failedConstructionLeavesNoSpoolDirectory(@TempDir Path tmp) throws Exception {
        Path config = tmp.resolve("ignite-without-the-module.json");
        Files.writeString(config,
                "{\"pipes\":{\"configStoreType\":\"ignite\",\"configStoreParams\":\"{}\"}}");
        Path tmpdir = Paths.get(System.getProperty("java.io.tmpdir"));
        long before = spoolDirectories(tmpdir);

        assertThrows(TikaConfigException.class,
                () -> new TikaGrpcServerImpl(config.toAbsolutePath().toString()));

        assertEquals(before, spoolDirectories(tmpdir),
                "the constructor acquired a spool directory and then failed: it must release it");
    }

    /**
     * INV-SPOOL-ISOLATION: two servers on one config store must each resolve their own spool
     * directory, and leave no fetcher behind when they go. The "file" store ships out of the
     * box, defaults to config-store.json in the working directory, and re-reads its file on
     * every get: with one fixed id the last server to start would win for both.
     */
    @Test
    void twoServersOnOneStoreKeepTheirOwnSpool(@TempDir Path tmp) throws Exception {
        Path store = tmp.resolve("shared-config-store.json");
        TikaGrpcServerImpl first = realImpl(Files.createDirectories(tmp.resolve("first")), null, store);
        TikaGrpcServerImpl second = realImpl(Files.createDirectories(tmp.resolve("second")), null, store);
        try {
            assertEquals(first.parseBytesDir.toString(), spoolBasePath(first),
                    "the first server must resolve its own spool directory, not the last writer's");
            assertEquals(second.parseBytesDir.toString(), spoolBasePath(second));
            assertNotEquals(first.parseBytesFetcherId, second.parseBytesFetcherId,
                    "each server names its own spool fetcher");
        } finally {
            first.postShutdown();
        }
        try {
            assertFalse(Files.readString(store).contains(first.parseBytesFetcherId),
                    "a departed server leaves no fetcher behind in a shared store");
            assertTrue(Files.readString(store).contains(second.parseBytesFetcherId),
                    "and takes only its own entry with it");
        } finally {
            second.postShutdown();
        }
        assertFalse(Files.readString(store).contains(second.parseBytesFetcherId));
    }

    /**
     * INV-SHARED-STORE-WRITEBACK: a file-backed store re-reads its file on get but not on
     * remove, which writes this JVM's cache back. Between the first server's start and its
     * shutdown nothing here looks the store up, so that cache predates the second server:
     * removing its own entry must not take the other server's entry with it.
     */
    @Test
    void aDepartingServerLeavesTheOthersEntriesAlone(@TempDir Path tmp) throws Exception {
        Path store = tmp.resolve("shared-config-store.json");
        TikaGrpcServerImpl first = realImpl(Files.createDirectories(tmp.resolve("first")), null, store);
        TikaGrpcServerImpl second = realImpl(Files.createDirectories(tmp.resolve("second")), null, store);
        try {
            first.postShutdown();
            assertTrue(Files.readString(store).contains(second.parseBytesFetcherId),
                    "removing its own entry must not write back a cache that predates the other server");
        } finally {
            second.postShutdown();
        }
    }

    private static String spoolBasePath(TikaGrpcServerImpl impl) throws Exception {
        String json = impl.fetcherManager.getFetcher(impl.parseBytesFetcherId)
                .getExtensionConfig().json();
        return new ObjectMapper().readTree(json).get("basePath").asText();
    }

    private static long spoolDirectories(Path tmpdir) throws IOException {
        try (var entries = Files.list(tmpdir)) {
            return entries.filter(p -> p.getFileName().toString().startsWith("tika-grpc-parse-bytes"))
                    .count();
        }
    }
}
