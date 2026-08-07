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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
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
import org.mockito.Mockito;

import org.apache.tika.grpc.v2.ParseBytesReply;
import org.apache.tika.grpc.v2.ParseBytesRequest;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.serialization.config.JsonConfigHelper;

/**
 * ParseBytes contract coverage at the unit tier: the v1 collaborator is mocked, so no
 * forked worker and no gRPC server are involved.
 */
class TikaGrpcV2ParseBytesUnitTest {

    @TempDir
    Path spoolDir;

    private TikaGrpcServerImpl v1;
    private TikaGrpcV2ServerImpl v2;

    @BeforeEach
    void setUp() {
        v1 = Mockito.mock(TikaGrpcServerImpl.class);
        Mockito.when(v1.denyPerRequestConfig(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(false);
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
        primary.set(Metadata.CONTENT_TYPE, "text/html");
        return primary;
    }

    private TikaGrpcServerImpl.ParseBytesOutcome outcome() throws Exception {
        Path spool = Files.createTempFile(spoolDir, "parse-bytes-", "");
        return new TikaGrpcServerImpl.ParseBytesOutcome(
                htmlMetadata(), "PARSE_SUCCESS", 5L, spool);
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
     * the internal spool name. What the id should be instead is still open on TIKA-4795,
     * so this pins the invariant rather than the eventual semantics.
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

    /** The spool file is released once the reply has been produced. */
    @Test
    void spoolFileIsReleasedAfterReply() throws Exception {
        Path spool = Files.createTempFile(spoolDir, "parse-bytes-", "");
        Mockito.when(v1.runParseBytes(Mockito.any(), Mockito.anyLong(), Mockito.any(), Mockito.any()))
                .thenReturn(new TikaGrpcServerImpl.ParseBytesOutcome(
                        htmlMetadata(), "PARSE_SUCCESS", 5L, spool));
        RecordingObserver observer = new RecordingObserver();

        v2.parseBytes(requestWithContent().setCorrelationId("c-1").build(), observer);

        assertEquals(1, observer.replies.size(), "release must follow a produced reply");
        assertTrue(observer.errors.isEmpty(), "no error may precede the release");
        assertFalse(Files.exists(spool), "the spool file must not outlive the reply");
    }

    // ------------------------------------------------------------------
    // INV-SPOOL-LIFECYCLE on the failure paths: stubbed-worker layer — a real
    // TikaGrpcServerImpl whose pipes round trip is stubbed, so no worker ever starts.
    // ------------------------------------------------------------------

    private static TikaGrpcServerImpl realImpl(Path tmp) throws Exception {
        return realImpl(tmp, null);
    }

    private static TikaGrpcServerImpl realImpl(Path tmp, Long parseBytesMaxContentBytes)
            throws Exception {
        Path config = tmp.resolve("unit-config.json");
        Map<String, Object> replacements = new HashMap<>();
        replacements.put("JAVA_PATH", Paths.get(System.getProperty("java.home"), "bin", "java"));
        replacements.put("FETCHER_BASE_PATH", tmp.toAbsolutePath());
        replacements.put("PLUGIN_ROOTS", Paths.get("target").toAbsolutePath().resolve("plugins"));
        JsonConfigHelper.writeConfigFromResource("/tika-pipes-test-config.json",
                TikaGrpcServerTest.class, replacements, config);
        if (parseBytesMaxContentBytes != null) {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode root = (ObjectNode) mapper.readTree(config.toFile());
            ObjectNode grpc = root.has("grpc") && root.get("grpc").isObject()
                    ? (ObjectNode) root.get("grpc")
                    : root.putObject("grpc");
            grpc.put("parseBytesMaxContentBytes", parseBytesMaxContentBytes);
            Files.writeString(config,
                    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root));
        }
        return new TikaGrpcServerImpl(config.toAbsolutePath().toString());
    }

    /** INV-SPOOL-LIFECYCLE: a failed pipes round trip must not leak the spool file. */
    @Test
    void spoolIsDeletedWhenThePipesRoundTripThrows(@TempDir Path tmp) throws Exception {
        TikaGrpcServerImpl real = realImpl(tmp);
        try {
            TikaGrpcServerImpl spied = Mockito.spy(real);
            Mockito.doThrow(new RuntimeException("worker unavailable"))
                    .when(spied).runFetchAndParse(Mockito.anyString(), Mockito.anyString(),
                            Mockito.isNull(), Mockito.any(), Mockito.any(Metadata.class));

            assertThrows(RuntimeException.class, () ->
                    spied.runParseBytes(bytes("<x/>"), 4, "a.html", null));

            try (var files = Files.list(spied.parseBytesDir)) {
                assertTrue(files.findAny().isEmpty(),
                        "a failed round trip must not leave a spool file behind");
            }
        } finally {
            // package-private cleanup: closes the never-started pipes client and removes
            // the temp dirs + augmented config the constructor created.
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
                    .when(spied).runFetchAndParse(Mockito.anyString(), Mockito.anyString(),
                            Mockito.isNull(), Mockito.any(), Mockito.any(Metadata.class));

            assertNull(spied.runParseBytes(bytes("<x/>"), 4, "a.html", null));

            try (var files = Files.list(spied.parseBytesDir)) {
                assertTrue(files.findAny().isEmpty(),
                        "an interrupted round trip must not leave a spool file behind");
            }
        } finally {
            real.postShutdown();
        }
    }
}
