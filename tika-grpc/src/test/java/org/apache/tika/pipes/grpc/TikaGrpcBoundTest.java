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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.InsecureServerCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.tika.config.loader.TikaJsonConfig;
import org.apache.tika.grpc.v2.ParseBytesReply;
import org.apache.tika.grpc.v2.ParseBytesRequest;
import org.apache.tika.grpc.v2.TikaV2Grpc;
import org.apache.tika.serialization.config.JsonConfigHelper;

/**
 * The transport bound, on a real Netty server. The in-process transport used by the rest
 * of the suite is exempt from {@code maxInboundMessageSize}, which is why the advertised
 * ParseBytes cap went unnoticed as unreachable: nothing here can be covered in-process.
 *
 * <p>Payloads stay a few MiB. Nothing allocates 64 MiB, and the reachability case uses a
 * large input with a negligible output so the reply stays well under the client's own
 * default and the server ingress is the only variable.
 */
public class TikaGrpcBoundTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /** Above grpc-java's ~4 MiB default, cheap to allocate. */
    private static final int ABOVE_DEFAULT = 5 * 1024 * 1024;

    /** 100 MiB: what dev-tika-config.json configures for the demo. */
    private static final int DEMO_LIMIT = 104857600;

    private static Path baseConfig;

    @BeforeAll
    static void init() throws Exception {
        baseConfig = Paths.get("target", "tika-config-bound-" + UUID.randomUUID() + ".json");
        Map<String, Object> replacements = new HashMap<>();
        replacements.put("JAVA_PATH", Paths.get(System.getProperty("java.home"), "bin", "java"));
        replacements.put("FETCHER_BASE_PATH", Paths.get("target").toAbsolutePath());
        replacements.put("PLUGIN_ROOTS", Paths.get("target").toAbsolutePath().resolve("plugins"));
        JsonConfigHelper.writeConfigFromResource("/tika-pipes-test-config.json",
                TikaGrpcBoundTest.class, replacements, baseConfig);
    }

    @AfterAll
    static void clean() throws Exception {
        Files.deleteIfExists(baseConfig);
    }

    /**
     * Derives a config carrying the given inbound limit, or no {@code grpc} section at all
     * when {@code maxInboundMessageBytes} is null.
     */
    private static Path configWithLimit(Integer maxInboundMessageBytes) throws Exception {
        return configWithLimits(maxInboundMessageBytes, null);
    }

    /** Like {@link #configWithLimit} but optionally setting the ParseBytes content cap. */
    private static Path configWithLimits(Integer maxInboundMessageBytes,
                                         Long parseBytesMaxContentBytes) throws Exception {
        ObjectNode root = (ObjectNode) OBJECT_MAPPER.readTree(baseConfig.toFile());
        if (maxInboundMessageBytes != null || parseBytesMaxContentBytes != null) {
            ObjectNode grpc = root.putObject("grpc");
            if (maxInboundMessageBytes != null) {
                grpc.put("maxInboundMessageBytes", maxInboundMessageBytes);
            }
            if (parseBytesMaxContentBytes != null) {
                grpc.put("parseBytesMaxContentBytes", parseBytesMaxContentBytes);
            }
        }
        Path derived = Paths.get("target", "tika-config-bound-"
                + maxInboundMessageBytes + "-" + UUID.randomUUID() + ".json");
        FileUtils.write(derived.toFile(),
                OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(root),
                StandardCharsets.UTF_8);
        derived.toFile().deleteOnExit();
        return derived;
    }

    /**
     * A real Netty server plus its channel and pipes runtime, all released together.
     * Lifecycle is explicit rather than delegated: the pipes client and the temp
     * directories the impl creates are only released by {@code postShutdown()}, and
     * nothing else calls it.
     */
    private static final class RealServer implements AutoCloseable {
        private final TikaGrpcServerImpl serviceImpl;
        private final Server server;
        private final ManagedChannel channel;
        final TikaV2Grpc.TikaV2BlockingStub v2;

        RealServer(Path config) throws Exception {
            serviceImpl = new TikaGrpcServerImpl(config.toAbsolutePath().toString());
            // Wired through the same helper production uses, so this exercises the real
            // wiring instead of a copy of it that could drift.
            ServerBuilder<?> builder =
                    Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create());
            TikaGrpcServer.applyInboundLimit(builder, serviceImpl.tikaGrpcConfig);
            server = builder
                    .addService(serviceImpl)
                    .addService(new TikaGrpcV2ServerImpl(serviceImpl))
                    .build()
                    .start();
            channel = Grpc.newChannelBuilderForAddress(
                            "localhost", server.getPort(), InsecureChannelCredentials.create())
                    .build();
            v2 = TikaV2Grpc.newBlockingStub(channel);
        }

        @Override
        public void close() throws Exception {
            channel.shutdownNow();
            channel.awaitTermination(10, TimeUnit.SECONDS);
            server.shutdownNow();
            server.awaitTermination(10, TimeUnit.SECONDS);
            serviceImpl.postShutdown();
        }
    }

    /**
     * Large input, negligible output: the padding sits inside an HTML comment, which the
     * parser drops, so the reply stays small and the client's own inbound limit never
     * enters the picture.
     */
    private static ByteString bigInputTinyOutput(int totalBytes) {
        String head = "<html><body>tiny<!--";
        String tail = "--></body></html>";
        int pad = totalBytes - head.length() - tail.length();
        return ByteString.copyFromUtf8(head + "x".repeat(Math.max(0, pad)) + tail);
    }

    /** INV-BOUND-REACHABLE: with the knob raised, a payload over grpc's default parses. */
    @Test
    public void payloadAboveTheGrpcDefaultIsAcceptedWhenTheKnobIsRaised() throws Exception {
        try (RealServer s = new RealServer(configWithLimit(DEMO_LIMIT))) {
            ParseBytesReply reply = s.v2.parseBytes(ParseBytesRequest.newBuilder()
                    .setCorrelationId("big-1")
                    .setContent(bigInputTinyOutput(ABOVE_DEFAULT))
                    .setResourceName("big.html")
                    .build());

            assertTrue(reply.hasDocument(), "a 5 MiB payload must reach the parser");
            assertTrue(reply.getSerializedSize() < 1024 * 1024,
                    "the reply must stay small, or the test is measuring two limits at once; "
                            + "got " + reply.getSerializedSize() + " bytes");
        }
    }

    /** The configured cap is inclusive. */
    @Test
    public void contentExactlyAtTheConfiguredCapIsAccepted() throws Exception {
        try (RealServer s = new RealServer(
                configWithLimits(DEMO_LIMIT, (long) ABOVE_DEFAULT))) {
            ParseBytesReply reply = s.v2.parseBytes(ParseBytesRequest.newBuilder()
                    .setCorrelationId("at-cap-1")
                    .setContent(bigInputTinyOutput(ABOVE_DEFAULT))
                    .setResourceName("atcap.html")
                    .build());

            assertTrue(reply.hasDocument(),
                    "content of exactly the configured cap must parse");
        }
    }

    /**
     * INV-DEFAULT-UNTOUCHED: with no knob, grpc's own default still governs. This is the
     * behaviour the repository ships today and the slice must not change it.
     */
    @Test
    public void withoutTheKnobTheGrpcDefaultStillApplies() throws Exception {
        try (RealServer s = new RealServer(configWithLimit(null))) {
            StatusRuntimeException thrown = assertThrows(StatusRuntimeException.class, () ->
                    s.v2.parseBytes(ParseBytesRequest.newBuilder()
                            .setContent(bigInputTinyOutput(ABOVE_DEFAULT))
                            .build()));

            assertEquals(Status.Code.RESOURCE_EXHAUSTED, thrown.getStatus().getCode());
        }
    }

    /**
     * INV-TRANSPORT-REFUSES: the knob is genuinely applied, shown by setting it BELOW
     * grpc's default. A 2 MiB request would sail through the 4 MiB default; it must not
     * sail through a 1 MiB limit. Only the status is asserted -- the description is
     * grpc-java's wording, not ours, and pinning it would make another project's internal
     * string part of this suite.
     */
    @Test
    public void aKnobBelowTheGrpcDefaultIsAlsoHonoured() throws Exception {
        try (RealServer s = new RealServer(configWithLimit(1024 * 1024))) {
            StatusRuntimeException thrown = assertThrows(StatusRuntimeException.class, () ->
                    s.v2.parseBytes(ParseBytesRequest.newBuilder()
                            .setContent(bigInputTinyOutput(2 * 1024 * 1024))
                            .build()));

            assertEquals(Status.Code.RESOURCE_EXHAUSTED, thrown.getStatus().getCode());
            // Recorded, not asserted: what a caller reads when the transport refuses.
            System.out.println("[evidence] transport refusal description: "
                    + thrown.getStatus().getDescription());
        }
    }

    /**
     * The production wiring, not just the helper. The three tests above build their own
     * server and call {@link TikaGrpcServer#applyInboundLimit} directly, so they prove the
     * helper works while saying nothing about whether {@link TikaGrpcServer#start()} calls
     * it. This one drives the real class.
     *
     * <p>The knob is set BELOW gRPC's default on purpose: a 2 MiB request sails through
     * the 4 MiB default, so it can only be refused if start() applied the 1 MiB limit.
     */
    @Test
    public void theRealServerAppliesTheKnobOnStart() throws Exception {
        int port;
        try (ServerSocket free = new ServerSocket(0)) {
            port = free.getLocalPort();
        }
        TikaGrpcServer server = new TikaGrpcServer()
                .setPort(port)
                .setTikaConfig(configWithLimit(1024 * 1024).toFile());
        ManagedChannel channel = null;
        try {
            server.start();
            channel = Grpc.newChannelBuilderForAddress(
                    "localhost", port, InsecureChannelCredentials.create()).build();
            TikaV2Grpc.TikaV2BlockingStub v2 = TikaV2Grpc.newBlockingStub(channel);

            StatusRuntimeException thrown = assertThrows(StatusRuntimeException.class, () ->
                    v2.parseBytes(ParseBytesRequest.newBuilder()
                            .setContent(bigInputTinyOutput(2 * 1024 * 1024))
                            .build()));

            assertEquals(Status.Code.RESOURCE_EXHAUSTED, thrown.getStatus().getCode(),
                    "start() must apply the configured limit, not only the helper");
        } finally {
            if (channel != null) {
                channel.shutdownNow();
                channel.awaitTermination(10, TimeUnit.SECONDS);
            }
            server.stop();
        }
    }

    /**
     * INV-CAP-INERT-VISIBLE: a configured limit at or below the ParseBytes cap leaves that
     * cap unreachable, and the operator is told. Equality counts: a request carrying
     * exactly the cap also carries its other fields, so it exceeds the envelope.
     */
    @Test
    public void aTransportLimitAtOrBelowTheContentCapIsAnnounced() {
        long cap = TikaGrpcConfig.DEFAULT_PARSE_BYTES_MAX_CONTENT_BYTES;
        assertNotNull(TikaGrpcServer.inertCapWarning(1024 * 1024, cap),
                "a limit below the cap leaves the cap unreachable");
        assertNotNull(TikaGrpcServer.inertCapWarning((int) cap, cap),
                "at equality the envelope overhead still puts the cap out of reach");
        assertNull(TikaGrpcServer.inertCapWarning(DEMO_LIMIT, cap),
                "the limit the demo config ships gives the default cap real headroom");
        assertNull(TikaGrpcServer.inertCapWarning(null, cap),
                "no knob is the documented default, not an operator mistake");
        assertNotNull(TikaGrpcServer.inertCapWarning(DEMO_LIMIT, DEMO_LIMIT * 2L),
                "a configured cap above the transport limit is unreachable regardless "
                        + "of the default");
    }

    /** Both size limits reject non-positive values through the JSON load path. */
    @Test
    public void nonPositiveLimitsAreRefusedAtLoad() throws Exception {
        assertLoadRejects("parseBytesMaxContentBytes", 0);
        assertLoadRejects("parseBytesMaxContentBytes", -1);
        assertLoadRejects("maxInboundMessageBytes", 0);
        assertLoadRejects("maxInboundMessageBytes", -1);
    }

    private static void assertLoadRejects(String key, long value) throws Exception {
        Path file = Paths.get("target",
                "tika-config-badknob-" + UUID.randomUUID() + ".json");
        FileUtils.write(file.toFile(),
                "{\"grpc\": {\"" + key + "\": " + value + "}}", StandardCharsets.UTF_8);
        file.toFile().deleteOnExit();
        IOException refused = assertThrows(IOException.class,
                () -> TikaGrpcConfig.load(TikaJsonConfig.load(file)),
                key + "=" + value + " must be refused at load");
        Throwable cause = refused;
        boolean fromValidation = false;
        while (cause != null && !fromValidation) {
            fromValidation = String.valueOf(cause.getMessage()).contains("must be positive");
            cause = cause.getCause();
        }
        assertTrue(fromValidation,
                "the refusal must come from the knob validation, got: " + refused);
    }

    /**
     * A transport limit the heap cannot absorb is announced. Suggested by Kristian
     * Rickert on TIKA-4795: a unary request is held in memory in full before parsing
     * starts, so an inbound limit close to the heap size is a configuration that will
     * fail under load rather than at startup.
     */
    @Test
    public void aTransportLimitTooLargeForTheHeapIsAnnounced() {
        long mib = 1024L * 1024L;
        assertNotNull(TikaGrpcServer.heapHeadroomWarning(100 * (int) mib, 256 * mib),
                "100 MiB inbound on a 256 MiB heap leaves no room for one request");
        assertNull(TikaGrpcServer.heapHeadroomWarning(100 * (int) mib, 1024 * mib),
                "a gigabyte of heap absorbs a 100 MiB request");
        assertNull(TikaGrpcServer.heapHeadroomWarning(100 * (int) mib, 300 * mib),
                "exactly the headroom floor is not a warning");
        assertNull(TikaGrpcServer.heapHeadroomWarning(null, 64 * mib),
                "no knob means grpc's own small default, nothing to say");
        assertNull(TikaGrpcServer.heapHeadroomWarning(100 * (int) mib, Long.MAX_VALUE),
                "an unbounded heap cannot be too small");
    }

    /** Both startup warnings reach the caller through one place, so one loop logs them. */
    @Test
    public void bothWarningsAreCollectedTogether() {
        long mib = 1024L * 1024L;
        long cap = TikaGrpcConfig.DEFAULT_PARSE_BYTES_MAX_CONTENT_BYTES;
        assertEquals(2, TikaGrpcServer.startupWarnings((int) cap, cap, 100 * mib).size(),
                "a limit at the cap on a small heap is two separate problems");
        assertTrue(TikaGrpcServer.startupWarnings(DEMO_LIMIT, cap, 8192 * mib).isEmpty(),
                "the demo limit on a healthy heap has nothing to report");
    }

    /**
     * The warning is emitted, not merely computed. Asserting only that
     * {@link TikaGrpcServer#inertCapWarning} builds a string would leave the same hole the
     * knob wiring had: deleting the {@code LOGGER.warn(...)} call would still pass.
     */
    @Test
    public void theRealServerLogsTheWarningOnStart() throws Exception {
        int port;
        try (ServerSocket free = new ServerSocket(0)) {
            port = free.getLocalPort();
        }
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        CapturingAppender captured = new CapturingAppender();
        captured.start();
        ctx.getConfiguration().getRootLogger().addAppender(captured, Level.WARN, null);
        ctx.updateLoggers();

        TikaGrpcServer server = new TikaGrpcServer()
                .setPort(port)
                .setTikaConfig(configWithLimit(1024 * 1024).toFile());
        try {
            server.start();
            assertTrue(captured.messages.stream()
                            .anyMatch(m -> m.contains("maxInboundMessageBytes is 1048576")),
                    "startup must announce that the ParseBytes cap is unreachable; saw: "
                            + captured.messages);
        } finally {
            server.stop();
            ctx.getConfiguration().getRootLogger().removeAppender("capture");
            ctx.updateLoggers();
            captured.stop();
        }
    }

    /** Collects WARN-level messages so the emission can be asserted without a log file. */
    private static final class CapturingAppender extends AbstractAppender {
        private final List<String> messages = Collections.synchronizedList(new ArrayList<>());

        CapturingAppender() {
            super("capture", null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }
    }

    /**
     * INV-START-ROLLBACK: start() builds the impl before it binds. When the bind fails --
     * the port is taken, which on Linux any outbound connection can do since 50052 sits in
     * the ephemeral range -- nothing else holds the impl, so start() must release it.
     */
    @Test
    public void aFailedStartReleasesTheImpl() throws Exception {
        Path tmpdir = Paths.get(System.getProperty("java.io.tmpdir"));
        try (ServerSocket taken = new ServerSocket(0)) {
            TikaGrpcServer server = new TikaGrpcServer()
                    .setPort(taken.getLocalPort())
                    .setTikaConfig(configWithLimit(null).toFile());
            long before = spoolDirectories(tmpdir);

            assertThrows(IOException.class, server::start, "the port is taken");

            assertEquals(before, spoolDirectories(tmpdir),
                    "start() built the impl and then failed: it must release its spool directory");
        }
    }

    /**
     * Same invariant, other failure point: a refusal raised between building the impl and
     * binding (here grpc rejecting a corrupt trust collection while it builds the server)
     * must release the impl just the same.
     */
    @Test
    public void aStartRefusedAfterBuildingTheImplReleasesIt(@TempDir Path tmp) throws Exception {
        Path corrupt = tmp.resolve("corrupt-ca.pem");
        Files.writeString(corrupt, "this is not a valid PEM certificate");
        Path certs = Paths.get("src", "test", "resources", "certs");
        TikaGrpcServer server = new TikaGrpcServer()
                .setPort(0)
                .setSecure(true)
                .setCertChain(certs.resolve("server1.pem").toFile())
                .setPrivateKey(certs.resolve("server1.key").toFile())
                .setTrustCertCollection(corrupt.toFile())
                .setClientAuthRequired(true)
                .setTikaConfig(configWithLimit(null).toFile());
        Path tmpdir = Paths.get(System.getProperty("java.io.tmpdir"));
        long before = spoolDirectories(tmpdir);

        assertThrows(Exception.class, server::start, "grpc must refuse the corrupt trust collection");

        assertEquals(before, spoolDirectories(tmpdir),
                "the refusal came after the impl was built: start() must release it");
    }

    private static long spoolDirectories(Path tmpdir) throws IOException {
        try (var entries = Files.list(tmpdir)) {
            return entries.filter(p -> p.getFileName().toString().startsWith("tika-grpc-parse-bytes"))
                    .count();
        }
    }
}
