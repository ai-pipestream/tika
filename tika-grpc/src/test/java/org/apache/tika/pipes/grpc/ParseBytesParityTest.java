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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.tika.grpc.v2.Document;
import org.apache.tika.grpc.v2.FetchAndParseRequest;
import org.apache.tika.grpc.v2.MetadataField;
import org.apache.tika.grpc.v2.ParseBytesRequest;
import org.apache.tika.grpc.v2.ParseStatus;
import org.apache.tika.grpc.v2.TikaV2Grpc;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.serialization.config.JsonConfigHelper;

/**
 * The parity oracle for ParseBytes: the same bytes parsed through FetchAndParse (a file
 * on disk) and through ParseBytes must produce the same {@link Document}. Inputs are
 * aligned so every remaining difference is signal: the correlation id is the fetch key
 * (both become the Document id), the resource name is the file's basename (what the
 * fetch path falls back to), the oracle fetcher extracts no filesystem metadata, and a
 * SHA-256 digester is configured so the observed digests have to agree.
 *
 * <p>The per-call fields are checked, then canonicalized away: {@code parsed_at} must
 * be present on both sides; {@code fetch_parse_time_ms} must be non-negative;
 * {@code tk:parse-time-millis} must appear on both sides or on neither, with
 * non-negative values when it does; a source path must never appear in the ParseBytes
 * reply, while the fetch side may record one and it is dropped from that side alone.
 * Everything else is compared whole-proto, with the tagged tail in a stable key order
 * since emission order is not part of the contract.
 */
public class ParseBytesParityTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final long TIMING_SENTINEL = 0L;

    private static final String HTML =
            "<html><head><title>Parity</title>"
                    + "<meta name=\"author\" content=\"Jane Doe\"/></head>"
                    + "<body>same bytes both ways</body></html>";

    private static Path config;

    @BeforeAll
    static void init() throws Exception {
        Path base = Paths.get("target", "tika-config-parity-" + UUID.randomUUID() + ".json");
        Map<String, Object> replacements = new HashMap<>();
        replacements.put("JAVA_PATH", Paths.get(System.getProperty("java.home"), "bin", "java"));
        replacements.put("FETCHER_BASE_PATH", Paths.get("target").toAbsolutePath());
        replacements.put("PLUGIN_ROOTS", Paths.get("target").toAbsolutePath().resolve("plugins"));
        JsonConfigHelper.writeConfigFromResource("/tika-pipes-test-config.json",
                ParseBytesParityTest.class, replacements, base);

        // The oracle fetcher must add nothing of its own: no filesystem metadata, so a
        // fetched file carries exactly what the bytes carry. The digester makes the two
        // observed digests comparable signal instead of an empty-equals-empty pass.
        ObjectNode root = (ObjectNode) OBJECT_MAPPER.readTree(base.toFile());
        ((ObjectNode) root.get("fetchers")).putObject("oracle-fetcher")
                .putObject("file-system-fetcher")
                .put("basePath", Paths.get("target").toAbsolutePath().toString())
                .put("extractFileSystemMetadata", false);
        ObjectNode parseCtx = root.has("parse-context") && root.get("parse-context").isObject()
                ? (ObjectNode) root.get("parse-context")
                : root.putObject("parse-context");
        ObjectNode digester = parseCtx.putObject("commons-digester-factory");
        digester.putArray("digests").addObject().put("algorithm", "SHA256");
        digester.put("skipContainerDocumentDigest", false);

        config = Paths.get("target", "tika-config-parity-final-" + UUID.randomUUID() + ".json");
        FileUtils.write(config.toFile(),
                OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(root),
                StandardCharsets.UTF_8);
        Files.deleteIfExists(base);
    }

    @AfterAll
    static void clean() throws Exception {
        Files.deleteIfExists(config);
    }

    @Test
    public void sameBytesEitherWayYieldTheSameDocument() throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        TikaGrpcServerImpl serviceImpl =
                new TikaGrpcServerImpl(config.toAbsolutePath().toString());
        Server server = InProcessServerBuilder.forName(serverName).directExecutor()
                .addService(serviceImpl)
                .addService(new TikaGrpcV2ServerImpl(serviceImpl))
                .build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName)
                .directExecutor().build();
        TikaV2Grpc.TikaV2BlockingStub v2 = TikaV2Grpc.newBlockingStub(channel);

        String folder = "parity-" + UUID.randomUUID();
        File dir = new File("target", folder);
        try {
            FileUtils.forceMkdir(dir);
            FileUtils.writeStringToFile(new File(dir, "parity.html"), HTML,
                    StandardCharsets.UTF_8);
            String fetchKey = folder + "/parity.html";

            Document viaFetch = v2.fetchAndParse(FetchAndParseRequest.newBuilder()
                    .setFetcherId("oracle-fetcher")
                    .setFetchKey(fetchKey)
                    .build()).getDocument();
            Document viaBytes = v2.parseBytes(ParseBytesRequest.newBuilder()
                    .setContent(ByteString.copyFromUtf8(HTML))
                    .setResourceName("parity.html")
                    .setCorrelationId(fetchKey)
                    .build()).getDocument();

            // Fixture health first: comparing two failures would prove nothing.
            assertEquals(ParseStatus.Status.SUCCESS, viaFetch.getStatus().getStatus(),
                    "the fetch path must parse the fixture");
            assertEquals(ParseStatus.Status.SUCCESS, viaBytes.getStatus().getStatus(),
                    "the bytes path must parse the fixture");

            // parsed_at is the only per-call field proto3 gives presence for; assert it
            // before it is cleared. The scalar timing can only be sanity-checked.
            assertTrue(viaFetch.hasParsedAt(), "fetch reply must carry parsed_at");
            assertTrue(viaBytes.hasParsedAt(), "bytes reply must carry parsed_at");
            assertTrue(viaFetch.getStatus().getFetchParseTimeMs() >= 0,
                    "fetch timing must be non-negative");
            assertTrue(viaBytes.getStatus().getFetchParseTimeMs() >= 0,
                    "bytes timing must be non-negative");

            // Same bytes, same digest; non-empty so an empty-equals-empty regression
            // cannot slip through the whole-proto comparison below.
            assertFalse(viaFetch.getOrigin().getSha256().isEmpty(),
                    "fetch reply must carry the observed digest");
            assertFalse(viaBytes.getOrigin().getSha256().isEmpty(),
                    "bytes reply must carry the observed digest");

            // One-sided by scope: ParseBytes must never carry a source path. Whether the
            // fetch path records one is that fetcher's business, not this contract, so
            // the canonical form drops it from that side when present.
            String sourcePathKey = TikaCoreProperties.SOURCE_PATH.getName();
            assertEquals(0, count(viaBytes, sourcePathKey),
                    "ParseBytes must not carry a source path");

            // Timing parity in the tail: recorded on both paths or on neither, and
            // always as a non-negative number, before being dropped symmetrically.
            String parseTimeKey = TikaCoreProperties.PARSE_TIME_MILLIS.getName();
            assertEquals(count(viaFetch, parseTimeKey), count(viaBytes, parseTimeKey),
                    "tk:parse-time-millis must be recorded on both paths or neither");
            assertNonNegativeNumbers(viaFetch, parseTimeKey, "fetch");
            assertNonNegativeNumbers(viaBytes, parseTimeKey, "bytes");

            assertEquals(canonical(viaFetch, true), canonical(viaBytes, false),
                    "same bytes must yield the same Document beyond the declared "
                            + "per-call fields");
        } finally {
            // The transport owns nothing of the service impl's: the pipes client, the
            // augmented config and the spool directory are released only by
            // postShutdown(), once the channel and server are down.
            channel.shutdownNow();
            channel.awaitTermination(10, TimeUnit.SECONDS);
            server.shutdownNow();
            server.awaitTermination(10, TimeUnit.SECONDS);
            serviceImpl.postShutdown();
            FileUtils.deleteDirectory(dir);
        }
    }

    /**
     * Removes the per-call fields: parsed_at, the two timings, and (fetch side only,
     * when present) the source path. The tagged tail is
     * stable-sorted by key with duplicates preserved, so within-key value order still
     * has to match while emission order across keys does not.
     */
    private static Document canonical(Document document, boolean dropSourcePath) {
        Document.Builder builder = document.toBuilder();
        builder.clearParsedAt();
        builder.getStatusBuilder().setFetchParseTimeMs(TIMING_SENTINEL);
        String sourcePathKey = TikaCoreProperties.SOURCE_PATH.getName();
        String parseTimeKey = TikaCoreProperties.PARSE_TIME_MILLIS.getName();
        List<MetadataField> tail = new ArrayList<>();
        for (MetadataField field : builder.getExtraList()) {
            if (field.getKey().equals(parseTimeKey)
                    || (dropSourcePath && field.getKey().equals(sourcePathKey))) {
                continue;
            }
            tail.add(field);
        }
        tail.sort(Comparator.comparing(MetadataField::getKey));
        builder.clearExtra();
        builder.addAllExtra(tail);
        return builder.build();
    }

    private static int count(Document document, String key) {
        int occurrences = 0;
        for (MetadataField field : document.getExtraList()) {
            if (field.getKey().equals(key)) {
                occurrences++;
            }
        }
        return occurrences;
    }

    private static void assertNonNegativeNumbers(Document document, String key, String side) {
        for (MetadataField field : document.getExtraList()) {
            if (!field.getKey().equals(key)) {
                continue;
            }
            assertTrue(field.getValue().hasStrings(),
                    key + " must be an untyped text value on the " + side + " side");
            assertFalse(field.getValue().getStrings().getValuesList().isEmpty(),
                    key + " must carry at least one value on the " + side + " side");
            for (String value : field.getValue().getStrings().getValuesList()) {
                assertTrue(Long.parseLong(value) >= 0,
                        key + " must be non-negative on the " + side + " side, was " + value);
            }
        }
    }
}
