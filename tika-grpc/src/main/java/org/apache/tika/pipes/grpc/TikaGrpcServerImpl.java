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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;
import com.google.rpc.Status;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.StringUtils;
import org.pf4j.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.tika.config.loader.TikaJsonConfig;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.pipes.api.ComponentIds;
import org.apache.tika.pipes.api.FetchEmitTuple;
import org.apache.tika.pipes.api.PipesResult;
import org.apache.tika.pipes.api.emitter.EmitKey;
import org.apache.tika.pipes.api.fetcher.FetchKey;
import org.apache.tika.pipes.api.fetcher.Fetcher;
import org.apache.tika.pipes.api.fetcher.FetcherFactory;
import org.apache.tika.pipes.api.pipesiterator.PipesIteratorFactory;
import org.apache.tika.pipes.core.PipesConfig;
import org.apache.tika.pipes.core.PipesException;
import org.apache.tika.pipes.core.PipesParser;
import org.apache.tika.pipes.core.config.ConfigMerger;
import org.apache.tika.pipes.core.config.ConfigOverrides;
import org.apache.tika.pipes.core.config.ConfigStore;
import org.apache.tika.pipes.core.config.ConfigStoreFactory;
import org.apache.tika.pipes.core.config.DefaultPluginsDir;
import org.apache.tika.pipes.core.fetcher.BytesFetcher;
import org.apache.tika.pipes.core.fetcher.FetcherManager;
import org.apache.tika.pipes.core.fetcher.InlineBytes;
import org.apache.tika.pipes.core.fetcher.PayloadRouter;
import org.apache.tika.pipes.grpc.proto.DeleteFetcherReply;
import org.apache.tika.pipes.grpc.proto.DeleteFetcherRequest;
import org.apache.tika.pipes.grpc.proto.DeletePipesIteratorReply;
import org.apache.tika.pipes.grpc.proto.DeletePipesIteratorRequest;
import org.apache.tika.pipes.grpc.proto.FetchAndParseReply;
import org.apache.tika.pipes.grpc.proto.FetchAndParseRequest;
import org.apache.tika.pipes.grpc.proto.GetFetcherConfigJsonSchemaReply;
import org.apache.tika.pipes.grpc.proto.GetFetcherConfigJsonSchemaRequest;
import org.apache.tika.pipes.grpc.proto.GetFetcherReply;
import org.apache.tika.pipes.grpc.proto.GetFetcherRequest;
import org.apache.tika.pipes.grpc.proto.GetPipesIteratorReply;
import org.apache.tika.pipes.grpc.proto.GetPipesIteratorRequest;
import org.apache.tika.pipes.grpc.proto.ListFetchersReply;
import org.apache.tika.pipes.grpc.proto.ListFetchersRequest;
import org.apache.tika.pipes.grpc.proto.SaveFetcherReply;
import org.apache.tika.pipes.grpc.proto.SaveFetcherRequest;
import org.apache.tika.pipes.grpc.proto.SavePipesIteratorReply;
import org.apache.tika.pipes.grpc.proto.SavePipesIteratorRequest;
import org.apache.tika.pipes.grpc.proto.TikaGrpc;
import org.apache.tika.plugins.ExtensionConfig;
import org.apache.tika.plugins.TikaPluginManager;
import org.apache.tika.serialization.ComponentNameResolver;
import org.apache.tika.serialization.ParseContextUtils;
import org.apache.tika.serialization.serdes.ParseContextDeserializer;

class TikaGrpcServerImpl extends TikaGrpc.TikaImplBase {
    private static final Logger LOG = LoggerFactory.getLogger(TikaGrpcServerImpl.class);
    private static final ObjectMapper OBJECT_MAPPER = JsonMapper.builder()
            .defaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.NON_NULL))
            .build();
    public static final JsonSchemaGenerator JSON_SCHEMA_GENERATOR = new JsonSchemaGenerator(OBJECT_MAPPER);

    private static final String PIPES_ITERATOR_PREFIX = "pipesIterator:";
    private static final String IGNITE_STORE_SERVER_CLASS = "org.apache.tika.pipes.ignite.server.IgniteStoreServer";

    PipesConfig pipesConfig;
    TikaGrpcConfig tikaGrpcConfig;
    PipesParser pipesParser;
    /**
     * Prefix of the id of this server's ParseBytes spool fetcher, registered into the
     * effective config at startup (ConfigOverrides, the same mechanism tika-server uses for
     * its own spool fetcher) so the forked worker can read what PayloadRouter spooled. It
     * lives in the reserved namespace like every host-wired component, and it ends in a
     * UUID: servers sharing a config store (the file store re-reads its file on every get)
     * must never resolve each other's spool directory, and the entry is removed again in
     * {@link #postShutdown()} so a shared store does not keep one dead fetcher per start.
     */
    static final String PARSE_BYTES_FETCHER_PREFIX =
            ComponentIds.SYSTEM_PREFIX + "tika-grpc-parse-bytes-";
    final String parseBytesFetcherId = PARSE_BYTES_FETCHER_PREFIX + UUID.randomUUID();
    long parseBytesMaxContentBytes;
    Path parseBytesDir;   // package-private for TikaGrpcV2ParseBytesUnitTest
    private Path effectiveConfigPath;
    FetcherManager fetcherManager;
    ConfigStore configStore;
    Path tikaConfigPath;
    PluginManager pluginManager;
    private AutoCloseable igniteStoreServer;

    TikaGrpcServerImpl(String tikaConfigPath) throws TikaConfigException, IOException {
        this(tikaConfigPath, null);
    }

    TikaGrpcServerImpl(String tikaConfigPath, String pluginRootsOverride) throws TikaConfigException, IOException {
        File tikaConfigFile = new File(tikaConfigPath);
        if (!tikaConfigFile.exists()) {
            throw new TikaConfigException("Tika config file does not exist: " + tikaConfigPath);
        }

        Path configPath = tikaConfigFile.toPath();
        this.tikaConfigPath = configPath;

        TikaJsonConfig tikaJsonConfig = TikaJsonConfig.load(configPath);

        // Load PipesConfig directly from root level (not from "parse-context")
        pipesConfig = tikaJsonConfig.deserialize("pipes", PipesConfig.class);
        if (pipesConfig == null) {
            pipesConfig = new PipesConfig();
        }

        // Security-sensitive grpc features (per-request config, runtime component
        // modifications) are off unless explicitly enabled in the "grpc" section.
        tikaGrpcConfig = TikaGrpcConfig.load(tikaJsonConfig);
        parseBytesMaxContentBytes = tikaGrpcConfig.effectiveParseBytesMaxContentBytes();
        // ParseBytes spools payloads above pipes.maxInlineBytes here. The reserved
        // file-system fetcher is added to an effective config copy (ConfigOverrides +
        // ConfigMerger, the same way tika-server registers its own spool fetcher), so the
        // forked worker can read the spool while the user's config file stays untouched.
        parseBytesDir = Files.createTempDirectory("tika-grpc-parse-bytes");
        try {
            Map<String, Object> spoolFetcherConfig = new HashMap<>();
            spoolFetcherConfig.put("basePath", parseBytesDir.toAbsolutePath().toString());
            spoolFetcherConfig.put("extractFileSystemMetadata", false);
            effectiveConfigPath = ConfigMerger.mergeOrCreate(configPath, ConfigOverrides.builder()
                    .addFetcher(parseBytesFetcherId, "file-system-fetcher", spoolFetcherConfig)
                    .build()).configPath();
            tikaJsonConfig = TikaJsonConfig.load(effectiveConfigPath);

            // PipesClient is single-threaded; the pool admits pipes.numClients at a time.
            pipesParser = PipesParser.load(tikaJsonConfig, pipesConfig, effectiveConfigPath);

            try {
                if (pluginRootsOverride != null && !pluginRootsOverride.trim().isEmpty()) {
                    // Use command-line plugin roots
                    pluginManager = TikaPluginManager.loadFromPaths(pluginRootsOverride);
                } else {
                    // Use plugin roots from config file
                    pluginManager = TikaPluginManager.load(tikaJsonConfig);
                }
                pluginManager.loadPlugins();
                pluginManager.startPlugins();
            } catch (TikaConfigException e) {
                // plugin-roots not configured: probe the install layout like the
                // other pipes entry points (TIKA-4864/TIKA-4865)
                String defaultRoot = DefaultPluginsDir.resolve(TikaGrpcServerImpl.class);
                LOG.warn("plugin-roots not configured ({}); falling back to {}",
                        e.getMessage(), defaultRoot);
                try {
                    pluginManager = TikaPluginManager.loadFromPaths(defaultRoot);
                    pluginManager.loadPlugins();
                    pluginManager.startPlugins();
                } catch (TikaConfigException | IOException e2) {
                    LOG.warn("could not load plugins from {}, starting with none: {}",
                            defaultRoot, e2.getMessage());
                    pluginManager = new org.pf4j.DefaultPluginManager();
                }
            }

            if (pluginManager.getPlugins().isEmpty()) {
                LOG.warn("tika-grpc started with no tika-pipes plugins loaded. "
                        + "Most RPC calls will fail with 'fetcher type unknown' or "
                        + "similar. Place tika-pipes-<plugin>-<version>.zip files in "
                        + "a `plugins/` directory next to tika-grpc.jar (or configure "
                        + "`plugin-roots` in your tika config). Plugin zips are "
                        + "published at https://downloads.apache.org/tika/<version>/.");
            }

            this.configStore = createConfigStore();

            fetcherManager = FetcherManager.load(pluginManager, tikaJsonConfig,
                    tikaGrpcConfig.isAllowComponentManagement(), this.configStore);
        } catch (TikaConfigException | IOException | RuntimeException e) {
            // Nobody gets a reference to a half-built server, so nobody could release what
            // it acquired: the spool directory, the effective config, the pipes pool.
            postShutdown();
            throw e;
        }
    }

    private ConfigStore createConfigStore() throws TikaConfigException {
        String configStoreType = pipesConfig.getConfigStoreType();
        String configStoreParams = pipesConfig.getConfigStoreParams();
        ExtensionConfig storeConfig = new ExtensionConfig(
            configStoreType, configStoreType, configStoreParams);

        // If using Ignite, start the embedded server first
        if ("ignite".equalsIgnoreCase(configStoreType)) {
            startIgniteServer(storeConfig);
        }

        return ConfigStoreFactory.createConfigStore(
                pluginManager,
                configStoreType,
                storeConfig);
    }
    
    /**
     * Starts the embedded Ignite node backing an {@code ignite} ConfigStore. Loaded reflectively so
     * that tika-grpc does not carry an Ignite dependency: Ignite is an opt-in extra that the operator
     * adds to the classpath, not part of the shipped server.
     */
    private void startIgniteServer(ExtensionConfig config) throws TikaConfigException {
        Class<?> serverClass;
        try {
            serverClass = Class.forName(IGNITE_STORE_SERVER_CLASS);
        } catch (ClassNotFoundException e) {
            throw new TikaConfigException("configStoreType=ignite requires tika-pipes-config-store-ignite "
                    + "(and its Ignite dependencies) on the classpath; tika-grpc does not ship them. Add the "
                    + "jars to the classpath, or use a configStoreType of 'memory' or 'file'.", e);
        }
        try {
            LOG.info("Starting embedded Ignite server for ConfigStore");

            com.fasterxml.jackson.databind.JsonNode params = OBJECT_MAPPER.readTree(config.json());

            String tableName = params.has("tableName") ? params.get("tableName").asText() :
                               params.has("cacheName") ? params.get("cacheName").asText() : "tika_config_store";
            String instanceName = params.has("igniteInstanceName") ? params.get("igniteInstanceName").asText() : "TikaIgniteServer";

            igniteStoreServer = (AutoCloseable) serverClass.getConstructor(String.class, String.class)
                    .newInstance(tableName, instanceName);
            serverClass.getMethod("start").invoke(igniteStoreServer);

            LOG.info("Embedded Ignite server started successfully");
        } catch (Exception e) {
            LOG.error("Failed to start embedded Ignite server", e);
            // The constructor propagates, so nothing else will ever call shutdown() for this node.
            shutdown();
            throw new TikaConfigException("Failed to start Ignite server", e);
        }
    }

    /**
     * If the operator has not opted in to runtime component management, closes
     * the call with {@code PERMISSION_DENIED} and returns {@code true}. Guards the
     * Save/Delete fetcher and pipes-iterator RPCs. The caller must {@code return}
     * immediately when this returns {@code true}.
     * <p>
     * We close the observer here rather than throwing, because a
     * {@link io.grpc.StatusRuntimeException} thrown out of a service method is
     * reported to the client as {@code UNKNOWN}; only {@code onError} transmits
     * the intended status.
     */
    private boolean denyComponentManagement(StreamObserver<?> responseObserver) {
        if (tikaGrpcConfig.isAllowComponentManagement()) {
            return false;
        }
        responseObserver.onError(io.grpc.Status.PERMISSION_DENIED
                .withDescription("Runtime component management is disabled. Set "
                        + "'allowComponentManagement' to true in the 'grpc' section of your "
                        + "tika-config to allow SaveFetcher/DeleteFetcher/SavePipesIterator/"
                        + "DeletePipesIterator. Understand the security implications first.")
                .asRuntimeException());
        return true;
    }

    /**
     * If the request carries per-request configuration
     * ({@code additional_fetch_config_json} or {@code parse_context_json}) but the
     * operator has not opted in, closes the call with {@code PERMISSION_DENIED} and
     * returns {@code true}. A request with no per-request config is always allowed.
     * The caller must {@code return} immediately when this returns {@code true}.
     */
    private boolean denyPerRequestConfig(FetchAndParseRequest request,
                                         StreamObserver<?> responseObserver) {
        return denyPerRequestConfig(request.getAdditionalFetchConfigJson(),
                request.getParseContextJson(), responseObserver);
    }

    /**
     * Shared by v1 and v2 parse RPCs: reject per-request config unless the operator
     * has opted in. Returns {@code true} when the call was closed with
     * {@code PERMISSION_DENIED}.
     */
    boolean denyPerRequestConfig(String additionalFetchConfigJson, String parseContextJson,
                                 StreamObserver<?> responseObserver) {
        boolean hasPerRequestConfig =
                StringUtils.isNotBlank(additionalFetchConfigJson)
                        || StringUtils.isNotBlank(parseContextJson);
        if (!hasPerRequestConfig || tikaGrpcConfig.isAllowPerRequestConfig()) {
            return false;
        }
        responseObserver.onError(io.grpc.Status.PERMISSION_DENIED
                .withDescription("Per-request configuration is disabled. Set "
                        + "'allowPerRequestConfig' to true in the 'grpc' section of your "
                        + "tika-config to allow additional_fetch_config_json / "
                        + "parse_context_json. Understand the security implications first.")
                .asRuntimeException());
        return true;
    }

    @Override
    public void fetchAndParseServerSideStreaming(FetchAndParseRequest request,
                                                 StreamObserver<FetchAndParseReply> responseObserver) {
        if (denyPerRequestConfig(request, responseObserver)) {
            return;
        }
        ParseContext parseContext = buildRequestParseContext(request, responseObserver);
        if (parseContext == null) {
            return;
        }
        fetchAndParseImpl(request, parseContext, responseObserver);
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<FetchAndParseRequest> fetchAndParseBiDirectionalStreaming(
            StreamObserver<FetchAndParseReply> responseObserver) {
        return new StreamObserver<>() {
            @Override
            public void onNext(FetchAndParseRequest fetchAndParseRequest) {
                if (denyPerRequestConfig(fetchAndParseRequest, responseObserver)) {
                    return;
                }
                ParseContext parseContext =
                        buildRequestParseContext(fetchAndParseRequest, responseObserver);
                if (parseContext == null) {
                    return;
                }
                fetchAndParseImpl(fetchAndParseRequest, parseContext, responseObserver);
            }

            @Override
            public void onError(Throwable throwable) {
                LOG.error("Parse error occurred", throwable);
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void fetchAndParse(FetchAndParseRequest request,
                              StreamObserver<FetchAndParseReply> responseObserver) {
        if (denyPerRequestConfig(request, responseObserver)) {
            return;
        }
        ParseContext parseContext = buildRequestParseContext(request, responseObserver);
        if (parseContext == null) {
            return;
        }
        fetchAndParseImpl(request, parseContext, responseObserver);
        responseObserver.onCompleted();
    }

    /**
     * Builds the per-request {@link ParseContext} from {@code parse_context_json}, or closes the
     * call with {@code INVALID_ARGUMENT} and returns {@code null}. The caller must {@code return}
     * immediately on {@code null}.
     * <p>
     * The request is untrusted wire input, so it is deserialized restricted and resolved here, as
     * the tika-server resources do. Left to the fork, the same refusal (of
     * {@code exception-reporting}, say) is a deserialization failure that exits it: a JVM restart
     * per bad request, and a crash status with no reason for the caller. The trade-off, accepted
     * deliberately: a component registered only on the fork's classpath is refused here too --
     * fail-fast validation runs against this JVM's registries. Serialization still forwards the
     * caller's original jsonConfigs, not the instances resolution materializes.
     */
    private ParseContext buildRequestParseContext(FetchAndParseRequest request,
                                                  StreamObserver<?> responseObserver) {
        return buildRequestParseContext(request.getFetcherId(),
                request.getAdditionalFetchConfigJson(), request.getParseContextJson(),
                responseObserver);
    }

    /**
     * Shared by v1 and v2 parse RPCs: see {@link #buildRequestParseContext(FetchAndParseRequest,
     * StreamObserver)}.
     */
    ParseContext buildRequestParseContext(String fetcherId, String additionalFetchConfigJson,
                                          String parseContextJson,
                                          StreamObserver<?> responseObserver) {
        ParseContext parseContext = new ParseContext();
        if (StringUtils.isNotBlank(parseContextJson)) {
            try {
                parseContext = ParseContextDeserializer.readParseContext(
                        OBJECT_MAPPER.readTree(parseContextJson), true);
                ParseContextUtils.resolveAll(parseContext, getClass().getClassLoader());
            } catch (IOException | TikaConfigException e) {
                responseObserver.onError(io.grpc.Status.INVALID_ARGUMENT
                        .withDescription("Invalid parse_context_json: " + e.getMessage())
                        .asRuntimeException());
                return null;
            }
        }
        if (StringUtils.isNotBlank(additionalFetchConfigJson)) {
            // The fork reads this jsonConfig by the fetcher's registered component name
            // (e.g. "http-fetcher"). An unregistered key fails the fork's resolveAll and a
            // wire-blocked one is refused by its restricted tuple deserialization -- both
            // exit the worker. Enforce here: a 400, not a JVM restart per request.
            var info = ComponentNameResolver.getComponentInfo(fetcherId);
            if (info.isEmpty() || ComponentNameResolver.isWireBlocked(
                    ComponentNameResolver.determineContextKey(info.get()))) {
                responseObserver.onError(io.grpc.Status.INVALID_ARGUMENT
                        .withDescription("additional_fetch_config_json requires fetcher_id to "
                                + "be a registered, wire-allowed component name; got '"
                                + fetcherId + "'")
                        .asRuntimeException());
                return null;
            }
            parseContext.setJsonConfig(fetcherId, additionalFetchConfigJson);
        }
        return parseContext;
    }

    private void fetchAndParseImpl(FetchAndParseRequest request, ParseContext parseContext,
                                   StreamObserver<FetchAndParseReply> responseObserver) {
        FetchParseOutcome outcome = runPublicFetchAndParse(
                request.getFetcherId(), request.getFetchKey(), parseContext);
        if (outcome == null) {
            return;
        }
        FetchAndParseReply.Builder fetchReplyBuilder =
                FetchAndParseReply.newBuilder()
                        .setFetchKey(outcome.fetchKey())
                        .setStatus(outcome.status())
                        .putAllFields(outcome.fields());
        if (outcome.errorMessage() != null) {
            fetchReplyBuilder.setErrorMessage(outcome.errorMessage());
        }
        responseObserver.onNext(fetchReplyBuilder.build());
    }

    /**
     * Public-surface variant of {@link #runFetchAndParse}: a host-wired fetcher id (the
     * reserved {@code __} namespace, this server's spool fetcher among them) fails exactly
     * the way an id that was never registered fails.
     */
    FetchParseOutcome runPublicFetchAndParse(String fetcherId, String fetchKey,
                                             ParseContext parseContext) {
        if (ComponentIds.isSystem(fetcherId)) {
            throw new RuntimeException("Could not find fetcher with name " + fetcherId);
        }
        return runFetchAndParse(fetcherId, fetchKey, parseContext);
    }

    /**
     * Shared pipes round-trip used by the v1 {@code fields}-map reply and the v2 typed
     * {@code Document} reply. {@code parseContext} is the per-request context already
     * validated by {@link #buildRequestParseContext}. Returns primary metadata as
     * {@code null} when the pipes result carried no metadata list (so the v2 builder can
     * distinguish empty output from an empty {@link Metadata} object).
     */
    FetchParseOutcome runFetchAndParse(String fetcherId, String fetchKey,
                                       ParseContext parseContext) {
        Fetcher fetcher;
        try {
            fetcher = fetcherManager.getFetcher(fetcherId);
        } catch (TikaException | IOException e) {
            throw new RuntimeException("Could not find fetcher with name " + fetcherId, e);
        }

        Metadata tikaMetadata = new Metadata();
        // Times the whole pipesParser.parse() round trip: fetch and parse both happen
        // inside the forked pipes worker, so this is fetch+parse latency, not parse-only.
        long fetchParseStart = System.nanoTime();
        try {
            return executeTuple(new FetchEmitTuple(
                    fetchKey,
                    new FetchKey(fetcher.getExtensionConfig().id(), fetchKey),
                    new EmitKey(),
                    tikaMetadata,
                    parseContext,
                    FetchEmitTuple.ON_PARSE_EXCEPTION.SKIP), fetchParseStart);
        } catch (IOException | PipesException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /** Runs one tuple through the pipes parser and folds the result into an outcome. */
    FetchParseOutcome executeTuple(FetchEmitTuple tuple, long fetchParseStart)
            throws PipesException, InterruptedException, IOException {
        String fetchKey = tuple.getFetchKey().getFetchKey();
        PipesResult pipesResult = pipesParser.parse(tuple);
        long fetchParseTimeMs = (System.nanoTime() - fetchParseStart) / 1_000_000L;
        Map<String, String> fields = new LinkedHashMap<>();
        Metadata primary = null;
        if (pipesResult.emitData() != null && pipesResult.emitData().getMetadataList() != null) {
            for (Metadata metadata : pipesResult.emitData().getMetadataList()) {
                for (String name : metadata.names()) {
                    String value = metadata.get(name);
                    if (value != null) {
                        fields.put(name, value);
                    }
                }
            }
            if (!pipesResult.emitData().getMetadataList().isEmpty()) {
                primary = pipesResult.emitData().getMetadataList().get(0);
            }
        }
        String errorMessage = null;
        if (pipesResult.status().equals(PipesResult.RESULT_STATUS.FETCH_EXCEPTION)) {
            errorMessage = pipesResult.message();
        }
        return new FetchParseOutcome(
                fetchKey,
                pipesResult.status().name(),
                errorMessage,
                fields,
                primary,
                fetchParseTimeMs);
    }

    /**
     * Parses the exact bytes the caller already holds and returns the parse outcome.
     *
     * <p>The bytes are routed the same way tika-server routes request bodies
     * ({@link PayloadRouter}): at or under {@code pipes.maxInlineBytes} they travel inline in
     * the IPC message via {@link BytesFetcher}; above it they are spooled into
     * {@code parseBytesDir} and fetched by the reserved {@link #parseBytesFetcherId}
     * file-system fetcher. Either way the spool never becomes document semantics: the
     * returned metadata is a {@link #sanitizedCopy(Metadata, String)}.
     *
     * @param requestContext the per-request context already validated by
     *                       {@link #buildRequestParseContext}; {@code null} means none
     * @return {@code null} when the calling thread was interrupted before a reply could be built
     */
    ParseBytesOutcome runParseBytes(InputStream content, long size, String resourceNameHint,
                                    ParseContext requestContext) throws IOException {
        if (content == null || size <= 0) {
            throw new IllegalArgumentException("content is required");
        }
        // Reject before routing; gRPC bounds the full request separately.
        if (size > parseBytesMaxContentBytes) {
            throw new ParseBytesTooLargeException(
                    "content exceeds ParseBytes bound of " + parseBytesMaxContentBytes
                            + " bytes");
        }
        Metadata tikaMetadata = new Metadata();
        if (resourceNameHint != null && !resourceNameHint.isBlank()) {
            tikaMetadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, resourceNameHint.trim());
        }
        long fetchParseStart = System.nanoTime();
        try (TikaInputStream tis = TikaInputStream.get(content);
                PayloadRouter.Routed routed = PayloadRouter.route(tis,
                        pipesConfig.getMaxInlineBytes(),
                        () -> Files.createTempFile(parseBytesDir, "parse-bytes-", ""))) {
            ParseContext parseContext =
                    requestContext == null ? new ParseContext() : requestContext;
            FetchKey fetchKey;
            if (routed.isInline()) {
                parseContext.set(InlineBytes.class, routed.inlineBytes());
                // The fetch key doubles as the caller's filename: nothing to scrub later.
                fetchKey = new FetchKey(BytesFetcher.FETCHER_ID,
                        resourceNameHint == null ? "" : resourceNameHint.trim());
            } else {
                fetchKey = new FetchKey(parseBytesFetcherId,
                        routed.path().getFileName().toString());
            }
            FetchParseOutcome outcome = executeTuple(new FetchEmitTuple(
                    fetchKey.getFetchKey(),
                    fetchKey,
                    new EmitKey(),
                    tikaMetadata,
                    parseContext,
                    FetchEmitTuple.ON_PARSE_EXCEPTION.SKIP), fetchParseStart);
            if (outcome == null) {
                return null;
            }
            return new ParseBytesOutcome(
                    sanitizedCopy(outcome.primary(), resourceNameHint),
                    outcome.status(),
                    outcome.fetchParseTimeMs());
        } catch (PipesException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /**
     * Copies the parse metadata without the two traces the spool file leaves in it: the
     * fetcher records the fetch key under {@link TikaCoreProperties#SOURCE_PATH}, and
     * Tika falls back to the spool filename as the resource name when the caller
     * supplied none. The copy is owned by the outcome, so nothing downstream shares
     * state with the pipes result.
     *
     * <p>The copy is written as a trusted transformation target, which is what
     * {@link Metadata#addTrusted(String, String)} exists for: plain writes drop reserved
     * Tika metadata keys such as the parsed-by chain and the observed digest.
     *
     * @return {@code null} when {@code source} is null, since that is how the mapper
     *         distinguishes "pipes returned no metadata" from an empty document
     */
    private static Metadata sanitizedCopy(Metadata source, String resourceNameHint) {
        if (source == null) {
            // null means "pipes returned no metadata at all" and DocumentBuilder relies on
            // that distinction; an empty Metadata would report a failure as an empty
            // document.
            return null;
        }
        Metadata copy = new Metadata();
        boolean dropResourceName = resourceNameHint == null || resourceNameHint.isBlank();
        String sourcePathKey = TikaCoreProperties.SOURCE_PATH.getName();
        String resourceNameKey = TikaCoreProperties.RESOURCE_NAME_KEY.getName();
        for (String name : source.names()) {
            if (name.equals(sourcePathKey)
                    || (dropResourceName && name.equals(resourceNameKey))) {
                continue;
            }
            for (String value : source.getValues(name)) {
                copy.addTrusted(name, value);
            }
        }
        return copy;
    }

    static void deleteQuietly(Path path) {
        if (path == null) {
            return;
        }
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            LOG.debug("Could not delete {}", path, e);
        }
    }

    private static void deleteRecursivelyQuietly(Path dir) {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(TikaGrpcServerImpl::deleteQuietly);
        } catch (IOException e) {
            LOG.warn("Could not clean ParseBytes temp dir {}", dir, e);
        }
    }

    static final class ParseBytesTooLargeException extends RuntimeException {
        ParseBytesTooLargeException(String message) {
            super(message);
        }
    }

    /**
     * Outcome of a ParseBytes round trip. The spool file, when one existed, is already
     * deleted by the time this is constructed: {@link PayloadRouter.Routed} owns it and the
     * try-with-resources in {@link #runParseBytes} closes it right after the parse.
     * {@code null} primary means the pipes worker returned no metadata at all.
     */
    record ParseBytesOutcome(Metadata primary, String status, long fetchParseTimeMs) {
    }

    /**
     * Outcome of a single fetch+parse pipes round trip, shared by v1 and v2 reply builders.
     * {@code null} means the calling thread was interrupted before a reply could be built.
     */
    record FetchParseOutcome(
            String fetchKey,
            String status,
            String errorMessage,
            Map<String, String> fields,
            Metadata primary,
            long fetchParseTimeMs) {
    }

    @SuppressWarnings("raw")
    @Override
    public void saveFetcher(SaveFetcherRequest request,
                              StreamObserver<SaveFetcherReply> responseObserver) {
        // Host-wired fetchers (reserved namespace) must not be replaced. Save has no
        // unknown-id twin (an unknown id creates a fetcher), so refuse neutrally.
        if (ComponentIds.isSystem(request.getFetcherId())) {
            responseObserver.onError(io.grpc.Status.INVALID_ARGUMENT
                    .withDescription("invalid fetcher id")
                    .asRuntimeException());
            return;
        }
        if (denyComponentManagement(responseObserver)) {
            return;
        }
        String fetcherType = request.getFetcherType();
        if (!isRegisteredFetcherType(fetcherType)) {
            responseObserver.onError(io.grpc.Status.INVALID_ARGUMENT
                    .withDescription("Unknown fetcher type: '" + fetcherType
                            + "'. Use the short factory name (e.g. 'file-system-fetcher').")
                    .asRuntimeException());
            return;
        }
        SaveFetcherReply reply =
                SaveFetcherReply.newBuilder().setFetcherId(request.getFetcherId()).build();
        try {
            // The fetcher type is the factory short name, used directly as the ConfigStore key
            // (shared with PipesServer) -- no class resolution or construction needed.
            ExtensionConfig config = new ExtensionConfig(request.getFetcherId(), fetcherType, request.getFetcherConfigJson());
            fetcherManager.saveFetcher(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    private boolean isRegisteredFetcherType(String fetcherType) {
        return findFetcherFactory(fetcherType) != null;
    }

    private FetcherFactory findFetcherFactory(String fetcherType) {
        for (FetcherFactory factory : pluginManager.getExtensions(FetcherFactory.class)) {
            if (factory.getName().equals(fetcherType)) {
                return factory;
            }
        }
        return null;
    }

    private boolean isRegisteredIteratorType(String iteratorType) {
        if (iteratorType == null) {
            return false;
        }
        for (PipesIteratorFactory factory : pluginManager.getExtensions(PipesIteratorFactory.class)) {
            if (factory.getName().equals(iteratorType)) {
                return true;
            }
        }
        return false;
    }
    static Status notFoundStatus(String fetcherId) {
        return Status.newBuilder()
                .setCode(io.grpc.Status.Code.NOT_FOUND.value())
                .setMessage("Could not find fetcher with id:" + fetcherId)
                .build();
    }

    @Override
    public void getFetcher(GetFetcherRequest request,
                           StreamObserver<GetFetcherReply> responseObserver) {
        // Host-wired fetchers (reserved namespace): identical answer to an id that does not
        // exist.
        if (ComponentIds.isSystem(request.getFetcherId())) {
            responseObserver.onError(StatusProto.toStatusException(
                    notFoundStatus(request.getFetcherId())));
            return;
        }
        GetFetcherReply.Builder getFetcherReply = GetFetcherReply.newBuilder();
        try {
            Fetcher fetcher = fetcherManager.getFetcher(request.getFetcherId());
            ExtensionConfig config = fetcher.getExtensionConfig();

            getFetcherReply.setFetcherId(config.id());
            getFetcherReply.setFetcherType(config.name());

            // The config may carry secrets (passwords, access keys, tokens). Only return it once the
            // operator has opted in to runtime component management; identity is always safe.
            if (tikaGrpcConfig.isAllowComponentManagement()) {
                Map<String, Object> paramMap = OBJECT_MAPPER.readValue(config.json(), new TypeReference<>() {
                });
                paramMap.forEach((k, v) -> getFetcherReply.putParams(Objects.toString(k), Objects.toString(v)));
            }

            responseObserver.onNext(getFetcherReply.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(StatusProto.toStatusException(notFoundStatus(request.getFetcherId())));
        }
    }

    @Override
    public void listFetchers(ListFetchersRequest request,
                             StreamObserver<ListFetchersReply> responseObserver) {
        ListFetchersReply.Builder listFetchersReplyBuilder = ListFetchersReply.newBuilder();
        // The config may carry secrets; only include it once component management is enabled.
        boolean includeConfig = tikaGrpcConfig.isAllowComponentManagement();
        for (String fetcherId : fetcherManager.getSupported()) {
            try {
                Fetcher fetcher = fetcherManager.getFetcher(fetcherId);
                ExtensionConfig config = fetcher.getExtensionConfig();

                GetFetcherReply.Builder replyBuilder = GetFetcherReply.newBuilder().setFetcherId(config.id()).setFetcherType(config.name());

                if (includeConfig) {
                    Map<String, Object> paramMap = OBJECT_MAPPER.readValue(config.json(), new TypeReference<>() {
                    });
                    paramMap.forEach((k, v) -> replyBuilder.putParams(Objects.toString(k), Objects.toString(v)));
                }

                listFetchersReplyBuilder.addGetFetcherReplies(replyBuilder.build());
            } catch (Exception e) {
                LOG.error("Error listing fetcher: {}", fetcherId, e);
            }
        }
        responseObserver.onNext(listFetchersReplyBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteFetcher(DeleteFetcherRequest request,
                              StreamObserver<DeleteFetcherReply> responseObserver) {
        // Host-wired fetchers (reserved namespace): identical answer to an id that does not
        // exist (success=false, no error).
        if (ComponentIds.isSystem(request.getFetcherId())) {
            responseObserver.onNext(DeleteFetcherReply.newBuilder().setSuccess(false).build());
            responseObserver.onCompleted();
            return;
        }
        if (denyComponentManagement(responseObserver)) {
            return;
        }
        boolean successfulDelete = deleteFetcher(request.getFetcherId());
        responseObserver.onNext(DeleteFetcherReply.newBuilder().setSuccess(successfulDelete).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getFetcherConfigJsonSchema(GetFetcherConfigJsonSchemaRequest request, StreamObserver<GetFetcherConfigJsonSchemaReply> responseObserver) {
        GetFetcherConfigJsonSchemaReply.Builder builder = GetFetcherConfigJsonSchemaReply.newBuilder();
        String fetcherType = request.getFetcherType();
        // Only resolve config classes from registered fetcher factories -- never load an arbitrary
        // classpath class on a client's say-so.
        FetcherFactory factory = findFetcherFactory(fetcherType);
        if (factory == null) {
            responseObserver.onError(io.grpc.Status.INVALID_ARGUMENT
                    .withDescription("Unknown fetcher type: '" + fetcherType
                            + "'. Use the short factory name (e.g. 'file-system-fetcher').")
                    .asRuntimeException());
            return;
        }
        try {
            JsonSchema jsonSchema = JSON_SCHEMA_GENERATOR.generateSchema(factory.getConfigClass());
            builder.setFetcherConfigJsonSchema(OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(jsonSchema));
        } catch (JsonProcessingException e) {
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Could not create json schema for fetcher type " + fetcherType)
                    .withCause(e)
                    .asRuntimeException());
            return;
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    private boolean deleteFetcher(String id) {
        try {
            // Delete from fetcher manager (updates ConfigStore which is shared with PipesServer)
            fetcherManager.deleteFetcher(id);
            LOG.info("Successfully deleted fetcher: {}", id);
            return true;
        } catch (Exception e) {
            LOG.error("Failed to delete fetcher: {}", id, e);
            return false;
        }
    }
    
    // ========== PipesIterator RPC Methods ==========
    
    @Override
    public void savePipesIterator(SavePipesIteratorRequest request,
                                  StreamObserver<SavePipesIteratorReply> responseObserver) {
        if (denyComponentManagement(responseObserver)) {
            return;
        }
        try {
            String iteratorId = request.getIteratorId();
            String iteratorType = request.getIteratorType();
            String iteratorConfigJson = request.getIteratorConfigJson();

            // Validate the iterator type up front, mirroring saveFetcher: reject unknown types
            // rather than persisting an unvalidated entry into the shared ConfigStore that would
            // only fail later (or in a co-deployed PipesServer that consumes iterator entries).
            if (!isRegisteredIteratorType(iteratorType)) {
                responseObserver.onError(io.grpc.Status.INVALID_ARGUMENT
                        .withDescription("Unknown pipes iterator type: '" + iteratorType
                                + "'. Use the short factory name (e.g. 'file-system-pipes-iterator').")
                        .asRuntimeException());
                return;
            }

            LOG.info("Saving pipes iterator: id={}, type={}", iteratorId, iteratorType);

            ExtensionConfig config = new ExtensionConfig(iteratorId, iteratorType, iteratorConfigJson);

            // Save directly to ConfigStore (shared with PipesServer)
            configStore.put(PIPES_ITERATOR_PREFIX + iteratorId, config);

            SavePipesIteratorReply reply = SavePipesIteratorReply.newBuilder()
                    .setMessage("Pipes iterator saved successfully")
                    .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();

            LOG.info("Successfully saved pipes iterator: {}", iteratorId);

        } catch (Exception e) {
            LOG.error("Failed to save pipes iterator", e);
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Failed to save pipes iterator: " + e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }
    
    @Override
    public void getPipesIterator(GetPipesIteratorRequest request,
                                 StreamObserver<GetPipesIteratorReply> responseObserver) {
        try {
            String iteratorId = request.getIteratorId();
            LOG.info("Getting pipes iterator: {}", iteratorId);

            // Get directly from ConfigStore (shared with PipesServer)
            ExtensionConfig config = configStore.get(PIPES_ITERATOR_PREFIX + iteratorId);

            if (config == null) {
                responseObserver.onError(io.grpc.Status.NOT_FOUND
                        .withDescription("Pipes iterator not found: " + iteratorId)
                        .asRuntimeException());
                return;
            }

            GetPipesIteratorReply.Builder reply = GetPipesIteratorReply.newBuilder()
                    .setIteratorId(config.id())
                    .setIteratorType(config.name());
            // The iterator config may carry secrets; only include it once component management is
            // enabled (identity is always safe to return).
            if (tikaGrpcConfig.isAllowComponentManagement()) {
                reply.setIteratorConfigJson(config.json());
            }
            responseObserver.onNext(reply.build());
            responseObserver.onCompleted();

            LOG.info("Successfully retrieved pipes iterator: {}", iteratorId);

        } catch (Exception e) {
            LOG.error("Failed to get pipes iterator", e);
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Failed to get pipes iterator: " + e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }
    
    @Override
    public void deletePipesIterator(DeletePipesIteratorRequest request,
                                    StreamObserver<DeletePipesIteratorReply> responseObserver) {
        if (denyComponentManagement(responseObserver)) {
            return;
        }
        try {
            String iteratorId = request.getIteratorId();
            LOG.info("Deleting pipes iterator: {}", iteratorId);

            // Delete directly from ConfigStore (shared with PipesServer)
            configStore.remove(PIPES_ITERATOR_PREFIX + iteratorId);

            DeletePipesIteratorReply reply = DeletePipesIteratorReply.newBuilder()
                    .setMessage("Pipes iterator deleted successfully")
                    .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();

            LOG.info("Successfully deleted pipes iterator: {}", iteratorId);

        } catch (Exception e) {
            LOG.error("Failed to delete pipes iterator", e);
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Failed to delete pipes iterator: " + e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    /**
     * Releases resources, including the embedded Ignite server if one was started.
     */
    public void shutdown() {
        if (igniteStoreServer != null) {
            LOG.info("Shutting down embedded Ignite server");
            try {
                igniteStoreServer.close();
            } catch (Exception e) {
                LOG.error("Error shutting down Ignite server", e);
            } finally {
                igniteStoreServer = null;
            }
        }
    }

    /**
     * Close the pipes parser, to be called after TikaGrpcServer has shut down.
     */
    void postShutdown() {
        if (pipesParser != null) {
            LOG.info("Shutting down the pipes parser");
            try {
                pipesParser.close();
            } catch (IOException e) {
                LOG.error("Error closing the pipes parser", e);
            } finally {
                pipesParser = null;
            }
        }
        // TikaGrpcServer.stop() calls shutdown() before the gRPC server drains and this
        // after it: only here can no request still be reading the spool.
        if (configStore != null) {
            // This server and its forks put the spool fetcher into the store at their start;
            // in a shared store it would outlive the directory it points at. containsKey first:
            // a file-backed store re-reads its file on lookups but not on remove, which writes
            // this JVM's cache back -- stale, it would take other servers' entries with it.
            try {
                if (configStore.containsKey(parseBytesFetcherId)) {
                    configStore.remove(parseBytesFetcherId);
                }
            } catch (RuntimeException e) {
                LOG.debug("Could not remove {} from the config store", parseBytesFetcherId, e);
            }
        }
        releaseParseBytesArtifacts();
    }

    /** Removes the spool directory and the effective config; safe to call more than once. */
    private void releaseParseBytesArtifacts() {
        deleteRecursivelyQuietly(parseBytesDir);
        parseBytesDir = null;
        if (effectiveConfigPath != null && !effectiveConfigPath.equals(tikaConfigPath)) {
            deleteQuietly(effectiveConfigPath);
        }
        effectiveConfigPath = null;
    }
}
