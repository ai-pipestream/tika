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

import static io.grpc.health.v1.HealthCheckResponse.ServingStatus;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.TimeUnit;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCredentials;
import io.grpc.TlsServerCredentials;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server that manages startup/shutdown of the GRPC Tika server.
 */
public class TikaGrpcServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(TikaGrpcServer.class);
    public static final int TIKA_SERVER_GRPC_DEFAULT_PORT = 50052;
    private Server server;
    private TikaGrpcServerImpl serviceImpl;
    @Parameter(names = {"-p", "--port"}, description = "The grpc server port", help = true)
    private Integer port = TIKA_SERVER_GRPC_DEFAULT_PORT;

    @Parameter(names = {"-c", "--config"}, description = "The tika config file", help = true)
    private File tikaConfig;

    @Parameter(names = {"-l", "--plugins"}, description = "The tika pipes plugins config file", help = true)
    private File tikaPlugins;

    @Parameter(names = {"--plugin-roots"}, description = "Comma-separated list of plugin root directories (overrides config file)", help = true)
    private String pluginRoots;

    @Parameter(names = {"-s", "--secure"}, description = "Enable credentials required to access this grpc server")
    private boolean secure;

    @Parameter(names = {"--cert-chain"}, description = "Certificate chain file. Example: server1.pem See: https://github.com/grpc/grpc-java/tree/b3ffb5078df361d7460786e134db7b5c00939246/examples/example-tls")
    private File certChain;

    @Parameter(names = {"--private-key"}, description = "Private key store. Example: server1.key See: https://github.com/grpc/grpc-java/tree/b3ffb5078df361d7460786e134db7b5c00939246/examples/example-tls")
    private File privateKey;

    @Parameter(names = {"--private-key-password"}, description = "Private key password, if needed")
    private String privateKeyPassword;

    @Parameter(names = {"--trust-cert-collection"}, description = "The trust certificate collection (root certs). Required, and must be a readable file, when --client-auth-required is set. Example: ca.pem See: https://github.com/grpc/grpc-java/tree/b3ffb5078df361d7460786e134db7b5c00939246/examples/example-tls")
    private File trustCertCollection;

    @Parameter(names = {"--client-auth-required"}, description = "Is Mutual TLS required? Implies --secure.")
    private boolean clientAuthRequired;

    @Parameter(names = {"-h", "-H", "--help"}, description = "Display help menu")
    private boolean help;

    public void start() throws Exception {
        HealthStatusManager healthStatusManager = new HealthStatusManager();
        ServerCredentials creds;
        if (clientAuthRequired && !secure) {
            LOGGER.info("--client-auth-required implies --secure; enabling TLS.");
            secure = true;
        }
        if (secure) {
            TlsServerCredentials.Builder channelCredBuilder = TlsServerCredentials.newBuilder();
            channelCredBuilder.keyManager(certChain, privateKey, privateKeyPassword);
            if (clientAuthRequired) {
                if (trustCertCollection == null || !trustCertCollection.isFile() || !trustCertCollection.canRead()) {
                    throw new IllegalArgumentException("--client-auth-required is set but --trust-cert-collection is " +
                            "missing, not a file, or unreadable; refusing to start");
                }
                channelCredBuilder.trustManager(trustCertCollection);
                channelCredBuilder.clientAuth(TlsServerCredentials.ClientAuth.REQUIRE);
            } else if (trustCertCollection != null && trustCertCollection.exists()) {
                channelCredBuilder.trustManager(trustCertCollection);
            }
            creds = channelCredBuilder.build();
        } else {
            creds = InsecureServerCredentials.create();
        }
        if (tikaConfig == null) {
            tikaConfig = extractDefaultConfig();
            LOGGER.info("No config file specified, using bundled default-tika-config.json");
        }
        File tikaConfigFile = new File(tikaConfig.getAbsolutePath());
        healthStatusManager.setStatus(TikaGrpcServer.class.getSimpleName(), ServingStatus.SERVING);
        serviceImpl = new TikaGrpcServerImpl(tikaConfigFile.getAbsolutePath(), pluginRoots);
        ServerBuilder<?> serverBuilder = Grpc.newServerBuilderForPort(port, creds);
        applyInboundLimit(serverBuilder, serviceImpl.tikaGrpcConfig);
        String inertCap = inertCapWarning(serviceImpl.tikaGrpcConfig.getMaxInboundMessageBytes());
        if (inertCap != null) {
            LOGGER.warn(inertCap);
        }
        // v1 (tika.Tika) stays the stable fields-map contract; v2 (TikaV2) is the
        // experimental typed Document surface. Both share the same pipes runtime.
        server = serverBuilder
                .addService(serviceImpl)
                .addService(new TikaGrpcV2ServerImpl(serviceImpl))
                .addService(healthStatusManager.getHealthService())
                .addService(ProtoReflectionServiceV1.newInstance())
                .build()
                .start();
        LOGGER.info("Server started, listening on " + port);
        Runtime
                .getRuntime()
                .addShutdownHook(new Thread(() -> {
                    // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                    System.err.println("*** shutting down gRPC server since JVM is shutting down");
                    healthStatusManager.clearStatus(TikaGrpcServer.class.getSimpleName());
                    try {
                        TikaGrpcServer.this.stop();
                    } catch (InterruptedException e) {
                        e.printStackTrace(System.err);
                    }
                    System.err.println("*** server shut down");
                }));
    }

    public void stop() throws InterruptedException {
        if (serviceImpl != null) {
            serviceImpl.shutdown();
        }
        if (server != null) {
            server
                    .shutdown()
                    .awaitTermination(30, TimeUnit.SECONDS);
        }
        if (serviceImpl != null) {
            serviceImpl.postShutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws Exception {
        TikaGrpcServer server = new TikaGrpcServer();
        JCommander commander = JCommander
                .newBuilder()
                .addObject(server)
                .build();

        commander.parse(args);

        if (server.help) {
            commander.usage();
            return;
        }

        server.start();
        server.blockUntilShutdown();
    }

    private static File extractDefaultConfig() {
        try (InputStream is = TikaGrpcServer.class.getResourceAsStream("/default-tika-config.json")) {
            if (is == null) {
                throw new IllegalArgumentException(
                        "Tika config file is required. Use -c to specify a config file.");
            }
            Path tempConfig = Files.createTempFile("tika-config-", ".json");
            tempConfig.toFile().deleteOnExit();
            Files.copy(is, tempConfig, StandardCopyOption.REPLACE_EXISTING);
            return tempConfig.toFile();
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Tika config file is required. Use -c to specify a config file.", e);
        }
    }

    /**
     * Applies the configured inbound limit, if any. An absent knob leaves the builder
     * exactly as gRPC handed it over, so an unconfigured server keeps gRPC's own default
     * rather than one this project invented.
     *
     * <p>Package-private so the real-transport test can wire a server the same way this
     * method does, instead of reimplementing it and testing its own copy.
     */
    static void applyInboundLimit(ServerBuilder<?> builder, TikaGrpcConfig config) {
        Integer maxInboundMessageBytes = config.getMaxInboundMessageBytes();
        if (maxInboundMessageBytes != null) {
            builder.maxInboundMessageSize(maxInboundMessageBytes);
        }
    }

    /**
     * The warning an operator needs when they have configured a transport limit that puts
     * the ParseBytes content cap out of reach, or {@code null} when there is nothing to
     * say.
     *
     * <p>Equality warns too: a request carrying exactly the cap also carries its field
     * tags, length prefixes and remaining fields, so it exceeds the envelope.
     *
     * <p>This deliberately warns in one direction only. It detects "certainly
     * unreachable", not "reachable": a limit slightly above the cap still cannot carry a
     * full-size request, and no fixed margin would fix that because the rest of the
     * request is itself unbounded ({@code parse_context_json} has no length limit).
     * Silence therefore means "nothing certain to report", never "your cap is reachable".
     *
     * <p>An absent knob does not warn. That is the documented default rather than an
     * operator mistake, and a line on every start of every server for a surface most
     * deployments never call is noise, not signal.
     *
     * <p>Returns the message instead of logging it so the condition can be tested without
     * capturing log output.
     */
    static String inertCapWarning(Integer maxInboundMessageBytes) {
        if (maxInboundMessageBytes == null
                || maxInboundMessageBytes > TikaGrpcServerImpl.PARSE_BYTES_MAX_BYTES) {
            return null;
        }
        return "ParseBytes accepts content up to " + TikaGrpcServerImpl.PARSE_BYTES_MAX_BYTES
                + " bytes, but this server's maxInboundMessageBytes is "
                + maxInboundMessageBytes + ". The request also has to carry its other "
                + "fields, so the transport refuses before the ParseBytes bound applies.";
    }

    public TikaGrpcServer setTikaConfig(File tikaConfig) {
        this.tikaConfig = tikaConfig;
        return this;
    }

    public TikaGrpcServer setServer(Server server) {
        this.server = server;
        return this;
    }

    public TikaGrpcServer setPort(Integer port) {
        this.port = port;
        return this;
    }

    public TikaGrpcServer setSecure(boolean secure) {
        this.secure = secure;
        return this;
    }

    public TikaGrpcServer setCertChain(File certChain) {
        this.certChain = certChain;
        return this;
    }

    public TikaGrpcServer setPrivateKey(File privateKey) {
        this.privateKey = privateKey;
        return this;
    }

    public TikaGrpcServer setPrivateKeyPassword(String privateKeyPassword) {
        this.privateKeyPassword = privateKeyPassword;
        return this;
    }

    public TikaGrpcServer setTrustCertCollection(File trustCertCollection) {
        this.trustCertCollection = trustCertCollection;
        return this;
    }

    public TikaGrpcServer setClientAuthRequired(boolean clientAuthRequired) {
        this.clientAuthRequired = clientAuthRequired;
        return this;
    }
}
