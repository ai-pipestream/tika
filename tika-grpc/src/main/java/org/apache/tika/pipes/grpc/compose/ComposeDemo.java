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
package org.apache.tika.pipes.grpc.compose;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;

import org.apache.tika.grpc.mapper.DocumentComposer;
import org.apache.tika.grpc.v2.Document;
import org.apache.tika.grpc.v2.ExtensionResult;
import org.apache.tika.grpc.v2.ParseBytesRequest;
import org.apache.tika.grpc.v2.TikaV2ServiceGrpc;

/**
 * Experimental TIKA-4771 round-one composer: sends one file to Tika's ParseBytes and to one
 * external parser's unary gRPC method, then prints one composed {@link Document} as JSON.
 * The external parser needs no Tika-specific code, only gRPC reflection. Example:
 * <pre>
 * ComposeDemo --file crawl.warc.gz --external localhost:50060 \
 *     --method fastwarc.v1.WarcService/ParseArchive --producer fastwarc
 * </pre>
 */
public final class ComposeDemo {

    @Parameter(names = "--file", required = true, description = "File to parse")
    private String file;

    @Parameter(names = "--tika", description = "tika-grpc host:port")
    private String tika = "localhost:50052";

    @Parameter(names = "--external", required = true, description = "External parser host:port")
    private String external;

    @Parameter(names = "--method", required = true,
            description = "Unary method, package.Service/Method")
    private String method;

    @Parameter(names = "--bytes-field", description = "Request field that gets the file "
            + "bytes; defaults to the request's only bytes field")
    private String bytesField;

    @Parameter(names = "--request-json", description = "Other request fields, as JSON")
    private String requestJson = "";

    @Parameter(names = "--producer", description = "Slot name for the external result")
    private String producer;

    @Parameter(names = "--work-item", description = "Id for the composed Document")
    private String workItem;

    @Parameter(names = "--timeout-ms", description = "Deadline for each call")
    private long timeoutMs = 60_000;

    @Parameter(names = {"-h", "--help"}, help = true)
    private boolean help;

    ComposeDemo() {
    }

    ComposeDemo method(String method, String bytesField, String producer) {
        this.method = method;
        this.bytesField = bytesField;
        this.producer = producer;
        return this;
    }

    public static void main(String[] args) throws Exception {
        ComposeDemo demo = new ComposeDemo();
        JCommander commander = JCommander.newBuilder().addObject(demo).build();
        commander.parse(args);
        if (demo.help) {
            commander.usage();
            return;
        }
        System.out.println(demo.run());
    }

    private String run() throws IOException, InterruptedException {
        byte[] content = Files.readAllBytes(Path.of(file));
        String fileName = Path.of(file).getFileName().toString();
        ManagedChannel tikaChannel = channel(tika);
        ManagedChannel externalChannel = channel(external);
        try {
            return compose(tikaChannel, externalChannel, content, fileName).toJson();
        } finally {
            shutdown(tikaChannel);
            shutdown(externalChannel);
        }
    }

    /** Parses {@code content} with Tika and the external method, and composes the two. */
    Composed compose(Channel tikaChannel, Channel externalChannel, byte[] content,
                     String fileName) {
        String slot = producer == null || producer.isBlank()
                ? method.substring(0, Math.max(0, method.indexOf('/'))) : producer;
        String id = workItem == null || workItem.isBlank() ? fileName : workItem;

        Document core = TikaV2ServiceGrpc.newBlockingStub(tikaChannel)
                .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                .parseBytes(ParseBytesRequest.newBuilder()
                        .setCorrelationId(id)
                        .setResourceName(fileName)
                        .setContent(ByteString.copyFrom(content))
                        .build())
                .getDocument();

        JsonFormat.TypeRegistry.Builder types = JsonFormat.TypeRegistry.newBuilder();
        ExtensionResult contribution;
        long start = System.nanoTime();
        try {
            ReflectiveUnaryCall call =
                    ReflectiveUnaryCall.resolve(externalChannel, method, timeoutMs);
            types.add(call.responseType());
            DynamicMessage reply = call.call(requestJson, bytesField, content, timeoutMs);
            contribution = ExtensionResult.newBuilder()
                    .setProducerId(slot)
                    .setStatus(ExtensionResult.Status.STATUS_OK)
                    .setPayload(Any.newBuilder()
                            .setTypeUrl("type.googleapis.com/"
                                    + reply.getDescriptorForType().getFullName())
                            .setValue(reply.toByteString()))
                    .build();
        } catch (RuntimeException | IOException e) {
            // Rule 6: a producer that failed still gets its slot, as STATUS_FAILED.
            contribution = DocumentComposer.failed(slot, String.valueOf(e));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            contribution = DocumentComposer.failed(slot, "interrupted");
        }
        contribution = contribution.toBuilder()
                .setElapsedMs((System.nanoTime() - start) / 1_000_000L)
                .build();

        Document composed = DocumentComposer.compose(core, id, sha256(content),
                List.of(contribution), List.of(slot));
        return new Composed(composed, types.build());
    }

    private static ManagedChannel channel(String target) {
        return Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
                .maxInboundMessageSize(256 * 1024 * 1024)
                .build();
    }

    private static void shutdown(ManagedChannel channel) throws InterruptedException {
        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    static String sha256(byte[] content) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** The composed Document, and the types needed to print its payloads as JSON. */
    record Composed(Document document, JsonFormat.TypeRegistry types) {
        String toJson() throws IOException {
            return JsonFormat.printer().usingTypeRegistry(types).print(document);
        }
    }
}
