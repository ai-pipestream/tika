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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.grpc.reflection.v1.FileDescriptorResponse;
import io.grpc.reflection.v1.ServerReflectionGrpc;
import io.grpc.reflection.v1.ServerReflectionRequest;
import io.grpc.reflection.v1.ServerReflectionResponse;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.apache.tika.grpc.v2.Document;
import org.apache.tika.grpc.v2.ExtensionResult;
import org.apache.tika.grpc.v2.ParseBytesRequest;
import org.apache.tika.grpc.v2.ParseBytesResponse;
import org.apache.tika.grpc.v2.SourceOrigin;
import org.apache.tika.grpc.v2.TikaV2Proto;
import org.apache.tika.grpc.v2.TikaV2ServiceGrpc;

/**
 * The composer against in-process services. The "external parser" is a second TikaV2Service
 * reached only through reflection: the composer has its generated classes on the classpath,
 * but never uses them for that call.
 */
class ComposeDemoTest {

    private static final byte[] CONTENT = "WARC/1.0\r\n".getBytes(StandardCharsets.UTF_8);

    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    /** Answers ParseBytes with the byte count and, as Tika does, the bytes' digest. */
    private static final class FakeParser extends TikaV2ServiceGrpc.TikaV2ServiceImplBase {
        private final String contentType;

        FakeParser(String contentType) {
            this.contentType = contentType;
        }

        @Override
        public void parseBytes(ParseBytesRequest request,
                               StreamObserver<ParseBytesResponse> observer) {
            observer.onNext(ParseBytesResponse.newBuilder()
                    .setCorrelationId(request.getCorrelationId())
                    .setDocument(Document.newBuilder()
                            .setId(request.getCorrelationId())
                            .setContentType(contentType)
                            .setOrigin(SourceOrigin.newBuilder()
                                    .setByteSize(request.getContent().size())
                                    .setSha256(ComposeDemo.sha256(
                                            request.getContent().toByteArray()))))
                    .build());
            observer.onCompleted();
        }
    }

    private ManagedChannel start(io.grpc.BindableService service, boolean reflection)
            throws Exception {
        String name = InProcessServerBuilder.generateName();
        InProcessServerBuilder builder = InProcessServerBuilder.forName(name).addService(service);
        if (reflection) {
            builder.addService(ProtoReflectionServiceV1.newInstance());
        }
        Server server = builder.build().start();
        closeables.add(() -> server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS));
        ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
        closeables.add(0, () -> channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS));
        return channel;
    }

    @Test
    void composesTikasDocumentWithTheExternalReplyAsAnAny() throws Exception {
        ManagedChannel tika = start(new FakeParser("application/warc"), false);
        ManagedChannel external = start(new FakeParser("external/x"), true);

        ComposeDemo.Composed composed = new ComposeDemo()
                // No bytes field named: ParseBytesRequest has exactly one, content.
                .method("org.apache.tika.grpc.v2.TikaV2Service/ParseBytes", null, "fake")
                .compose(tika, external, CONTENT, "crawl.warc");

        Document document = composed.document();
        assertEquals("crawl.warc", document.getId());
        assertEquals("application/warc", document.getContentType());
        assertEquals(1, document.getExtensionsCount());
        ExtensionResult result = document.getExtensions(0);
        assertEquals("fake", result.getProducerId());
        assertEquals(ExtensionResult.Status.STATUS_OK, result.getStatus(),
                "warnings=" + result.getWarningsList());
        ParseBytesResponse payload = result.getPayload().unpack(ParseBytesResponse.class);
        assertEquals(CONTENT.length, payload.getDocument().getOrigin().getByteSize());
        assertTrue(composed.toJson().contains("\"external/x\""),
                "the JSON prints the payload's fields: " + composed.toJson());
    }

    /** An external parser that fails still gets its slot, as STATUS_FAILED with the reason. */
    @Test
    void aFailingExternalParserLeavesAFailedEnvelope() throws Exception {
        ManagedChannel tika = start(new FakeParser("application/warc"), false);
        ManagedChannel noReflection = start(new FakeParser("external/x"), false);

        Document document = new ComposeDemo()
                .method("org.apache.tika.grpc.v2.TikaV2Service/ParseBytes", "content", "fake")
                .compose(tika, noReflection, CONTENT, "crawl.warc")
                .document();

        ExtensionResult result = document.getExtensions(0);
        assertEquals(ExtensionResult.Status.STATUS_FAILED, result.getStatus());
        assertEquals(1, result.getWarningsCount());
    }

    @Test
    void streamingMethodsAndWrongFieldsAreRefused() throws Exception {
        ManagedChannel external = start(new FakeParser("external/x"), true);

        assertThrows(IllegalArgumentException.class, () -> ReflectiveUnaryCall.resolve(external,
                "org.apache.tika.grpc.v2.TikaV2Service/FetchAndParseServerSideStreaming", 5000));
        ReflectiveUnaryCall call = ReflectiveUnaryCall.resolve(external,
                "org.apache.tika.grpc.v2.TikaV2Service/ParseBytes", 5000);
        assertThrows(IllegalArgumentException.class,
                () -> call.call("", "resource_name", CONTENT, 5000));
    }

    /**
     * A reflection server that answers with only the file asked for, as tonic (Rust) does:
     * the composer fetches the missing dependencies by name.
     */
    @Test
    void fetchesDependenciesAReflectionServerLeavesOut() throws Exception {
        String name = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(name)
                .addService(new FakeParser("external/x"))
                .addService(new FileOnlyReflection())
                .build()
                .start();
        closeables.add(() -> server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS));
        ManagedChannel external = InProcessChannelBuilder.forName(name).build();
        closeables.add(0, () -> external.shutdownNow().awaitTermination(5, TimeUnit.SECONDS));

        ReflectiveUnaryCall call = ReflectiveUnaryCall.resolve(external,
                "org.apache.tika.grpc.v2.TikaV2Service/ParseBytes", 5000);
        DynamicMessage reply = call.call("", null, CONTENT, 5000);

        assertEquals("org.apache.tika.grpc.v2.ParseBytesResponse",
                reply.getDescriptorForType().getFullName());
    }

    /** Serves each file without its dependencies, from the files linked into this test. */
    private static final class FileOnlyReflection extends ServerReflectionGrpc.ServerReflectionImplBase {
        private static final Map<String, Descriptors.FileDescriptor> FILES = new HashMap<>();

        static {
            add(TikaV2Proto.getDescriptor());
        }

        private static void add(Descriptors.FileDescriptor file) {
            FILES.put(file.getName(), file);
            file.getDependencies().forEach(FileOnlyReflection::add);
        }

        @Override
        public StreamObserver<ServerReflectionRequest> serverReflectionInfo(
                StreamObserver<ServerReflectionResponse> responses) {
            return new StreamObserver<>() {
                @Override
                public void onNext(ServerReflectionRequest request) {
                    Descriptors.FileDescriptor file = request.hasFileByFilename()
                            ? FILES.get(request.getFileByFilename())
                            : TikaV2Proto.getDescriptor();
                    responses.onNext(ServerReflectionResponse.newBuilder()
                            .setFileDescriptorResponse(FileDescriptorResponse.newBuilder()
                                    .addFileDescriptorProto(file.toProto().toByteString()))
                            .build());
                }

                @Override
                public void onError(Throwable t) {
                }

                @Override
                public void onCompleted() {
                    responses.onCompleted();
                }
            };
        }
    }
}
