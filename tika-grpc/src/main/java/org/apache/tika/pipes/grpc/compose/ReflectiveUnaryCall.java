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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.reflection.v1.ServerReflectionGrpc;
import io.grpc.reflection.v1.ServerReflectionRequest;
import io.grpc.reflection.v1.ServerReflectionResponse;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;

/**
 * Calls one unary method of a gRPC service that Tika has no generated code for. The
 * service must serve gRPC reflection (v1), which supplies its descriptors; the request is
 * built from JSON, with the document bytes set on one bytes field.
 */
final class ReflectiveUnaryCall {

    private final Channel channel;
    private final MethodDescriptor method;

    private ReflectiveUnaryCall(Channel channel, MethodDescriptor method) {
        this.channel = channel;
        this.method = method;
    }

    /**
     * @param fullMethodName {@code package.Service/Method}, e.g.
     *                       {@code fastwarc.v1.WarcService/ParseArchive}
     */
    static ReflectiveUnaryCall resolve(Channel channel, String fullMethodName, long timeoutMs)
            throws IOException, InterruptedException {
        int slash = fullMethodName.indexOf('/');
        if (slash <= 0 || slash == fullMethodName.length() - 1) {
            throw new IllegalArgumentException("expected package.Service/Method, got "
                    + fullMethodName);
        }
        String serviceName = fullMethodName.substring(0, slash);
        String methodName = fullMethodName.substring(slash + 1);
        FileDescriptor file = fetchFile(channel, serviceName, timeoutMs);
        Descriptors.ServiceDescriptor service = file.findServiceByName(
                serviceName.substring(serviceName.lastIndexOf('.') + 1));
        if (service == null) {
            throw new IllegalArgumentException("no service " + serviceName);
        }
        MethodDescriptor method = service.findMethodByName(methodName);
        if (method == null) {
            throw new IllegalArgumentException("no method " + methodName + " on " + serviceName);
        }
        if (method.isClientStreaming() || method.isServerStreaming()) {
            throw new IllegalArgumentException(fullMethodName + " streams; only unary methods "
                    + "are supported");
        }
        return new ReflectiveUnaryCall(channel, method);
    }

    Descriptors.Descriptor responseType() {
        return method.getOutputType();
    }

    /**
     * Builds the request from {@code requestJson} (may be blank), sets {@code content} on
     * the bytes field {@link #bytesField} picks, and returns the reply.
     */
    DynamicMessage call(String requestJson, String bytesField, byte[] content, long timeoutMs)
            throws InvalidProtocolBufferException {
        Descriptors.Descriptor requestType = method.getInputType();
        DynamicMessage.Builder request = DynamicMessage.newBuilder(requestType);
        if (requestJson != null && !requestJson.isBlank()) {
            JsonFormat.parser().merge(requestJson, request);
        }
        Descriptors.FieldDescriptor field = bytesField(requestType, bytesField);
        request.setField(field, ByteString.copyFrom(content));
        io.grpc.MethodDescriptor<DynamicMessage, DynamicMessage> grpcMethod =
                io.grpc.MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
                        .setType(MethodType.UNARY)
                        .setFullMethodName(io.grpc.MethodDescriptor.generateFullMethodName(
                                method.getService().getFullName(), method.getName()))
                        .setRequestMarshaller(new DynamicMarshaller(requestType))
                        .setResponseMarshaller(new DynamicMarshaller(method.getOutputType()))
                        .build();
        return ClientCalls.blockingUnaryCall(channel, grpcMethod,
                CallOptions.DEFAULT.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS),
                request.build());
    }

    /**
     * The request field that gets the document bytes: the one named {@code name}, or, when
     * {@code name} is blank, the request's only singular bytes field (FastWARC's
     * {@code ParseArchiveRequest.archive}, for example).
     */
    static Descriptors.FieldDescriptor bytesField(Descriptors.Descriptor requestType,
                                                  String name) {
        List<Descriptors.FieldDescriptor> candidates = new ArrayList<>();
        for (Descriptors.FieldDescriptor field : requestType.getFields()) {
            if (field.getType() == Descriptors.FieldDescriptor.Type.BYTES
                    && !field.isRepeated()) {
                candidates.add(field);
            }
        }
        if (name != null && !name.isBlank()) {
            for (Descriptors.FieldDescriptor field : candidates) {
                if (field.getName().equals(name)) {
                    return field;
                }
            }
            throw new IllegalArgumentException(requestType.getFullName()
                    + " has no single bytes field named '" + name + "'; bytes fields: "
                    + names(candidates));
        }
        if (candidates.size() != 1) {
            throw new IllegalArgumentException(requestType.getFullName() + " has "
                    + candidates.size() + " bytes fields " + names(candidates)
                    + "; pick one with --bytes-field");
        }
        return candidates.get(0);
    }

    private static List<String> names(List<Descriptors.FieldDescriptor> fields) {
        return fields.stream().map(Descriptors.FieldDescriptor::getName).toList();
    }

    /**
     * Fetches the file that defines {@code symbol}, and its dependencies, by reflection.
     * grpc-java sends the dependencies with the file; tonic (Rust) sends only the file
     * asked for, so any dependency still missing is then fetched by name.
     */
    private static FileDescriptor fetchFile(Channel channel, String symbol, long timeoutMs)
            throws IOException, InterruptedException {
        Map<String, FileDescriptorProto> protos = new HashMap<>();
        List<FileDescriptorProto> first = request(channel, ServerReflectionRequest.newBuilder()
                .setFileContainingSymbol(symbol).build(), timeoutMs);
        if (first.isEmpty()) {
            throw new IOException("reflection returned no file for " + symbol);
        }
        for (FileDescriptorProto proto : first) {
            protos.putIfAbsent(proto.getName(), proto);
        }
        List<String> missing = missingDependencies(protos);
        while (!missing.isEmpty()) {
            for (String name : missing) {
                List<FileDescriptorProto> files = request(channel, ServerReflectionRequest
                        .newBuilder().setFileByFilename(name).build(), timeoutMs);
                for (FileDescriptorProto proto : files) {
                    protos.putIfAbsent(proto.getName(), proto);
                }
                if (!protos.containsKey(name)) {
                    throw new IOException("reflection did not send dependency " + name);
                }
            }
            missing = missingDependencies(protos);
        }
        try {
            return build(first.get(0).getName(), protos, new HashMap<>());
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IOException("invalid descriptor from reflection", e);
        }
    }

    private static List<String> missingDependencies(Map<String, FileDescriptorProto> protos) {
        List<String> missing = new ArrayList<>();
        for (FileDescriptorProto proto : protos.values()) {
            for (String dependency : proto.getDependencyList()) {
                if (!protos.containsKey(dependency) && !missing.contains(dependency)) {
                    missing.add(dependency);
                }
            }
        }
        return missing;
    }

    /** Sends one reflection request on its own stream and returns the files it answers with. */
    private static List<FileDescriptorProto> request(Channel channel,
                                                     ServerReflectionRequest request,
                                                     long timeoutMs)
            throws IOException, InterruptedException {
        List<FileDescriptorProto> files = new ArrayList<>();
        CompletableFuture<Void> done = new CompletableFuture<>();
        StreamObserver<ServerReflectionRequest> requests = ServerReflectionGrpc
                .newStub(channel)
                .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                .serverReflectionInfo(new StreamObserver<>() {
                    @Override
                    public void onNext(ServerReflectionResponse response) {
                        if (response.hasErrorResponse()) {
                            done.completeExceptionally(new IOException("reflection: "
                                    + response.getErrorResponse().getErrorMessage()));
                            return;
                        }
                        try {
                            for (ByteString bytes : response.getFileDescriptorResponse()
                                    .getFileDescriptorProtoList()) {
                                files.add(FileDescriptorProto.parseFrom(bytes));
                            }
                        } catch (InvalidProtocolBufferException e) {
                            done.completeExceptionally(e);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        done.completeExceptionally(t);
                    }

                    @Override
                    public void onCompleted() {
                        done.complete(null);
                    }
                });
        requests.onNext(request);
        requests.onCompleted();
        try {
            done.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new IOException("reflection request failed: " + request, e.getCause());
        } catch (TimeoutException e) {
            throw new IOException("reflection request timed out: " + request, e);
        }
        return files;
    }

    private static FileDescriptor build(String name, Map<String, FileDescriptorProto> protos,
                                        Map<String, FileDescriptor> built)
            throws Descriptors.DescriptorValidationException, IOException {
        FileDescriptor existing = built.get(name);
        if (existing != null) {
            return existing;
        }
        FileDescriptorProto proto = protos.get(name);
        if (proto == null) {
            throw new IOException("reflection did not send dependency " + name);
        }
        List<FileDescriptor> deps = new ArrayList<>();
        for (String dependency : proto.getDependencyList()) {
            deps.add(build(dependency, protos, built));
        }
        FileDescriptor file = FileDescriptor.buildFrom(proto, deps.toArray(new FileDescriptor[0]));
        built.put(name, file);
        return file;
    }

    private record DynamicMarshaller(Descriptors.Descriptor type)
            implements Marshaller<DynamicMessage> {
        @Override
        public InputStream stream(DynamicMessage value) {
            return new ByteArrayInputStream(value.toByteArray());
        }

        @Override
        public DynamicMessage parse(InputStream stream) {
            try {
                return DynamicMessage.parseFrom(type, stream);
            } catch (IOException e) {
                throw io.grpc.Status.INTERNAL.withDescription("bad reply: " + e.getMessage())
                        .withCause(e).asRuntimeException();
            }
        }
    }
}
