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

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.tika.grpc.mapper.DocumentBuilder;
import org.apache.tika.grpc.v2.Document;
import org.apache.tika.grpc.v2.FetchAndParseReply;
import org.apache.tika.grpc.v2.FetchAndParseRequest;
import org.apache.tika.grpc.v2.ParseBytesReply;
import org.apache.tika.grpc.v2.ParseBytesRequest;
import org.apache.tika.grpc.v2.SourceOrigin;
import org.apache.tika.grpc.v2.TikaV2Grpc;

/**
 * Experimental v2 parse surface. Reuses the same pipes/fetcher runtime as the v1
 * {@link TikaGrpcServerImpl}; fetcher management stays on v1. Replies carry the typed
 * {@link Document} contract instead of the legacy fields map.
 *
 * <p>Also hosts the TIKA-4795 ParseBytes PoC: parse-only entrypoint returning the same
 * {@link Document}, with caller provenance carried on {@code SourceOrigin}.
 */
class TikaGrpcV2ServerImpl extends TikaV2Grpc.TikaV2ImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(TikaGrpcV2ServerImpl.class);

    private final TikaGrpcServerImpl v1;

    TikaGrpcV2ServerImpl(TikaGrpcServerImpl v1) {
        this.v1 = v1;
    }

    @Override
    public void fetchAndParse(FetchAndParseRequest request,
                              StreamObserver<FetchAndParseReply> responseObserver) {
        if (v1.denyPerRequestConfig(request.getAdditionalFetchConfigJson(),
                request.getParseContextJson(), responseObserver)) {
            return;
        }
        fetchAndParseImpl(request, responseObserver);
        responseObserver.onCompleted();
    }

    @Override
    public void fetchAndParseServerSideStreaming(FetchAndParseRequest request,
                                                 StreamObserver<FetchAndParseReply> responseObserver) {
        if (v1.denyPerRequestConfig(request.getAdditionalFetchConfigJson(),
                request.getParseContextJson(), responseObserver)) {
            return;
        }
        fetchAndParseImpl(request, responseObserver);
    }

    @Override
    public StreamObserver<FetchAndParseRequest> fetchAndParseBiDirectionalStreaming(
            StreamObserver<FetchAndParseReply> responseObserver) {
        return new StreamObserver<>() {
            @Override
            public void onNext(FetchAndParseRequest request) {
                if (v1.denyPerRequestConfig(request.getAdditionalFetchConfigJson(),
                        request.getParseContextJson(), responseObserver)) {
                    return;
                }
                fetchAndParseImpl(request, responseObserver);
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
    public void parseBytes(ParseBytesRequest request,
                           StreamObserver<ParseBytesReply> responseObserver) {
        if (request.getContent().isEmpty()) {
            responseObserver.onError(io.grpc.Status.INVALID_ARGUMENT
                    .withDescription("content is required")
                    .asRuntimeException());
            return;
        }
        if (request.getContent().size() > TikaGrpcServerImpl.PARSE_BYTES_MAX_BYTES) {
            responseObserver.onError(io.grpc.Status.RESOURCE_EXHAUSTED
                    .withDescription("content exceeds ParseBytes bound of "
                            + TikaGrpcServerImpl.PARSE_BYTES_MAX_BYTES + " bytes")
                    .asRuntimeException());
            return;
        }
        if (v1.denyPerRequestConfig(null, request.getParseContextJson(), responseObserver)) {
            return;
        }

        // try-with-resources: the outcome owns the spool file and releases it on every
        // exit, including the null case (a null resource is simply not closed).
        try (TikaGrpcServerImpl.ParseBytesOutcome parseBytesOutcome = v1.runParseBytes(
                request.getContent().toByteArray(),
                request.getResourceName(),
                request.getParseContextJson())) {
            if (parseBytesOutcome == null) {
                return;
            }
            Document.Builder document = DocumentBuilder.build(
                            parseBytesOutcome.primary(),
                            request.getCorrelationId(),
                            parseBytesOutcome.status(),
                            parseBytesOutcome.fetchParseTimeMs())
                    .toBuilder();

            SourceOrigin.Builder origin = document.getOriginBuilder();
            if (!request.getResourceName().isBlank()) {
                origin.setFilename(request.getResourceName());
            }
            origin.setByteSize(request.getContent().size());
            if (!request.getSourceUri().isBlank()) {
                origin.setSourceUri(request.getSourceUri());
            }
            if (!request.getEffectiveUri().isBlank()) {
                origin.setEffectiveUri(request.getEffectiveUri());
            }
            if (!request.getBaseUri().isBlank()) {
                origin.setBaseUri(request.getBaseUri());
            }
            origin.setTruncated(request.getTruncated());
            document.setOrigin(origin);

            ParseBytesReply.Builder reply = ParseBytesReply.newBuilder()
                    .setDocument(document);
            if (!request.getCorrelationId().isEmpty()) {
                reply.setCorrelationId(request.getCorrelationId());
            }
            responseObserver.onNext(reply.build());
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            responseObserver.onError(io.grpc.Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void fetchAndParseImpl(FetchAndParseRequest request,
                                   StreamObserver<FetchAndParseReply> responseObserver) {
        TikaGrpcServerImpl.FetchParseOutcome outcome = v1.runFetchAndParse(
                request.getFetcherId(),
                request.getFetchKey(),
                request.getAdditionalFetchConfigJson(),
                request.getParseContextJson());
        if (outcome == null) {
            return;
        }
        responseObserver.onNext(FetchAndParseReply.newBuilder()
                .setFetchKey(outcome.fetchKey())
                .setDocument(DocumentBuilder.build(
                        outcome.primary(),
                        outcome.fetchKey(),
                        outcome.status(),
                        outcome.fetchParseTimeMs()))
                .build());
    }
}
