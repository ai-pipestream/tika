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

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.tika.grpc.messages.v2alpha1.MessageParserServiceGrpc;
import org.apache.tika.grpc.messages.v2alpha1.ParseMessageBidiRequest;
import org.apache.tika.grpc.messages.v2alpha1.ParseMessageBidiResponse;
import org.apache.tika.grpc.messages.v2alpha1.ParseMessageRequest;
import org.apache.tika.grpc.messages.v2alpha1.ParseMessageResponse;
import org.apache.tika.grpc.messages.v2alpha1.ParseMessageStreamRequest;
import org.apache.tika.grpc.messages.v2alpha1.ParseMessageStreamResponse;
import org.apache.tika.grpc.messages.v2alpha1.ParsedDocument;
import org.apache.tika.grpc.messages.v2alpha1.RequestError;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.metadata.schema.MetadataKeyValidator;

/**
 * Parses protobuf messages with the installed {@link org.apache.tika.grpc.spi.MessageParser}s and
 * returns them in the shape /rmeta returns, with metadata as a {@link Struct}.
 */
class MessageParserServiceImpl extends MessageParserServiceGrpc.MessageParserServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(MessageParserServiceImpl.class);
    private static final String CONTENT_KEY = TikaCoreProperties.TIKA_CONTENT.getName();
    // integers past this lose precision as a double
    private static final long MAX_EXACT_DOUBLE = 1L << 53;

    private final MessageParsers parsers;
    private final MetadataKeyValidator registry;

    MessageParserServiceImpl(MessageParsers parsers) {
        this.parsers = parsers;
        this.registry = MetadataKeyValidator.fromClasspath();
    }

    @Override
    public void parseMessage(ParseMessageRequest request,
                             StreamObserver<ParseMessageResponse> responseObserver) {
        try {
            responseObserver.onNext(parse(request.getMessage()));
            responseObserver.onCompleted();
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void parseMessageStream(ParseMessageStreamRequest request,
                                   StreamObserver<ParseMessageStreamResponse> responseObserver) {
        try {
            for (ParsedDocument document : parse(request.getMessage()).getDocumentsList()) {
                responseObserver.onNext(
                        ParseMessageStreamResponse.newBuilder().setDocument(document).build());
            }
            responseObserver.onCompleted();
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public StreamObserver<ParseMessageBidiRequest> parseMessageBidi(
            StreamObserver<ParseMessageBidiResponse> responseObserver) {
        return new StreamObserver<>() {
            @Override
            public void onNext(ParseMessageBidiRequest request) {
                ParseMessageBidiResponse.Builder response =
                        ParseMessageBidiResponse.newBuilder().setRequestId(request.getRequestId());
                try {
                    response.setResult(parse(request.getMessage()));
                } catch (StatusException e) {
                    response.setError(RequestError.newBuilder()
                            .setCode(e.getStatus().getCode().value())
                            .setMessage(StringUtils.defaultString(e.getStatus().getDescription())));
                }
                responseObserver.onNext(response.build());
            }

            @Override
            public void onError(Throwable throwable) {
                LOG.debug("message parser bidi stream ended by the client", throwable);
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    private ParseMessageResponse parse(Any message) throws StatusException {
        if (message.getTypeUrl().isEmpty()) {
            throw Status.INVALID_ARGUMENT.withDescription("message is required").asException();
        }
        ParseMessageResponse.Builder response = ParseMessageResponse.newBuilder();
        for (Metadata metadata : parsers.parse(message)) {
            response.addDocuments(toDocument(metadata));
        }
        return response.build();
    }

    ParsedDocument toDocument(Metadata metadata) {
        ParsedDocument.Builder document = ParsedDocument.newBuilder();
        String path = metadata.get(TikaCoreProperties.EMBEDDED_ID_PATH);
        if (path != null) {
            document.setEmbeddedPath(path);
        }
        String content = metadata.get(CONTENT_KEY);
        if (content != null) {
            document.setContent(content);
        }
        Struct.Builder fields = Struct.newBuilder();
        for (String name : metadata.names()) {
            if (name.equals(CONTENT_KEY)) {
                continue;
            }
            fields.putFields(name, toValue(metadata.getValues(name), typeOf(name), cardinalityOf(name)));
        }
        return document.setMetadata(fields).build();
    }

    /** One key's values: a list when there is more than one or the key is multi-valued. */
    static Value toValue(String[] values, String type, String cardinality) {
        boolean multiValued = "BAG".equals(cardinality) || "SEQ".equals(cardinality)
                || "ALT".equals(cardinality);
        if (values.length == 1 && !multiValued) {
            return toScalar(values[0], type, true);
        }
        ListValue.Builder list = ListValue.newBuilder();
        for (String value : values) {
            list.addValues(toScalar(value, type, false));
        }
        return Value.newBuilder().setListValue(list).build();
    }

    // a value that does not fit its type stays Tika's string, so nothing is lost
    private static Value toScalar(String value, String type, boolean single) {
        if ("BOOLEAN".equals(type) && (value.equals("true") || value.equals("false"))) {
            return Value.newBuilder().setBoolValue(Boolean.parseBoolean(value)).build();
        }
        if ("INTEGER".equals(type)) {
            try {
                long n = Long.parseLong(value.trim());
                if (Math.abs(n) <= MAX_EXACT_DOUBLE) {
                    return Value.newBuilder().setNumberValue(n).build();
                }
            } catch (NumberFormatException e) {
                // a string
            }
        }
        if ("REAL".equals(type)) {
            try {
                double d = Double.parseDouble(value.trim());
                if (Double.isFinite(d)) {
                    return Value.newBuilder().setNumberValue(d).build();
                }
            } catch (NumberFormatException e) {
                // a string
            }
        }
        String trimmed = value.trim();
        // only a single object or array: a JSON scalar would read as an ordinary value
        if ("JSON".equals(type) && single && (trimmed.startsWith("{") || trimmed.startsWith("["))) {
            try {
                Value.Builder json = Value.newBuilder();
                JsonFormat.parser().merge(trimmed, json);
                return json.build();
            } catch (InvalidProtocolBufferException | RuntimeException e) {
                // a string
            }
        }
        return Value.newBuilder().setStringValue(value).build();
    }

    // the registry first, then the dictionary of the message parsers
    private String typeOf(String name) {
        String registered = registry.valueTypeOf(name);
        if (registered != null) {
            return registered;
        }
        Property property = parsers.property(name);
        return property == null || property.getValueType() == null ? null
                : property.getValueType().name();
    }

    private String cardinalityOf(String name) {
        if (registry.valueTypeOf(name) != null) {
            return registry.cardinalityOf(name);
        }
        Property property = parsers.property(name);
        return property == null || property.getPropertyType() == null ? null
                : property.getPropertyType().name();
    }
}
