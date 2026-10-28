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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import com.google.protobuf.Any;
import com.google.protobuf.Internal;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.StatusException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.tika.exception.TikaException;
import org.apache.tika.grpc.spi.MessageParser;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;

/** The installed {@link MessageParser}s, by the type URL of the message each one reads. */
class MessageParsers {

    private static final Logger LOG = LoggerFactory.getLogger(MessageParsers.class);
    private static final String TYPE_URL_PREFIX = "type.googleapis.com/";

    private final Map<String, MessageParser<?>> byTypeUrl;
    private final Map<String, Property> dictionary;

    MessageParsers(Iterable<MessageParser<?>> parsers) {
        Map<String, MessageParser<?>> parsersByType = new HashMap<>();
        Map<String, Property> properties = new HashMap<>();
        for (MessageParser<?> parser : parsers) {
            String typeUrl = typeUrl(parser.messageType());
            MessageParser<?> previous = parsersByType.putIfAbsent(typeUrl, parser);
            if (previous != null) {
                throw new IllegalStateException("both " + previous.getClass().getName() + " and "
                        + parser.getClass().getName() + " parse " + typeUrl);
            }
            for (Property property : parser.properties()) {
                properties.putIfAbsent(property.getName(), property);
            }
            LOG.info("message parser {} parses {}", parser.getClass().getName(), typeUrl);
        }
        this.byTypeUrl = Collections.unmodifiableMap(parsersByType);
        this.dictionary = Collections.unmodifiableMap(properties);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    static MessageParsers fromClasspath() {
        return new MessageParsers((Iterable) ServiceLoader.load(MessageParser.class));
    }

    /** A key a parser declared, or {@code null}. */
    Property property(String name) {
        return dictionary.get(name);
    }

    /** Unpacks the message and parses it with the parser for its type. */
    List<Metadata> parse(Any message) throws StatusException {
        MessageParser<?> parser = byTypeUrl.get(stripHost(message.getTypeUrl()));
        if (parser == null) {
            throw Status.NOT_FOUND.withDescription("no message parser for '"
                    + StringUtils.abbreviate(message.getTypeUrl(), 200) + "'").asException();
        }
        try {
            return unpackAndParse(parser, message);
        } catch (InvalidProtocolBufferException e) {
            throw Status.INVALID_ARGUMENT.withDescription("message does not parse as "
                    + parser.messageType().getName()).asException();
        } catch (TikaException e) {
            throw Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException();
        } catch (RuntimeException e) {
            LOG.warn("message parser {} failed", parser.getClass().getName(), e);
            throw Status.INTERNAL.withDescription("message parser failed").asException();
        }
    }

    private static <T extends Message> List<Metadata> unpackAndParse(MessageParser<T> parser,
                                                                     Any message)
            throws InvalidProtocolBufferException, TikaException {
        List<Metadata> documents = parser.parse(message.unpack(parser.messageType()));
        return documents == null ? List.of() : documents;
    }

    private static String typeUrl(Class<? extends Message> type) {
        return TYPE_URL_PREFIX
                + Internal.getDefaultInstance(type).getDescriptorForType().getFullName();
    }

    // Any allows any host in front of the full name
    private static String stripHost(String typeUrl) {
        return TYPE_URL_PREFIX + typeUrl.substring(typeUrl.lastIndexOf('/') + 1);
    }
}
