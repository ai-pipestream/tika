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
package org.apache.tika.grpc.spi;

import java.util.List;

import com.google.protobuf.Message;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;

/**
 * Turns one protobuf message into Tika metadata, the way a Tika parser turns a file into
 * metadata. The message's Java class comes from the plugin's own generated stubs; Tika never
 * sees the .proto.
 * <p>
 * Ship the implementation and the generated stubs in one jar on the tika-grpc classpath, listed
 * in {@code META-INF/services/org.apache.tika.grpc.spi.MessageParser}. tika-grpc serves it
 * through {@code MessageParserService} for any {@code google.protobuf.Any} whose type is
 * {@link #messageType()}.
 *
 * @param <T> the message this parser reads
 */
public interface MessageParser<T extends Message> {

    /** The message this parser reads. One parser per message type. */
    Class<T> messageType();

    /**
     * The dictionary: every key this parser writes, with its type and cardinality. Keys Tika's
     * metadata registry already declares keep the registry's type.
     */
    List<Property> properties();

    /**
     * Maps the message, in the shape {@code /rmeta} returns: [0] is the container, and any
     * embedded documents follow with {@link org.apache.tika.metadata.TikaCoreProperties#EMBEDDED_ID_PATH}
     * set. Extracted text, if any, goes under
     * {@link org.apache.tika.metadata.TikaCoreProperties#TIKA_CONTENT}.
     *
     * @throws TikaException when the message cannot be mapped
     */
    List<Metadata> parse(T message) throws TikaException;
}
