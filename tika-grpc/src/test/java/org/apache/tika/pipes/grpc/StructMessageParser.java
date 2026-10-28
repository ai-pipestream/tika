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

import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;

import org.apache.tika.exception.TikaException;
import org.apache.tika.grpc.spi.MessageParser;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.metadata.TikaCoreProperties;

/**
 * A test parser over a well-known type: "title" and "count" on the container, each string in
 * "records" as an embedded document, and "fail" to fail the parse.
 */
public class StructMessageParser implements MessageParser<Struct> {

    static final Property COUNT = Property.externalInteger("test:count");
    static final Property TAGS = Property.externalTextBag("test:tags");

    @Override
    public Class<Struct> messageType() {
        return Struct.class;
    }

    @Override
    public List<Property> properties() {
        return List.of(COUNT, TAGS);
    }

    @Override
    public List<Metadata> parse(Struct message) throws TikaException {
        if (message.containsFields("fail")) {
            throw new TikaException("asked to fail");
        }
        if (message.containsFields("crash")) {
            throw new IllegalStateException("asked to crash");
        }
        List<Metadata> documents = new ArrayList<>();
        Metadata container = new Metadata();
        container.set(TikaCoreProperties.TITLE,
                message.getFieldsOrDefault("title", Value.getDefaultInstance()).getStringValue());
        container.set(COUNT,
                (int) message.getFieldsOrDefault("count", Value.getDefaultInstance()).getNumberValue());
        container.add(TAGS, "a");
        container.add(TAGS, "b");
        documents.add(container);
        List<Value> records =
                message.getFieldsOrDefault("records", Value.getDefaultInstance()).getListValue()
                        .getValuesList();
        for (int i = 0; i < records.size(); i++) {
            Metadata record = new Metadata();
            record.set(TikaCoreProperties.EMBEDDED_ID_PATH, "/" + i);
            record.set(TikaCoreProperties.TIKA_CONTENT, records.get(i).getStringValue());
            documents.add(record);
        }
        return documents;
    }
}
