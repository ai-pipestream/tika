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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.asarkar.grpc.test.GrpcCleanupExtension;
import com.asarkar.grpc.test.Resources;
import com.google.protobuf.Any;
import com.google.protobuf.ListValue;
import com.google.protobuf.StringValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.apache.tika.grpc.messages.v2alpha1.MessageParserServiceGrpc;
import org.apache.tika.grpc.messages.v2alpha1.ParseMessageBidiRequest;
import org.apache.tika.grpc.messages.v2alpha1.ParseMessageBidiResponse;
import org.apache.tika.grpc.messages.v2alpha1.ParseMessageRequest;
import org.apache.tika.grpc.messages.v2alpha1.ParseMessageResponse;
import org.apache.tika.grpc.messages.v2alpha1.ParseMessageStreamRequest;
import org.apache.tika.grpc.messages.v2alpha1.ParsedDocument;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;

@ExtendWith(GrpcCleanupExtension.class)
public class ParseMessageTest {

    private static ManagedChannel start(Resources resources) throws Exception {
        String name = InProcessServerBuilder.generateName();
        // the parser comes from META-INF/services in the test resources, as a plugin jar's would
        Server server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new MessageParserServiceImpl(MessageParsers.fromClasspath()))
                .build().start();
        resources.register(server, Duration.ofSeconds(10));
        ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        resources.register(channel, Duration.ofSeconds(10));
        return channel;
    }

    private static Any struct(String title, String... records) {
        ListValue.Builder list = ListValue.newBuilder();
        for (String record : records) {
            list.addValues(Value.newBuilder().setStringValue(record));
        }
        return Any.pack(Struct.newBuilder()
                .putFields("title", Value.newBuilder().setStringValue(title).build())
                .putFields("count", Value.newBuilder().setNumberValue(records.length).build())
                .putFields("records", Value.newBuilder().setListValue(list).build())
                .build());
    }

    private static Any flag(String flag) {
        return Any.pack(Struct.newBuilder()
                .putFields(flag, Value.newBuilder().setBoolValue(true).build()).build());
    }

    @Test
    public void unaryReturnsTheContainerThenEachRecord(Resources resources) throws Exception {
        ParseMessageResponse response = MessageParserServiceGrpc.newBlockingStub(start(resources))
                .parseMessage(ParseMessageRequest.newBuilder()
                        .setMessage(struct("a title", "one", "two")).build());

        assertEquals(3, response.getDocumentsCount());
        ParsedDocument container = response.getDocuments(0);
        assertEquals("", container.getEmbeddedPath());
        Struct metadata = container.getMetadata();
        // dc:title is declared multi-valued (BAG), so it is a list even with one value
        assertEquals(List.of(string("a title")),
                metadata.getFieldsOrThrow("dc:title").getListValue().getValuesList());
        assertEquals("/0", response.getDocuments(1).getEmbeddedPath());
        assertEquals("one", response.getDocuments(1).getContent());
        assertFalse(response.getDocuments(1).getMetadata().containsFields("tk:content"));
        assertEquals("two", response.getDocuments(2).getContent());

        // keys the registry does not know take their hints from the parser's dictionary
        // keys the registry does not know are typed from the parser's properties()
        assertEquals(2.0, metadata.getFieldsOrThrow("test:count").getNumberValue());
        assertEquals(List.of(string("a"), string("b")),
                metadata.getFieldsOrThrow("test:tags").getListValue().getValuesList());
    }

    @Test
    public void streamSendsTheSameDocumentsOnePerMessage(Resources resources) throws Exception {
        ManagedChannel channel = start(resources);
        Any message = struct("streamed", "one", "two", "three");
        List<ParsedDocument> unary = MessageParserServiceGrpc.newBlockingStub(channel)
                .parseMessage(ParseMessageRequest.newBuilder().setMessage(message).build())
                .getDocumentsList();
        List<ParsedDocument> streamed = new ArrayList<>();
        MessageParserServiceGrpc.newBlockingStub(channel)
                .parseMessageStream(ParseMessageStreamRequest.newBuilder().setMessage(message).build())
                .forEachRemaining(r -> streamed.add(r.getDocument()));
        assertEquals(unary, streamed);
    }

    @Test
    public void failuresAreStatusCodes(Resources resources) throws Exception {
        MessageParserServiceGrpc.MessageParserServiceBlockingStub stub = MessageParserServiceGrpc.newBlockingStub(start(resources));

        assertEquals(Status.Code.NOT_FOUND, codeOf(stub, Any.pack(StringValue.of("no parser"))));
        assertEquals(Status.Code.INVALID_ARGUMENT, codeOf(stub, Any.getDefaultInstance()));
        assertEquals(Status.Code.INVALID_ARGUMENT, codeOf(stub, flag("fail")));
        assertEquals(Status.Code.INTERNAL, codeOf(stub, flag("crash")));
        // the right type URL with bytes that are not a Struct
        Any corrupt = Any.newBuilder().setTypeUrl("type.googleapis.com/google.protobuf.Struct")
                .setValue(com.google.protobuf.ByteString.copyFrom(new byte[]{(byte) 0xff})).build();
        assertEquals(Status.Code.INVALID_ARGUMENT, codeOf(stub, corrupt));
    }

    private static Status.Code codeOf(MessageParserServiceGrpc.MessageParserServiceBlockingStub stub, Any message) {
        return assertThrows(StatusRuntimeException.class, () -> stub.parseMessage(
                ParseMessageRequest.newBuilder().setMessage(message).build())).getStatus().getCode();
    }

    @Test
    public void bidiAnswersEachRequestAndKeepsGoingAfterAFailure(Resources resources) throws Exception {
        List<ParseMessageBidiResponse> responses = new ArrayList<>();
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<ParseMessageBidiRequest> requests = MessageParserServiceGrpc.newStub(start(resources))
                .parseMessageBidi(new StreamObserver<>() {
                    @Override
                    public void onNext(ParseMessageBidiResponse response) {
                        responses.add(response);
                    }

                    @Override
                    public void onError(Throwable t) {
                        done.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        done.countDown();
                    }
                });
        requests.onNext(ParseMessageBidiRequest.newBuilder().setRequestId("1")
                .setMessage(struct("first", "x")).build());
        requests.onNext(ParseMessageBidiRequest.newBuilder().setRequestId("2")
                .setMessage(flag("fail")).build());
        requests.onNext(ParseMessageBidiRequest.newBuilder().setRequestId("3")
                .setMessage(struct("third")).build());
        requests.onCompleted();
        assertTrue(done.await(10, TimeUnit.SECONDS));

        assertEquals(3, responses.size());
        assertEquals("1", responses.get(0).getRequestId());
        assertEquals(2, responses.get(0).getResult().getDocumentsCount());
        assertEquals("2", responses.get(1).getRequestId());
        assertEquals(Status.Code.INVALID_ARGUMENT.value(), responses.get(1).getError().getCode());
        assertFalse(responses.get(1).hasResult());
        assertEquals("third", responses.get(2).getResult().getDocuments(0)
                .getMetadata().getFieldsOrThrow("dc:title").getListValue().getValues(0).getStringValue());
    }

    @Test
    public void twoParsersForOneTypeIsAStartupError() {
        assertThrows(IllegalStateException.class,
                () -> new MessageParsers(List.of(new StructMessageParser(), new StructMessageParser())));
    }

    @Test
    public void valuesAreTypedAndJsonIsNested() {
        Metadata metadata = new Metadata();
        metadata.add("pdf:chars-per-page", "10");
        metadata.add("pdf:chars-per-page", "20");
        metadata.set("pdf:encrypted", "false");
        metadata.set("x-not-registered", "anything");
        metadata.set(TikaCoreProperties.TIKA_CHUNKS, "[{\"text\":\"hi\",\"start\":0}]");
        Struct fields = new MessageParserServiceImpl(new MessageParsers(List.of()))
                .toDocument(metadata).getMetadata();

        assertEquals(List.of(number(10), number(20)),
                fields.getFieldsOrThrow("pdf:chars-per-page").getListValue().getValuesList());
        assertEquals(Value.newBuilder().setBoolValue(false).build(), fields.getFieldsOrThrow("pdf:encrypted"));
        assertEquals(string("anything"), fields.getFieldsOrThrow("x-not-registered"));
        assertEquals("hi", fields.getFieldsOrThrow("tk:chunks").getListValue().getValues(0)
                .getStructValue().getFieldsOrThrow("text").getStringValue());
        // a value that does not fit its type stays a string
        assertEquals(string("not a number"),
                MessageParserServiceImpl.toValue(new String[]{"not a number"}, "INTEGER", null));
        // a double cannot hold this integer exactly
        assertEquals(string("9007199254740993"),
                MessageParserServiceImpl.toValue(new String[]{"9007199254740993"}, "INTEGER", null));
    }

    private static Value string(String s) {
        return Value.newBuilder().setStringValue(s).build();
    }

    private static Value number(double n) {
        return Value.newBuilder().setNumberValue(n).build();
    }
}
