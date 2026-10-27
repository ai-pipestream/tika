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
package org.apache.tika.grpc.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import org.junit.jupiter.api.Test;

import org.apache.tika.grpc.v2.Document;
import org.apache.tika.grpc.v2.DocumentMetadata;
import org.apache.tika.grpc.v2.ExtensionResult;
import org.apache.tika.grpc.v2.SourceOrigin;

class DocumentComposerTest {

    private static final Document CORE = Document.newBuilder()
            .setId("tika-id")
            .setContentType("application/warc")
            .setOrigin(SourceOrigin.newBuilder().setSha256("ABCDEF"))
            .setMetadata(DocumentMetadata.newBuilder().setTitle("t"))
            .build();

    private static ExtensionResult ok(String producer, String payload) {
        return ExtensionResult.newBuilder()
                .setProducerId(producer)
                .setStatus(ExtensionResult.Status.STATUS_OK)
                .setPayload(Any.pack(StringValue.of(payload)))
                .build();
    }

    @Test
    void appendsContributionsSortedBySlotAndLeavesTheCoreAlone() {
        Document composed = DocumentComposer.compose(CORE, "work-1", "abcdef",
                List.of(ok("fastwarc", "w"), ok("calamine", "c")),
                List.of("calamine", "fastwarc"));

        assertEquals("work-1", composed.getId());
        assertEquals(List.of("calamine", "fastwarc"), composed.getExtensionsList().stream()
                .map(ExtensionResult::getProducerId).toList());
        assertEquals(CORE.toBuilder().setId("work-1").build(),
                composed.toBuilder().clearExtensions().build(), "core fields unchanged");
    }

    @Test
    void refusesTheMergeWhenTheDigestsDiffer() {
        assertThrows(IllegalArgumentException.class, () -> DocumentComposer.compose(CORE,
                "w", "012345", List.of(ok("fastwarc", "w")), List.of("fastwarc")));
    }

    @Test
    void identicalContributionsCollapseAndDifferentOnesFailTheSlot() {
        Document composed = DocumentComposer.compose(CORE, null, null,
                List.of(ok("a", "same"), ok("a", "same"), ok("b", "one"), ok("b", "two")),
                List.of("a", "b"));

        assertEquals("tika-id", composed.getId());
        assertEquals(ok("a", "same"), composed.getExtensions(0));
        ExtensionResult conflict = composed.getExtensions(1);
        assertEquals("b", conflict.getProducerId());
        assertEquals(ExtensionResult.Status.STATUS_FAILED, conflict.getStatus());
        assertEquals(false, conflict.hasPayload(), "neither conflicting payload is kept");
    }

    @Test
    void conflictResultDoesNotDependOnArrivalOrder() {
        assertEquals(
                DocumentComposer.compose(CORE, "w", null,
                        List.of(ok("b", "one"), ok("b", "two")), List.of("b")),
                DocumentComposer.compose(CORE, "w", null,
                        List.of(ok("b", "two"), ok("b", "one")), List.of("b")));
    }

    @Test
    void aPlannedSlotWithNoContributionIsFailed() {
        Document composed = DocumentComposer.compose(CORE, "w", null, List.of(),
                List.of("fastwarc"));

        assertEquals(ExtensionResult.Status.STATUS_FAILED,
                composed.getExtensions(0).getStatus());
        assertEquals(List.of("no contribution"), composed.getExtensions(0).getWarningsList());
    }

    @Test
    void anEnvelopeWithoutAProducerIdIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> DocumentComposer.compose(CORE, "w",
                null, List.of(ok("", "x")), List.of()));
    }
}
