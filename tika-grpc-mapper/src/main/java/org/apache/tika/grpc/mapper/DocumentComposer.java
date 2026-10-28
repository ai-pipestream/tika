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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import org.apache.tika.grpc.v2.Document;
import org.apache.tika.grpc.v2.ExtensionResult;

/**
 * Appends external parsers' results to Tika's {@link Document}, following the
 * TIKA-4771 composition rules:
 * <ol>
 *     <li>The core Document (everything but {@code id} and {@code extensions}) is never
 *     changed.</li>
 *     <li>The composer's work-item id becomes the Document id.</li>
 *     <li>When Tika recorded {@code origin.sha256} and the composer hashed its input, the
 *     two must match, or the merge is refused.</li>
 *     <li>Contributions to one slot that carry the same status and payload are the same
 *     result, whatever their {@code elapsed_ms} and {@code warnings} say (a retry, or a
 *     producer called again downstream): one is kept, the fastest. Otherwise the best status
 *     wins, {@code STATUS_OK} over {@code STATUS_EMPTY} over {@code STATUS_SKIPPED} over
 *     {@code STATUS_FAILED}, so a successful retry replaces a failure. Two different
 *     payloads with the same best status are a conflict: neither is kept, and the slot gets a
 *     {@code STATUS_FAILED} envelope.</li>
 *     <li>One envelope per slot.</li>
 *     <li>A planned slot with no contribution gets a {@code STATUS_FAILED} envelope, so the
 *     result accounts for every slot the composer planned.</li>
 * </ol>
 * Envelopes come out sorted by {@code producer_id}, so the result does not depend on the
 * order contributions arrived in.
 */
public final class DocumentComposer {

    private DocumentComposer() {
    }

    /**
     * @param core          Tika's Document for the bytes; extensions it already carries
     *                      count as contributions
     * @param workItemId    the composer's id for this work item; blank keeps the core id
     * @param inputSha256   hex SHA-256 of the bytes the composer sent, or {@code null} to
     *                      skip the check
     * @param contributions envelopes the producers returned, in any order
     * @param plannedSlots  every producer id the composer planned to call
     * @throws IllegalArgumentException if the digests differ or an envelope has no
     *                                  producer id
     */
    public static Document compose(Document core, String workItemId, String inputSha256,
                                   Collection<ExtensionResult> contributions,
                                   Collection<String> plannedSlots) {
        String tikaSha256 = core.getOrigin().getSha256();
        if (inputSha256 != null && !inputSha256.isBlank() && !tikaSha256.isBlank()
                && !tikaSha256.trim().toLowerCase(Locale.ROOT)
                        .equals(inputSha256.trim().toLowerCase(Locale.ROOT))) {
            throw new IllegalArgumentException("Tika parsed different bytes: origin.sha256 is "
                    + tikaSha256 + ", the composer sent " + inputSha256);
        }

        Map<String, List<ExtensionResult>> bySlot = new TreeMap<>();
        List<ExtensionResult> all = new ArrayList<>(core.getExtensionsList());
        all.addAll(contributions);
        for (ExtensionResult result : all) {
            if (result.getProducerId().isBlank()) {
                throw new IllegalArgumentException("an extension result has no producer_id");
            }
            bySlot.computeIfAbsent(result.getProducerId(), k -> new ArrayList<>()).add(result);
        }
        for (String planned : plannedSlots) {
            bySlot.computeIfAbsent(planned, k -> new ArrayList<>());
        }

        Document.Builder composed = core.toBuilder().clearExtensions();
        if (workItemId != null && !workItemId.isBlank()) {
            composed.setId(workItemId);
        }
        for (Map.Entry<String, List<ExtensionResult>> slot : bySlot.entrySet()) {
            composed.addExtensions(resolve(slot.getKey(), slot.getValue()));
        }
        return composed.build();
    }

    /** The one envelope a slot keeps, by the precedence in the class comment. */
    private static ExtensionResult resolve(String producerId, List<ExtensionResult> results) {
        if (results.isEmpty()) {
            return failed(producerId, "no contribution");
        }
        int best = results.stream().mapToInt(DocumentComposer::rank).max().getAsInt();
        // Keyed by status and payload only, so a retry with another elapsed_ms or other
        // warnings is the same result. The fastest of each is kept.
        Map<ExtensionResult, ExtensionResult> distinct = new LinkedHashMap<>();
        ExtensionResult.Status bestStatus = null;
        for (ExtensionResult result : results) {
            if (rank(result) != best) {
                continue;
            }
            bestStatus = result.getStatus();
            ExtensionResult key = ExtensionResult.newBuilder()
                    .setStatus(result.getStatus())
                    .setPayload(result.getPayload())
                    .build();
            distinct.merge(key, result, (kept, other) ->
                    other.getElapsedMs() < kept.getElapsedMs() ? other : kept);
        }
        if (distinct.size() == 1) {
            return distinct.values().iterator().next();
        }
        return failed(producerId, distinct.size() + " conflicting " + bestStatus.name()
                + " contributions; none kept");
    }

    private static int rank(ExtensionResult result) {
        return switch (result.getStatus()) {
            case STATUS_OK -> 4;
            case STATUS_EMPTY -> 3;
            case STATUS_SKIPPED -> 2;
            case STATUS_FAILED -> 1;
            default -> 0;
        };
    }

    /** A {@code STATUS_FAILED} envelope the composer writes for {@code producerId}. */
    public static ExtensionResult failed(String producerId, String warning) {
        return ExtensionResult.newBuilder()
                .setProducerId(producerId)
                .setStatus(ExtensionResult.Status.STATUS_FAILED)
                .addWarnings(warning)
                .build();
    }
}
