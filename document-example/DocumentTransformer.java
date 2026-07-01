/*
 * Reference example for document-proposal.proto (design draft, not yet wired to a module).
 */
package org.apache.tika.grpc.transform;

import org.apache.tika.grpc.v1.Document;
import org.apache.tika.metadata.Metadata;

/**
 * Maps one Tika {@link Metadata} (a document, or an embedded part) onto the typed
 * {@link Document}. Transformers are code, not schema: adding a parser adds a
 * transformer, and the wire contract never changes. More than one transformer may
 * apply to a single document (a format transformer plus cross-cutting ones).
 */
public interface DocumentTransformer {

    /** True if this transformer handles the given Tika Content-Type (may be null). */
    boolean appliesTo(String contentType);

    /** Populate typed fields and/or the tagged tail on the builder. */
    void transform(Metadata tika, Document.Builder document);
}
