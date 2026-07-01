/*
 * Reference example for document-proposal.proto (design draft, not yet wired to a module).
 */
package org.apache.tika.grpc.transform;

import java.util.List;

import org.apache.tika.grpc.v1.Document;
import org.apache.tika.metadata.Metadata;

/**
 * Picks and runs the applicable transformer(s) for a parsed document. Multiple may
 * apply; the generic fallback runs when none do, so an unknown format still yields a
 * useful, lossless Document.
 *
 * Adding support for a new format means adding a transformer to this list. The proto
 * does not change, so clients never rebuild for it.
 */
public final class DocumentTransformers {

    private final List<DocumentTransformer> transformers;
    private final DocumentTransformer fallback = new GenericDocumentTransformer();

    public DocumentTransformers(List<DocumentTransformer> transformers) {
        this.transformers = transformers;
    }

    public static DocumentTransformers defaults() {
        return new DocumentTransformers(List.of(
                new PdfDocumentTransformer()
                // new OfficeDocumentTransformer(),
                // new ImageDocumentTransformer(), ...
        ));
    }

    public Document transform(Metadata tika) {
        Document.Builder document = Document.newBuilder();
        String contentType = tika.get(Metadata.CONTENT_TYPE);
        if (contentType != null) {
            document.setContentType(contentType);
        }

        boolean matched = false;
        for (DocumentTransformer transformer : transformers) {
            if (transformer.appliesTo(contentType)) {
                transformer.transform(tika, document);
                matched = true;
            }
        }
        if (!matched) {
            fallback.transform(tika, document);
        }
        return document.build();
    }
}
