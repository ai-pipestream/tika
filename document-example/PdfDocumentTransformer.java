/*
 * Reference example for document-proposal.proto (design draft, not yet wired to a module).
 */
package org.apache.tika.grpc.transform;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.apache.tika.grpc.v1.Document;
import org.apache.tika.grpc.v1.DocumentMetadata;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Office;
import org.apache.tika.metadata.PagedText;
import org.apache.tika.metadata.TikaCoreProperties;

/**
 * Real example: PDF. Maps the common, cross-format facts into typed DocumentMetadata.
 *
 * PDF-specific properties (encryption flags, XMP, permissions, incremental updates, ...)
 * are NOT given their own proto fields. They flow into the tagged tail, typed where Tika
 * declares the type and string otherwise. That is the whole point: format richness lives
 * in code plus the tail, not in the wire contract - so the schema stays small and stable
 * and clients never rebuild when we add or change a PDF property.
 */
public final class PdfDocumentTransformer implements DocumentTransformer {

    @Override
    public boolean appliesTo(String contentType) {
        return contentType != null && contentType.toLowerCase(Locale.ROOT).contains("pdf");
    }

    @Override
    public void transform(Metadata tika, Document.Builder document) {
        Set<String> consumed = new HashSet<>();
        DocumentMetadata.Builder meta = document.getMetadataBuilder();

        // Common, cross-format fields -> typed (a date is a Timestamp, a count is an int).
        TransformSupport.setString(tika, TikaCoreProperties.TITLE, meta::setTitle, consumed);
        TransformSupport.setString(tika, TikaCoreProperties.DESCRIPTION, meta::setDescription, consumed);
        TransformSupport.addStrings(tika, TikaCoreProperties.CREATOR, meta::addAllAuthors, consumed);
        TransformSupport.addStrings(tika, TikaCoreProperties.SUBJECT, meta::addAllKeywords, consumed);
        TransformSupport.addStrings(tika, TikaCoreProperties.LANGUAGE, meta::addAllLanguages, consumed);
        TransformSupport.setTimestamp(tika, TikaCoreProperties.CREATED, meta::setCreated, consumed);
        TransformSupport.setTimestamp(tika, TikaCoreProperties.MODIFIED, meta::setModified, consumed);
        TransformSupport.setInt(tika, PagedText.N_PAGES, meta::setPageCount, consumed);
        TransformSupport.setInt(tika, Office.WORD_COUNT, n -> meta.setWordCount(n.longValue()), consumed);

        // Everything PDF-specific (pdf:*, xmpPDF:*, access_permission:*, ...) lands here,
        // typed where Tika declares the type. No proto-per-format.
        MetadataTagger.appendTail(tika, consumed, document);
    }
}
