/*
 * Reference example for document-proposal.proto (design draft, not yet wired to a module).
 */
package org.apache.tika.grpc.transform;

import java.util.HashSet;
import java.util.Set;

import org.apache.tika.grpc.v1.Document;
import org.apache.tika.grpc.v1.DocumentMetadata;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;

/**
 * Always-applicable fallback. Pulls the universal Dublin Core fields into typed metadata
 * and routes everything else to the tagged tail, so an unknown or unsupported format
 * still produces a useful, lossless Document.
 */
public final class GenericDocumentTransformer implements DocumentTransformer {

    @Override
    public boolean appliesTo(String contentType) {
        return true;
    }

    @Override
    public void transform(Metadata tika, Document.Builder document) {
        Set<String> consumed = new HashSet<>();
        DocumentMetadata.Builder meta = document.getMetadataBuilder();

        TransformSupport.setString(tika, TikaCoreProperties.TITLE, meta::setTitle, consumed);
        TransformSupport.setString(tika, TikaCoreProperties.DESCRIPTION, meta::setDescription, consumed);
        TransformSupport.addStrings(tika, TikaCoreProperties.CREATOR, meta::addAllAuthors, consumed);
        TransformSupport.addStrings(tika, TikaCoreProperties.SUBJECT, meta::addAllKeywords, consumed);
        TransformSupport.addStrings(tika, TikaCoreProperties.LANGUAGE, meta::addAllLanguages, consumed);
        TransformSupport.setTimestamp(tika, TikaCoreProperties.CREATED, meta::setCreated, consumed);
        TransformSupport.setTimestamp(tika, TikaCoreProperties.MODIFIED, meta::setModified, consumed);

        MetadataTagger.appendTail(tika, consumed, document);
    }
}
