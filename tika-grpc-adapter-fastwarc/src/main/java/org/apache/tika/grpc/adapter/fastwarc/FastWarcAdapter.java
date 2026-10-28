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
package org.apache.tika.grpc.adapter.fastwarc;

import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.Date;
import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.ByteString;
import io.grpc.MethodDescriptor;

import org.apache.tika.grpc.adapter.AdapterContribution;
import org.apache.tika.grpc.adapter.AdapterRequest;
import org.apache.tika.grpc.adapter.ExternalParserAdapter;
import org.apache.tika.grpc.adapter.fastwarc.v1.HeaderBlock;
import org.apache.tika.grpc.adapter.fastwarc.v1.HeaderField;
import org.apache.tika.grpc.adapter.fastwarc.v1.ParseArchiveRequest;
import org.apache.tika.grpc.adapter.fastwarc.v1.ParseArchiveResponse;
import org.apache.tika.grpc.adapter.fastwarc.v1.ParseWarcConfig;
import org.apache.tika.grpc.adapter.fastwarc.v1.ParsedRecord;
import org.apache.tika.grpc.adapter.fastwarc.v1.RecordError;
import org.apache.tika.grpc.adapter.fastwarc.v1.RecordMetadata;
import org.apache.tika.grpc.adapter.fastwarc.v1.WarcServiceGrpc;
import org.apache.tika.metadata.HttpHeaders;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.metadata.WARC;
import org.apache.tika.mime.MediaType;

/**
 * Maps fastwarc-grpc's {@code fastwarc.v1.WarcService/ParseArchive} reply (TIKA-4951) into
 * tika-grpc's Document: each WARC record becomes an embedded document keyed as Tika's own
 * {@code WARCParser} keys an embedded record ({@code warc:<WARC header>},
 * {@code warc:http:status}, {@code warc:http:<HTTP header>}, ...), with the record's date as
 * {@code dcterms:created} and its HTTP content type as its content type. The parent gets
 * {@code warc:record-count} when FastWARC read the whole archive. Payloads are not
 * requested. Records come back as child documents only: the adapter never changes the
 * parent's own fields. A record the adapter cannot map is left out with a warning; the
 * others are kept.
 * <p>
 * Options ({@code grpc.externalParsers[].options}): {@code verifyDigests} (default false) and
 * {@code quirksMode} (default false), passed to FastWARC as they are.
 */
public class FastWarcAdapter implements ExternalParserAdapter<ParseArchiveRequest, ParseArchiveResponse> {

    public static final String PRODUCER_ID = "fastwarc";

    /** How many records FastWARC reported. */
    public static final Property RECORD_COUNT = Property.externalInteger(WARC.PREFIX + "record-count");

    /**
     * The record's byte offset in the archive, to read it again. Declared as Tika's integer
     * type, which the Document carries as int64, so offsets past 2 GiB are kept whole.
     */
    public static final Property STREAM_POS = Property.externalInteger(WARC.PREFIX + "stream-pos");

    private static final String HTTP_PREFIX = WARC.PREFIX + "http:";

    private static final int MAX_HEADER_NAME = 128;

    /** An HTTP status code: three digits. */
    private static final Pattern STATUS_CODE = Pattern.compile("[1-5][0-9]{2}");

    /** A charset name as RFC 9110 allows it in a parameter: a token, kept short. */
    private static final Pattern CHARSET = Pattern.compile("[A-Za-z0-9!#$%&'*+.^_`|~-]{1,40}");

    /** A {@code charset} parameter already in a media type, whatever its spacing or case. */
    private static final Pattern HAS_CHARSET =
            Pattern.compile(";\\s*charset\\s*=", Pattern.CASE_INSENSITIVE);

    private static final Set<String> OPTIONS = Set.of("verifyDigests", "quirksMode");

    private static final Set<MediaType> TYPES = Set.of(
            MediaType.application("warc"), MediaType.application("warc+gz"));

    @Override
    public String producerId() {
        return PRODUCER_ID;
    }

    @Override
    public Set<MediaType> supportedTypes() {
        return TYPES;
    }

    @Override
    public MethodDescriptor<ParseArchiveRequest, ParseArchiveResponse> method() {
        return WarcServiceGrpc.getParseArchiveMethod();
    }

    @Override
    public String keyPrefix() {
        return WARC.PREFIX;
    }

    /** {@code verifyDigests} and {@code quirksMode}, both booleans; nothing else. */
    @Override
    public void validateOptions(JsonNode options) {
        if (!options.isObject()) {
            throw new IllegalArgumentException("fastwarc options must be an object, got: "
                    + options);
        }
        options.fieldNames().forEachRemaining(name -> {
            if (!OPTIONS.contains(name)) {
                throw new IllegalArgumentException("fastwarc has no option '" + name
                        + "'; it takes " + OPTIONS);
            }
            if (!options.get(name).isBoolean()) {
                throw new IllegalArgumentException("fastwarc option '" + name
                        + "' must be true or false, got: " + options.get(name));
            }
        });
    }

    @Override
    public ParseArchiveRequest buildRequest(AdapterRequest request) {
        JsonNode options = request.options();
        return ParseArchiveRequest.newBuilder()
                .setConfig(ParseWarcConfig.newBuilder()
                        .setParseHttp(true)
                        .setIncludeHeaders(true)
                        .setIncludePayload(false)
                        .setVerifyDigests(options.path("verifyDigests").asBoolean(false))
                        .setQuirksMode(options.path("quirksMode").asBoolean(false)))
                .setArchive(request.content())
                .build();
    }

    /**
     * Records in reply order, as {@code /record-1}, {@code /record-2}, ... These paths number
     * the records FastWARC returned, so they are stable for one FastWARC version and
     * configuration, not across them; {@code warc:WARC-Record-ID} and {@code warc:stream-pos}
     * identify a record for good. An unrecoverable FastWARC error after some records is a
     * partial failure that keeps them; one before any record fails the contribution. When
     * there are several, the first is the reason and the rest are warnings.
     */
    @Override
    public AdapterContribution map(ParseArchiveResponse reply, AdapterRequest request) {
        AdapterContribution.Builder contribution = request.contribution()
                .reportedChildren(reply.getRecordsCount());
        boolean complete = true;
        for (RecordError error : reply.getErrorsList()) {
            complete &= error.getRecoverable();
        }
        // The count says how many records the archive has: only when FastWARC read it all.
        if (complete) {
            contribution.parentKey(RECORD_COUNT.getName(),
                    Integer.toString(reply.getRecordsCount()));
        }
        int unmapped = 0;
        for (int i = 0; i < reply.getRecordsCount(); i++) {
            int number = i + 1;
            Metadata metadata;
            try {
                metadata = record(reply.getRecords(i), number, contribution);
            } catch (RuntimeException e) {
                // One malformed record costs that record, not the archive.
                unmapped++;
                continue;
            }
            contribution.addChild("/record-" + number, metadata);
        }
        if (unmapped > 0) {
            contribution.warn(unmapped + " records could not be mapped and were left out");
        }
        for (RecordError error : reply.getErrorsList()) {
            String message = "record at byte " + error.getStreamPos() + ": " + error.getMessage();
            if (error.getRecoverable()) {
                contribution.warn(message);
            } else if (reply.getRecordsCount() > 0) {
                contribution.partialFailure(message);
            } else {
                contribution.fail(message);
            }
        }
        return contribution.build();
    }

    private static Metadata record(ParsedRecord record, int number,
                                   AdapterContribution.Builder contribution) {
        Metadata metadata = new Metadata();
        RecordMetadata warc = record.getMetadata();
        headers(warc.getWarcHeaders(), WARC.PREFIX, metadata, contribution, number);
        String recordContentType = header(warc.getWarcHeaders(), "Content-Type");
        if (recordContentType != null) {
            metadata.set(WARC.WARC_RECORD_CONTENT_TYPE, recordContentType);
        }
        if (warc.hasRecordId()) {
            metadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, warc.getRecordId());
        }
        if (warc.hasRecordDate()) {
            try {
                metadata.set(TikaCoreProperties.CREATED, Date.from(Instant.ofEpochSecond(
                        warc.getRecordDate().getSeconds(), warc.getRecordDate().getNanos())));
            } catch (DateTimeException | ArithmeticException | IllegalArgumentException e) {
                contribution.warn("record " + number + ": WARC-Date out of range, left out");
            }
        }
        metadata.set(STREAM_POS, Long.toString(warc.getStreamPos()));
        if (warc.hasHttpHeaders()) {
            http(warc.getHttpHeaders(), metadata, contribution, number);
        }
        if (warc.hasHttpContentType() && !warc.getHttpContentType().isBlank()) {
            String contentType = warc.getHttpContentType().trim();
            if (warc.hasHttpCharset() && !HAS_CHARSET.matcher(contentType).find()) {
                String charset = warc.getHttpCharset().trim();
                if (CHARSET.matcher(charset).matches()) {
                    contentType += "; charset=" + charset;
                } else {
                    contribution.warn("record " + number + ": charset is not a token, left"
                            + " out");
                }
            }
            metadata.set(HttpHeaders.CONTENT_TYPE, contentType);
            metadata.set(WARC.WARC_PAYLOAD_CONTENT_TYPE, contentType);
        }
        // No Content-Length of its own: the payload is not requested, and the record's
        // HTTP Content-Length header is kept as warc:http:Content-Length.
        return metadata;
    }

    /**
     * The first line of the HTTP block. A response's status line, {@code HTTP/1.1 200 OK},
     * becomes status 200 and reason OK, as WARCParser splits it. A request's request line,
     * {@code GET /x HTTP/1.1}, becomes {@code warc:http:method} and {@code warc:http:target}.
     * FastWARC returns both in {@code status_line}.
     */
    private static void http(HeaderBlock http, Metadata metadata,
                             AdapterContribution.Builder contribution, int number) {
        if (http.hasStatusLine()) {
            String line = text(http.getStatusLine()).trim();
            // Any run of spaces or tabs separates the parts, as lenient HTTP parsers allow.
            String[] parts = line.split("[ \\t]+", 3);
            if (line.startsWith("HTTP/")) {
                if (parts.length >= 2 && STATUS_CODE.matcher(parts[1]).matches()) {
                    metadata.set(HTTP_PREFIX + "status", parts[1]);
                    if (parts.length == 3 && !parts[2].isBlank()) {
                        metadata.set(HTTP_PREFIX + "status:reason", parts[2].trim());
                    }
                } else {
                    contribution.warn("record " + number + ": HTTP status line has no"
                            + " three-digit status code");
                }
            } else if (parts.length == 3 && parts[2].startsWith("HTTP/")) {
                metadata.set(HTTP_PREFIX + "method", parts[0]);
                metadata.set(HTTP_PREFIX + "target", parts[1]);
            } else if (!line.isEmpty()) {
                contribution.warn("record " + number + ": HTTP first line is neither a status"
                        + " line nor a request line");
            }
        }
        headers(http, HTTP_PREFIX, metadata, contribution, number);
    }

    /**
     * Adds each header as {@code prefix + name}. A name that is not an RFC 9110 token (a
     * malformed or hostile record) is skipped with a warning; tika-grpc bounds the number of
     * keys and the length of values.
     */
    private static void headers(HeaderBlock block, String prefix, Metadata metadata,
                                AdapterContribution.Builder contribution, int number) {
        int skipped = 0;
        for (HeaderField field : block.getFieldsList()) {
            String name = text(field.getName()).trim();
            if (!isToken(name)) {
                skipped++;
                continue;
            }
            metadata.add(prefix + name, text(field.getValue()).trim());
        }
        if (skipped > 0) {
            contribution.warn("record " + number + ": skipped " + skipped
                    + " headers whose names are not tokens");
        }
    }

    private static boolean isToken(String name) {
        if (name.isEmpty() || name.length() > MAX_HEADER_NAME) {
            return false;
        }
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            boolean ok = c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
                    || "!#$%&'*+-.^_`|~".indexOf(c) >= 0;
            if (!ok) {
                return false;
            }
        }
        return true;
    }

    private static String header(HeaderBlock block, String name) {
        for (HeaderField field : block.getFieldsList()) {
            if (text(field.getName()).trim().equalsIgnoreCase(name)) {
                return text(field.getValue()).trim();
            }
        }
        return null;
    }

    /** Header bytes as text: UTF-8 when valid, else ISO-8859-1, which HTTP allows. */
    private static String text(ByteString bytes) {
        return bytes.isValidUtf8()
                ? bytes.toStringUtf8()
                : bytes.toString(StandardCharsets.ISO_8859_1);
    }
}
