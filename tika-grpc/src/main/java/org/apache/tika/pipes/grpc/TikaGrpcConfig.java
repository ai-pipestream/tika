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

import java.io.IOException;

import org.apache.tika.config.loader.TikaJsonConfig;

/**
 * Configuration for security-sensitive tika-grpc features, loaded from the
 * {@code "grpc"} section of the tika-config JSON.
 * <p>
 * Both flags default to {@code false} so an out-of-the-box server is locked
 * down: clients may only fetch-and-parse using the fetchers and pipes iterators
 * the operator declared in the config file, using the server's own parse
 * configuration. The capabilities below are dangerous and must be opted into
 * explicitly:
 * <ul>
 *   <li>{@link #isAllowPerRequestConfig()} lets a client reconfigure any
 *   pipeline component for a single request.</li>
 *   <li>{@link #isAllowComponentManagement()} lets a client change which
 *   fetchers and iterators the server has, and read their stored configs.</li>
 * </ul>
 */
public class TikaGrpcConfig {

    /**
     * The ParseBytes content cap that governs when {@code parseBytesMaxContentBytes}
     * is absent from the config.
     */
    public static final long DEFAULT_PARSE_BYTES_MAX_CONTENT_BYTES = 64L * 1024 * 1024;

    private boolean allowPerRequestConfig = false;

    private boolean allowComponentManagement = false;

    private Integer maxInboundMessageBytes = null;

    private Long parseBytesMaxContentBytes = null;

    /**
     * Loads {@link TikaGrpcConfig} from the {@code "grpc"} section of the JSON
     * configuration, or returns a locked-down default instance (all flags
     * {@code false}) if no {@code "grpc"} section is present.
     *
     * @param tikaJsonConfig the JSON configuration to load from
     * @return the loaded config, or a default (locked-down) instance
     * @throws IOException if deserialization fails
     */
    public static TikaGrpcConfig load(TikaJsonConfig tikaJsonConfig) throws IOException {
        TikaGrpcConfig config = tikaJsonConfig.deserialize("grpc", TikaGrpcConfig.class);
        if (config == null) {
            config = new TikaGrpcConfig();
        }
        return config;
    }

    /**
     * Whether clients may attach per-request configuration to FetchAndParse
     * requests (the {@code additional_fetch_config_json} and
     * {@code parse_context_json} fields), overriding the server's defaults for
     * that single request.
     * <p>
     * This is dangerous because the supplied JSON can reconfigure <em>any</em>
     * pipeline component (fetcher, parser/handler, timeout limits, ...), not
     * just parse behavior. Defaults to {@code false}; when {@code false}, a
     * request carrying either field is rejected.
     *
     * @return true if per-request configuration is permitted
     */
    public boolean isAllowPerRequestConfig() {
        return allowPerRequestConfig;
    }

    public void setAllowPerRequestConfig(boolean allowPerRequestConfig) {
        this.allowPerRequestConfig = allowPerRequestConfig;
    }

    /**
     * Whether clients may manage fetchers and pipes iterators at runtime: add,
     * modify, or delete them (the SaveFetcher, DeleteFetcher, SavePipesIterator
     * and DeletePipesIterator RPCs), and read their stored configuration back
     * (the GetFetcher, ListFetchers and GetPipesIterator RPCs).
     * <p>
     * This is dangerous on two counts. Writes change what the server can reach
     * for all subsequent requests and clients (for example, adding a fetcher
     * that escapes a configured base path or points at an internal host). Reads
     * return stored component configs verbatim, which may include secrets
     * (passwords, access keys, tokens). Defaults to {@code false}; when
     * {@code false}, the mutating RPCs are rejected and the read RPCs return
     * only component identity (id and class), never the config.
     *
     * @return true if runtime component management is permitted
     */
    public boolean isAllowComponentManagement() {
        return allowComponentManagement;
    }

    public void setAllowComponentManagement(boolean allowComponentManagement) {
        this.allowComponentManagement = allowComponentManagement;
    }

    /**
     * The largest request the server will accept, in bytes, applied as gRPC's
     * {@code maxInboundMessageSize}. {@code null} means the key is absent and gRPC's own
     * default (roughly 4 MiB) governs, which is what an unconfigured server ships with.
     * <p>
     * The type is boxed on purpose: "not configured" is a distinct state from any value,
     * and the server must leave the builder untouched rather than reassert a default of
     * its own.
     * <p>
     * This is server-global. Raising it raises what <em>every</em> RPC will accept,
     * including the management calls that only ever carry small JSON, and a unary message
     * is materialised in full in heap before the handler runs, so this also bounds the
     * heap one <em>request</em> can demand. It is not an aggregate budget: concurrent
     * requests each get this much, and nothing here caps how many are in flight. It is
     * also unrelated to the ParseBytes content cap, which bounds only {@code content} and
     * is enforced separately; the two are never combined into a third number.
     *
     * @return the configured inbound limit in bytes, or {@code null} if unset
     */
    public Integer getMaxInboundMessageBytes() {
        return maxInboundMessageBytes;
    }

    /**
     * @param maxInboundMessageBytes positive limit in bytes, or {@code null} to defer to
     *                               gRPC's default
     * @throws IllegalArgumentException if the value is present and not positive
     */
    public void setMaxInboundMessageBytes(Integer maxInboundMessageBytes) {
        if (maxInboundMessageBytes != null && maxInboundMessageBytes <= 0) {
            throw new IllegalArgumentException(
                    "maxInboundMessageBytes must be positive, got: " + maxInboundMessageBytes);
        }
        this.maxInboundMessageBytes = maxInboundMessageBytes;
    }

    /**
     * Maximum ParseBytes content size, in bytes. {@code null} keeps the 64 MiB default
     * ({@link #DEFAULT_PARSE_BYTES_MAX_CONTENT_BYTES}). Checked before spooling; gRPC
     * applies its inbound limit separately to the whole request and may reject it first.
     * Values above the unary limit only become usable with a future streaming entrypoint.
     *
     * @return the configured cap in bytes, or {@code null} if unset
     */
    public Long getParseBytesMaxContentBytes() {
        return parseBytesMaxContentBytes;
    }

    /**
     * @param parseBytesMaxContentBytes positive cap in bytes, or {@code null} to use
     *                                  {@link #DEFAULT_PARSE_BYTES_MAX_CONTENT_BYTES}
     * @throws IllegalArgumentException if the value is present and not positive
     */
    public void setParseBytesMaxContentBytes(Long parseBytesMaxContentBytes) {
        if (parseBytesMaxContentBytes != null && parseBytesMaxContentBytes <= 0) {
            throw new IllegalArgumentException(
                    "parseBytesMaxContentBytes must be positive, got: "
                            + parseBytesMaxContentBytes);
        }
        this.parseBytesMaxContentBytes = parseBytesMaxContentBytes;
    }

    /**
     * @return the configured {@link #getParseBytesMaxContentBytes()}, or
     *         {@link #DEFAULT_PARSE_BYTES_MAX_CONTENT_BYTES} when unset
     */
    public long effectiveParseBytesMaxContentBytes() {
        return parseBytesMaxContentBytes != null
                ? parseBytesMaxContentBytes
                : DEFAULT_PARSE_BYTES_MAX_CONTENT_BYTES;
    }
}
