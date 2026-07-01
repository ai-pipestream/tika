/*
 * Reference example for document-proposal.proto (design draft, not yet wired to a module).
 */
package org.apache.tika.grpc.transform;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.protobuf.Timestamp;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;

/**
 * Helpers for transformers: set a typed Document field from a Tika Property and mark the
 * key consumed so it is not duplicated into the tagged tail.
 */
final class TransformSupport {

    private TransformSupport() {
    }

    static void setString(Metadata md, Property key, Consumer<String> setter, Set<String> consumed) {
        String v = md.get(key);
        if (v != null && !v.trim().isEmpty()) {
            setter.accept(v.trim());
            consumed.add(key.getName());
        }
    }

    static void addStrings(Metadata md, Property key, Consumer<Iterable<String>> setter, Set<String> consumed) {
        String[] values = md.getValues(key);
        if (values != null && values.length > 0) {
            List<String> list = Arrays.stream(values)
                    .filter(s -> s != null && !s.trim().isEmpty())
                    .map(String::trim)
                    .collect(Collectors.toList());
            if (!list.isEmpty()) {
                setter.accept(list);
                consumed.add(key.getName());
            }
        }
    }

    static void setInt(Metadata md, Property key, Consumer<Integer> setter, Set<String> consumed) {
        Integer v = md.getInt(key);
        if (v != null) {
            setter.accept(v);
            consumed.add(key.getName());
        }
    }

    static void setTimestamp(Metadata md, Property key, Consumer<Timestamp> setter, Set<String> consumed) {
        Date d = md.getDate(key);
        if (d != null) {
            setter.accept(MetadataTagger.toTimestamp(d));
            consumed.add(key.getName());
        }
    }
}
