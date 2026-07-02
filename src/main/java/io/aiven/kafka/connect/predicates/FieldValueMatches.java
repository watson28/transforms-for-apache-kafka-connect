/*
 * Copyright 2026 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.predicates;

import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.transforms.field.FieldSyntaxVersion;
import org.apache.kafka.connect.transforms.field.SingleFieldPath;

/**
 * A {@link org.apache.kafka.connect.transforms.predicates.Predicate} that matches a record when a
 * field in its value equals an expected value or matches a regular expression.
 *
 * <p>The field is resolved with Kafka Connect's {@link SingleFieldPath}, whose behaviour is
 * controlled by the {@code field.syntax.version} config (see {@link FieldSyntaxVersion}): {@code V1}
 * (default) treats the configured name as a single root-level field, while {@code V2} interprets it
 * as a path and can navigate into nested structures (e.g. {@code after.state}). Both schema-based
 * (Avro) and schemaless (e.g. JSON) values are supported. If no field is configured, the whole value
 * is used.
 */
public class FieldValueMatches<R extends ConnectRecord<R>>
        implements org.apache.kafka.connect.transforms.predicates.Predicate<R> {

    public static final String FIELD_CONFIG = "field";
    public static final String VALUE_CONFIG = "value";
    public static final String VALUE_PATTERN_CONFIG = "value.pattern";

    private Optional<SingleFieldPath> fieldPath;
    private Predicate<SchemaAndValue> condition;

    @Override
    public ConfigDef config() {
        final ConfigDef configDef = new ConfigDef()
                .define(FIELD_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.HIGH,
                        "The field in the record value the predicate is evaluated against, using the field "
                                + "path syntax selected by '" + FieldSyntaxVersion.FIELD_SYNTAX_VERSION_CONFIG + "'. "
                                + "Both schema-based (Avro) and schemaless (e.g. JSON) values are supported. "
                                + "If empty, the whole value is used.")
                .define(VALUE_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.HIGH,
                        "The expected value the field is compared to. Matches string, numeric and boolean fields. "
                                + "An empty string is a valid value. "
                                + "Either define this or '" + VALUE_PATTERN_CONFIG + "'.")
                .define(VALUE_PATTERN_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.HIGH,
                        "A regular expression the whole field value must match (full match, not a substring "
                                + "search); an empty pattern matches only an empty string. "
                                + "Either define this or '" + VALUE_CONFIG + "'.");
        return FieldSyntaxVersion.appendConfigTo(configDef);
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        final AbstractConfig config = new AbstractConfig(config(), configs);

        this.fieldPath = Optional.ofNullable(config.getString(FIELD_CONFIG))
                .filter(name -> !name.isEmpty())
                .map(name -> new SingleFieldPath(name, FieldSyntaxVersion.fromConfig(config)));

        final Optional<String> expectedValue = Optional.ofNullable(config.getString(VALUE_CONFIG));
        final Optional<String> valuePattern = Optional.ofNullable(config.getString(VALUE_PATTERN_CONFIG));
        // A provided empty string is a valid, non-null configuration for both options:
        // an empty 'value' matches the empty string, and an empty 'value.pattern' (a
        // full match) matches only the empty string.
        final boolean expectedValuePresent = expectedValue.isPresent();
        final boolean regexPatternPresent = valuePattern.isPresent();
        if (expectedValuePresent == regexPatternPresent) {
            throw new ConfigException(
                    "Either " + VALUE_CONFIG + " or " + VALUE_PATTERN_CONFIG
                            + " has to be set to apply the predicate");
        }

        if (expectedValuePresent) {
            // Compare the string representations of both sides so that, for example, a
            // numeric field matches value=5 regardless of its concrete numeric type.
            final String expected = expectedValue.get();
            this.condition = schemaAndValue -> schemaAndValue != null
                    && expected.equals(Values.convertToString(schemaAndValue.schema(), schemaAndValue.value()));
        } else {
            final Predicate<String> regexPredicate = compilePattern(valuePattern.get()).asMatchPredicate();
            this.condition = schemaAndValue -> schemaAndValue != null
                    && regexPredicate.test(Values.convertToString(schemaAndValue.schema(), schemaAndValue.value()));
        }
    }

    private static Pattern compilePattern(final String pattern) {
        try {
            return Pattern.compile(pattern);
        } catch (final PatternSyntaxException e) {
            throw new ConfigException(VALUE_PATTERN_CONFIG, pattern, "Invalid regular expression: " + e.getMessage());
        }
    }

    @Override
    public boolean test(final R record) {
        if (record.value() == null) {
            return condition.test(null);
        }
        return condition.test(extractFieldValue(record).orElse(null));
    }

    @SuppressWarnings("unchecked")
    private Optional<SchemaAndValue> extractFieldValue(final R record) {
        final Object recordValue = record.value();
        if (fieldPath.isEmpty()) {
            return Optional.ofNullable(recordValue)
                    .map(value -> new SchemaAndValue(record.valueSchema(), value));
        }
        final SingleFieldPath path = fieldPath.get();
        // A field path can only be resolved against a Struct (schema-based) or a Map
        // (schemaless);
        // any other value type (e.g. a raw byte array or string) has no addressable
        // field.
        if (recordValue instanceof Struct) {
            final Struct struct = (Struct) recordValue;
            return Optional.ofNullable(path.fieldFrom(struct.schema()))
                    .flatMap(field -> Optional.ofNullable(path.valueFrom(struct))
                            .map(value -> new SchemaAndValue(field.schema(), value)));
        }
        if (recordValue instanceof Map) {
            return Optional.ofNullable(path.valueFrom((Map<String, Object>) recordValue))
                    .map(value -> new SchemaAndValue(Values.inferSchema(value), value));
        }
        return Optional.empty();
    }

    @Override
    public void close() {
    }
}
