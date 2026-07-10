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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FieldValueMatchesTest {

    private static final String SYNTAX_V2 = "field.syntax.version";

    private final Schema afterSchema = SchemaBuilder.struct()
        .field("pk", Schema.STRING_SCHEMA)
        .field("state", Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    private final Schema valueSchema = SchemaBuilder.struct()
        .field("after", afterSchema)
        .field("op", Schema.STRING_SCHEMA)
        .build();

    @Test
    void shouldMatchWhenNestedFieldEqualsValue() {
        final FieldValueMatches<SourceRecord> predicate = new FieldValueMatches<>();
        predicate.configure(Map.of(
            "field", "after.state",
            "value", "deleted",
            SYNTAX_V2, "V2"
        ));

        assertThat(predicate.test(structRecord("deleted")))
            .as("record with matching nested field should satisfy the predicate")
            .isTrue();
        assertThat(predicate.test(structRecord("active")))
            .as("record with non-matching nested field should not satisfy the predicate")
            .isFalse();
    }

    @Test
    void shouldNotMatchWhenNestedFieldIsMissing() {
        final FieldValueMatches<SourceRecord> predicate = new FieldValueMatches<>();
        predicate.configure(Map.of(
            "field", "after.state",
            "value", "deleted",
            SYNTAX_V2, "V2"
        ));

        assertThat(predicate.test(structRecord(null)))
            .as("record with null field value should not satisfy the predicate")
            .isFalse();
    }

    @Test
    void shouldMatchWithRegex() {
        final FieldValueMatches<SourceRecord> predicate = new FieldValueMatches<>();
        predicate.configure(Map.of(
            "field", "after.state",
            "value.pattern", "^del.*",
            SYNTAX_V2, "V2"
        ));

        assertThat(predicate.test(structRecord("deleted")))
            .as("record matching the regex should satisfy the predicate")
            .isTrue();
        assertThat(predicate.test(structRecord("active")))
            .as("record not matching the regex should not satisfy the predicate")
            .isFalse();
    }

    @Test
    void shouldMatchNumericFieldValue() {
        final Schema schema = SchemaBuilder.struct()
            .field("count", Schema.INT32_SCHEMA)
            .build();
        final SourceRecord matching = new SourceRecord(null, null, "some_topic",
            null, "key", schema, new Struct(schema).put("count", 5));
        final SourceRecord nonMatching = new SourceRecord(null, null, "some_topic",
            null, "key", schema, new Struct(schema).put("count", 6));

        final FieldValueMatches<SourceRecord> predicate = new FieldValueMatches<>();
        predicate.configure(Map.of(
            "field", "count",
            "value", "5"
        ));

        assertThat(predicate.test(matching))
            .as("integer field equal to 5 should match value=5")
            .isTrue();
        assertThat(predicate.test(nonMatching))
            .as("integer field not equal to 5 should not match value=5")
            .isFalse();
    }

    @Test
    void shouldRequireFullMatchForRegex() {
        final FieldValueMatches<SourceRecord> predicate = new FieldValueMatches<>();
        predicate.configure(Map.of(
            "field", "after.state",
            "value.pattern", "del",
            SYNTAX_V2, "V2"
        ));

        assertThat(predicate.test(structRecord("del")))
            .as("value fully matching the pattern should satisfy the predicate")
            .isTrue();
        assertThat(predicate.test(structRecord("undeleted")))
            .as("value only containing the pattern as a substring should not satisfy the predicate")
            .isFalse();
    }

    @Test
    void shouldMatchNestedFieldContainingDotUsingBacktick() {
        final Schema schema = SchemaBuilder.struct()
            .field("a.b", Schema.STRING_SCHEMA)
            .build();
        final SourceRecord matching = new SourceRecord(null, null, "some_topic",
            null, "key", schema, new Struct(schema).put("a.b", "x"));

        final FieldValueMatches<SourceRecord> predicate = new FieldValueMatches<>();
        predicate.configure(Map.of(
            "field", "`a.b`",
            "value", "x",
            SYNTAX_V2, "V2"
        ));

        assertThat(predicate.test(matching))
            .as("backtick-escaped field name containing a dot should match")
            .isTrue();
    }

    @Test
    void shouldSupportSchemalessValues() {
        final FieldValueMatches<SourceRecord> predicate = new FieldValueMatches<>();
        predicate.configure(Map.of(
            "field", "after.state",
            "value", "deleted",
            SYNTAX_V2, "V2"
        ));

        assertThat(predicate.test(mapRecord("deleted")))
            .as("matching map record should satisfy the predicate")
            .isTrue();
        assertThat(predicate.test(mapRecord("active")))
            .as("non-matching map record should not satisfy the predicate")
            .isFalse();
    }

    @Test
    void shouldNotMatchWhenFieldConfiguredButValueIsNotStructOrMap() {
        final FieldValueMatches<SourceRecord> predicate = new FieldValueMatches<>();
        predicate.configure(Map.of(
            "field", "after.state",
            "value", "deleted",
            SYNTAX_V2, "V2"
        ));

        final SourceRecord schemaButNotStruct = new SourceRecord(null, null, "some_topic",
            null, "key", Schema.BYTES_SCHEMA, new byte[] {1, 2, 3});
        final SourceRecord schemalessScalar = new SourceRecord(null, null, "some_topic",
            null, "key", null, "raw");

        assertThat(predicate.test(schemaButNotStruct))
            .as("record with a non-Struct schema-based value should not satisfy the predicate")
            .isFalse();
        assertThat(predicate.test(schemalessScalar))
            .as("record with a schemaless non-Map value should not satisfy the predicate")
            .isFalse();
    }

    @Test
    void shouldMatchWholeValueWhenFieldIsEmpty() {
        final FieldValueMatches<SourceRecord> predicate = new FieldValueMatches<>();
        predicate.configure(Map.of("value", "deleted"));

        final SourceRecord matching = new SourceRecord(null, null, "some_topic",
            null, "key", Schema.STRING_SCHEMA, "deleted");
        final SourceRecord nonMatching = new SourceRecord(null, null, "some_topic",
            null, "key", Schema.STRING_SCHEMA, "active");
        assertThat(predicate.test(matching))
            .as("record whose whole value matches should satisfy the predicate")
            .isTrue();
        assertThat(predicate.test(nonMatching))
            .as("record whose whole value does not match should not satisfy the predicate")
            .isFalse();
    }

    @Test
    void shouldNotMatchTombstoneValue() {
        final FieldValueMatches<SourceRecord> predicate = new FieldValueMatches<>();
        predicate.configure(Map.of(
            "field", "after.state",
            "value", "deleted",
            SYNTAX_V2, "V2"
        ));

        final SourceRecord tombstone = new SourceRecord(null, null, "some_topic",
            Schema.STRING_SCHEMA, "key", null, null);
        assertThat(predicate.test(tombstone))
            .as("tombstone record should not satisfy the predicate")
            .isFalse();
    }

    @Test
    void shouldFailWhenNeitherValueNorPatternIsSet() {
        final FieldValueMatches<SourceRecord> predicate = new FieldValueMatches<>();
        assertThatThrownBy(() -> predicate.configure(Map.of("field", "after.state")))
            .isInstanceOf(ConfigException.class);
    }

    @Test
    void shouldFailWhenBothValueAndPatternAreSet() {
        final FieldValueMatches<SourceRecord> predicate = new FieldValueMatches<>();
        final Map<String, String> configs = new HashMap<>();
        configs.put("field", "after.state");
        configs.put("value", "deleted");
        configs.put("value.pattern", "^del.*");
        assertThatThrownBy(() -> predicate.configure(configs))
            .isInstanceOf(ConfigException.class);
    }

    @Test
    void shouldMatchEmptyStringValue() {
        final FieldValueMatches<SourceRecord> predicate = new FieldValueMatches<>();
        predicate.configure(Map.of(
            "field", "after.state",
            "value", "",
            SYNTAX_V2, "V2"
        ));

        assertThat(predicate.test(structRecord("")))
            .as("record whose field equals the empty string should satisfy the predicate")
            .isTrue();
        assertThat(predicate.test(structRecord("active")))
            .as("record whose field is non-empty should not satisfy the predicate")
            .isFalse();
    }

    @Test
    void shouldFailWithConfigExceptionOnInvalidRegex() {
        final FieldValueMatches<SourceRecord> predicate = new FieldValueMatches<>();
        assertThatThrownBy(() -> predicate.configure(Map.of(
            "field", "after.state",
            "value.pattern", "[unclosed"
        )))
            .isInstanceOf(ConfigException.class);
    }

    private SourceRecord structRecord(final String state) {
        final Struct after = new Struct(afterSchema).put("pk", "1");
        if (state != null) {
            after.put("state", state);
        }
        final Struct value = new Struct(valueSchema)
            .put("after", after)
            .put("op", "u");
        return new SourceRecord(null, null, "some_topic",
            Schema.STRING_SCHEMA, "key", valueSchema, value);
    }

    private SourceRecord mapRecord(final String state) {
        final Map<String, Object> after = new HashMap<>();
        after.put("pk", "1");
        after.put("state", state);
        final Map<String, Object> value = new HashMap<>();
        value.put("after", after);
        value.put("op", "u");
        return new SourceRecord(null, null, "some_topic",
            null, "key", null, value);
    }
}
