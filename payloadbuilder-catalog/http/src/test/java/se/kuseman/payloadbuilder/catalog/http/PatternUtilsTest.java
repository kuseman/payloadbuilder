package se.kuseman.payloadbuilder.catalog.http;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.ContentType;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate.ISeekKey;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.test.ExpressionTestUtils;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test {@link PatternUtils}. */
class PatternUtilsTest
{
    @Test
    void test_createValues_describe_mode()
    {
        IExecutionContext executionContext = Mockito.mock(IExecutionContext.class);
        assertEquals(emptyMap(), PatternUtils.createValues(executionContext, null, emptyList(), true));

        ISeekPredicate seekPredicate = Mockito.mock(ISeekPredicate.class);
        when(seekPredicate.getIndexColumns()).thenReturn(List.of("id", "type"));

        //@formatter:off
        assertEquals(Map.of(
                        "field", UTF8String.from("value"),
                        "fields", List.of(UTF8String.from("value")),
                        "@seekValues", List.of(
                            Map.of("type", "<index values>",
                                   "id", "<index values>",
                                   "_", Map.of(
                                       "field", UTF8String.from("value"),
                                       "fields", List.of(UTF8String.from("value"))
                                   ))
                        )
                ),
                PatternUtils.createValues(executionContext, seekPredicate, List.of(
                        new HttpCatalog.Predicate("field", List.of(ExpressionTestUtils.createStringExpression("value")), true),
                        new HttpCatalog.Predicate("fields", List.of(ExpressionTestUtils.createStringExpression("value")), false)), true));
        //@formatter:on
    }

    @Test
    void test_createValues()
    {
        IExecutionContext executionContext = Mockito.mock(IExecutionContext.class);
        assertEquals(emptyMap(), PatternUtils.createValues(executionContext, null, emptyList(), false));

        //@formatter:off
        assertEquals(Map.of(
                        "field", UTF8String.from("value"),
                        "fields", List.of(UTF8String.from("value"))
                ),
                PatternUtils.createValues(executionContext, null, List.of(
                        new HttpCatalog.Predicate("field", List.of(ExpressionTestUtils.createStringExpression("value")), true),
                        new HttpCatalog.Predicate("fields", List.of(ExpressionTestUtils.createStringExpression("value")), false)), false));
        //@formatter:on

        ISeekPredicate seekPredicate = Mockito.mock(ISeekPredicate.class);
        when(seekPredicate.getIndexColumns()).thenReturn(List.of("id", "type"));

        ISeekPredicate.ISeekKey key1 = mock(ISeekKey.class);
        ISeekPredicate.ISeekKey key2 = mock(ISeekKey.class);
        when(key1.getValue()).thenReturn(vv(Column.Type.Int, 1, 2, 3));
        when(key2.getValue()).thenReturn(vv(Column.Type.String, "one", "two", "three"));
        when(seekPredicate.getSeekKeys(executionContext)).thenReturn(List.of(key1, key2));

        //@formatter:off
        assertEquals(Map.of(
                PatternUtils.SEEK_VALUES, List.of(
                        Map.of("id", 1, "type", UTF8String.from("one"), "_", Map.of("field", UTF8String.from("value"), "fields", List.of(UTF8String.from("value")))),
                        Map.of("id", 2, "type", UTF8String.from("two"), "_", Map.of("field", UTF8String.from("value"), "fields", List.of(UTF8String.from("value")))),
                        Map.of("id", 3, "type", UTF8String.from("three"), "_", Map.of("field", UTF8String.from("value"), "fields", List.of(UTF8String.from("value"))))
                        ),
                "field", UTF8String.from("value"),
                "fields", List.of(UTF8String.from("value"))),
                PatternUtils.createValues(executionContext, seekPredicate, List.of(
                        new HttpCatalog.Predicate("field", List.of(ExpressionTestUtils.createStringExpression("value")), true),
                        new HttpCatalog.Predicate("fields", List.of(ExpressionTestUtils.createStringExpression("value")), false)), false));
        //@formatter:on

        // Verify that we transform single seek key to predicate
        when(seekPredicate.getIndexColumns()).thenReturn(List.of("id"));
        when(seekPredicate.getSeekKeys(executionContext)).thenReturn(List.of(key1));
        //@formatter:off
        assertEquals(Map.of(
                    "id", List.of(1, 2, 3)),
                PatternUtils.createValues(executionContext, seekPredicate, List.of(), false));
        //@formatter:on

        // Verify that transformed single seek key are merged with conflicting predicate
        when(seekPredicate.getIndexColumns()).thenReturn(List.of("id"));
        when(seekPredicate.getSeekKeys(executionContext)).thenReturn(List.of(key1));
        //@formatter:off
        assertEquals(Map.of(
                    "id", List.of(4, 1, 2, 3)),
                PatternUtils.createValues(executionContext, seekPredicate, List.of(
                        new HttpCatalog.Predicate("id", List.of(ExpressionTestUtils.createIntegerExpression(4)), true)
                        ), false));

        assertEquals(Map.of(
                "id", List.of(4, 5, 1, 2, 3)),
            PatternUtils.createValues(executionContext, seekPredicate, List.of(
                    new HttpCatalog.Predicate("id", List.of(ExpressionTestUtils.createIntegerExpression(4), ExpressionTestUtils.createIntegerExpression(5)), false)
                    ), false));
        //@formatter:on
    }

    @Test
    void test_extractFields_and_process()
    {
        IExecutionContext executionContext = Mockito.mock(IExecutionContext.class);
        ISeekPredicate seekPredicate = Mockito.mock(ISeekPredicate.class);
        when(seekPredicate.getIndexColumns()).thenReturn(List.of("id", "type"));

        ISeekPredicate.ISeekKey key1 = mock(ISeekKey.class);
        ISeekPredicate.ISeekKey key2 = mock(ISeekKey.class);
        when(key1.getValue()).thenReturn(VectorTestUtils.vv(Column.Type.Int, 1, 2, 3));
        when(key2.getValue()).thenReturn(VectorTestUtils.vv(Column.Type.String, "one", "two", "three"));
        when(seekPredicate.getSeekKeys(executionContext)).thenReturn(List.of(key1, key2));

        Map<String, Object> values = PatternUtils.createValues(executionContext, seekPredicate,
                List.of(new HttpCatalog.Predicate("field", List.of(ExpressionTestUtils.createFloatExpression(1.33F)), true),
                        new HttpCatalog.Predicate("fields", List.of(ExpressionTestUtils.createStringExpression("value")), false)),
                false);

        assertEquals("""
                {
                    "query": {
                        "boolean": {
                            "should": [
                                {
                                    "terms": {
                                        "id": 1.33
                                    }
                                },
                                {
                                    "terms": {
                                        "ids": ["value"]
                                    }
                                },
                                {
                                    "boolean": {
                                        "must": [
                                            {
                                                "term": {
                                                    "id": "1"
                                                }
                                            }
                                                {
                                                "term": {
                                                    "type": "one"
                                                }
                                            }
                                        ]
                                    }
                                },
                                {
                                    "boolean": {
                                        "must": [
                                            {
                                                "term": {
                                                    "id": "2"
                                                }
                                            }
                                                {
                                                "term": {
                                                    "type": "two"
                                                }
                                            }
                                        ]
                                    }
                                },
                                {
                                    "boolean": {
                                        "must": [
                                            {
                                                "term": {
                                                    "id": "3"
                                                }
                                            }
                                                {
                                                "term": {
                                                    "type": "three"
                                                }
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                }
                """, PatternUtils.replacePattern("""
                {
                    "query": {
                        "boolean": {
                            "should": [
                                {{#field}}
                                {
                                    "terms": {
                                        "id": {{field}}
                                    }
                                }{{#fields}},{{/fields}}
                                {{/field}}
                                {{#fields.size}}
                                {
                                    "terms": {
                                        "ids": [{{#fields}}"{{.}}"{{#iterHasNext}},{{/iterHasNext}}{{/fields}}]
                                    }
                                }{{#@seekValues.size}},{{/@seekValues.size}}
                                {{/fields.size}}
                                {{#@seekValues}}
                                {
                                    "boolean": {
                                        "must": [
                                            {
                                                "term": {
                                                    "id": "{{id}}"
                                                }
                                            }
                                                {
                                                "term": {
                                                    "type": "{{type}}"
                                                }
                                            }
                                        ]
                                    }
                                }{{#iterHasNext}},{{/iterHasNext}}
                                {{/@seekValues}}
                            ]
                        }
                    }
                }
                """, ContentType.APPLICATION_JSON, values));
    }

    @Test
    void test_extractFields()
    {
        assertEquals(List.of("id"), PatternUtils.extractFields("{{id}}"));
        // Verify we pick up parent scope fields from within seek values
        assertEquals(List.of("id"), PatternUtils.extractFields("""
                {{#@seekValues}}
                {{_.id}}
                {{/@seekValues}}
                """));
        // Verify that inverted blocks are picked up
        assertEquals(List.of("id"), PatternUtils.extractFields("""
                {{^@seekValues}}
                {{id}}
                {{/@seekValues}}
                """));
        assertEquals(List.of("id", "type"), PatternUtils.extractFields("""
                {{^id}}
                {{type}}
                {{/id}}
                """));

        assertEquals(List.of("id"), PatternUtils.extractFields("""
                {{#@seekValues}}
                {{id}}
                {{/@seekValues}}
                """));

        assertEquals(List.of("ids", "id", "type"), PatternUtils.extractFields("""
                {
                    "query": {
                        "boolean": {
                            "should": [
                                {{#ids.size}}
                                {
                                    "terms": {
                                        "ids": {{#ids}}"{{.}}",{{/ids}}
                                    }
                                },
                                {{/ids.size}}
                                {{#@seekValues}}
                                {
                                    "boolean": {
                                        "must": [
                                            {
                                                "term": {
                                                    "id": "{{id#join}}"
                                                }
                                            }
                                                {
                                                "term": {
                                                    "type": "{{type#tojsonarray}}"
                                                }
                                            }
                                        ]
                                    }
                                }<delim>
                                {{/@seekValues}}
                            ]
                        }
                    }
                }
                """));
    }

    @Test
    void test_helpers()
    {
        Map<String, Object> map = new HashMap<>();
        map.put("list", null);
        // join
        assertEquals("", PatternUtils.replacePattern("{{list#join}}", ContentType.APPLICATION_JSON, map));
        assertEquals("", PatternUtils.replacePattern("{{list#join}}", ContentType.APPLICATION_JSON, emptyMap()));
        assertEquals("1", PatternUtils.replacePattern("{{list#join}}", ContentType.APPLICATION_JSON, Map.of("list", 1)));
        assertEquals("Welcome hello-world", PatternUtils.replacePattern("Welcome {{list#join}}-world", ContentType.APPLICATION_JSON, Map.of("list", "hello")));
        assertEquals("1-value", PatternUtils.replacePattern("{{list#join}}-value", ContentType.APPLICATION_JSON, Map.of("list", 1)));
        assertEquals("1,2,3", PatternUtils.replacePattern("{{list#join}}", ContentType.APPLICATION_JSON, Map.of("list", List.of(1, 2, 3))));
        assertEquals("1;2;3", PatternUtils.replacePattern("{{list#join:;}}", ContentType.APPLICATION_JSON, Map.of("list", List.of(1, 2, 3))));
        assertEquals("1;2;3", PatternUtils.replacePattern("{{list#join:;}}", ContentType.APPLICATION_JSON, Map.of("list", new PatternUtils.ValueVectorList(vv(Column.Type.Int, 1, 2, 3)))));
        assertEquals("1<->2<->3", PatternUtils.replacePattern("{{list#join:<->}}", ContentType.APPLICATION_JSON, Map.of("list", List.of(1, 2, 3))));
        assertEquals("""
                {
                    "ids": [\\"hello,2,3]
                }
                """, PatternUtils.replacePattern("""
                {
                    "ids": [{{list#join:,}}]
                }
                """, ContentType.APPLICATION_JSON, Map.of("list", List.of("\"hello", 2, 3))));

        // tojsonarray
        assertEquals("null", PatternUtils.replacePattern("{{list#tojsonarray}}", ContentType.APPLICATION_JSON, map));
        assertEquals("null", PatternUtils.replacePattern("{{list#tojsonarray}}", ContentType.APPLICATION_JSON, emptyMap()));
        assertEquals("[1]", PatternUtils.replacePattern("{{list#tojsonarray}}", ContentType.APPLICATION_JSON, Map.of("list", 1)));
        assertEquals("[1,2,3]", PatternUtils.replacePattern("{{list#tojsonarray}}", ContentType.APPLICATION_JSON, Map.of("list", List.of(1, 2, 3))));
        assertEquals("[\"\\\"hello\",2,3]", PatternUtils.replacePattern("{{list#tojsonarray}}", ContentType.APPLICATION_JSON, Map.of("list", List.of("\"hello", 2, 3))));
    }

    @Test
    void test_iterHasNext()
    {
        Map<String, Object> map = new HashMap<>();
        map.put("field1", null);
        // Null
        assertEquals("[]", PatternUtils.replacePattern("[{{#field1}}\"{{.}}\"{{#iterHasNext}};{{/iterHasNext}}{{/field1}}]", ContentType.APPLICATION_JSON, map));

        // String
        assertEquals("[\"hello\"]", PatternUtils.replacePattern("[{{#field1}}\"{{.}}\"{{#iterHasNext}};{{/iterHasNext}}{{/field1}}]", ContentType.APPLICATION_JSON, Map.of("field1", "hello")));
        // String empty
        assertEquals("[]", PatternUtils.replacePattern("[{{#field1}}\"{{.}}\"{{#iterHasNext}};{{/iterHasNext}}{{/field1}}]", ContentType.APPLICATION_JSON, Map.of("field1", "")));

        // List
        assertEquals("[\"1\";\"2\";\"3\"]",
                PatternUtils.replacePattern("[{{#field1}}\"{{.}}\"{{#iterHasNext}};{{/iterHasNext}}{{/field1}}]", ContentType.APPLICATION_JSON, Map.of("field1", List.of(1, 2, 3))));

        // Iterable
        assertEquals("[\"1\";\"2\";\"3\"]",
                PatternUtils.replacePattern("[{{#field1}}\"{{.}}\"{{#iterHasNext}};{{/iterHasNext}}{{/field1}}]", ContentType.APPLICATION_JSON, Map.of("field1", new Iterable<Integer>()
                {
                    @Override
                    public Iterator<Integer> iterator()
                    {
                        return List.of(1, 2, 3)
                                .iterator();
                    }
                })));

        // Iterator
        assertEquals("[\"1\";\"2\";\"3\"]",
                PatternUtils.replacePattern("[{{#field1}}\"{{.}}\"{{#iterHasNext}};{{/iterHasNext}}{{/field1}}]", ContentType.APPLICATION_JSON, Map.of("field1", List.of(1, 2, 3)
                        .iterator())));

        // Array
        assertEquals("[\"1\";\"2\";\"3\"]",
                PatternUtils.replacePattern("[{{#field1}}\"{{.}}\"{{#iterHasNext}};{{/iterHasNext}}{{/field1}}]", ContentType.APPLICATION_JSON, Map.of("field1", List.of(1, 2, 3)
                        .toArray())));

        // Nested
        //@formatter:off
        assertEquals("""
                [
                    {
                        "key1": 1,
                        "key2": 2
                    },
                    {
                        "key1": 1,
                        "key2": 2,
                        "key3": 3
                    },
                    {
                        "key1": 1
                    }
                ]
                """,
                PatternUtils.replacePattern("""
                        [
                            {{#outer}}
                            {
                                {{#inner}}
                                "key{{.}}": {{.}}{{#iterHasNext}},{{/iterHasNext}}
                                {{/inner}}
                            }{{#iterHasNext}},{{/iterHasNext}}
                            {{/outer}}
                        ]
                        """, ContentType.APPLICATION_JSON, Map.of("outer", List.of(
                                Map.of("inner", List.of(1, 2)),
                                Map.of("inner", List.of(1, 2, 3).iterator()),
                                Map.of("inner", List.of(1))
                                ))));

        //@formatter:on
    }

    /** Verify that the old placeholder pattern still works that was used before introduction of mustache. */
    @Test
    void test_replaceBodyPattern_old_iterable_style()
    {
        assertEquals("[\"\\\"hello\",2,3]", PatternUtils.replacePattern("[{{field1}}]", ContentType.APPLICATION_JSON, Map.of("field1", List.of("\"hello", 2, 3))));
        // UTF8 String
        assertEquals("[\"\\\"hello\",2,3]", PatternUtils.replacePattern("[{{field1}}]", ContentType.APPLICATION_JSON, Map.of("field1", List.of(UTF8String.from("\"hello"), 2, 3))));
        assertEquals("id=%22hello,2,3", PatternUtils.replacePattern("id={{field1}}", PatternUtils.URL_ENCODED_CONTENT_TYPE, Map.of("field1", List.of("\"hello", 2, 3))));
        // UTF8 String
        assertEquals("id=%22hello,2,3", PatternUtils.replacePattern("id={{field1}}", PatternUtils.URL_ENCODED_CONTENT_TYPE, Map.of("field1", List.of(UTF8String.from("\"hello"), 2, 3))));
    }

    @Test
    void test_replaceBodyPattern_urlencode()
    {
        assertEquals("id=%22hello,2,3",
                PatternUtils.replacePattern("id={{#field1}}{{.}}{{#iterHasNext}},{{/iterHasNext}}{{/field1}}", PatternUtils.URL_ENCODED_CONTENT_TYPE, Map.of("field1", List.of("\"hello", 2, 3))));
        // Encoded delimiter
        assertEquals("id=%22hello%3F2%3F3", PatternUtils.replacePattern("id={{field1#join:?}}", PatternUtils.URL_ENCODED_CONTENT_TYPE, Map.of("field1", List.of("\"hello", 2, 3))));
        assertEquals("id=%22hello%7C2%7C3", PatternUtils.replacePattern("id={{field1#join:|}}", PatternUtils.URL_ENCODED_CONTENT_TYPE, Map.of("field1", List.of("\"hello", 2, 3))));
        assertEquals("id=%22hello%2C2%2C3", PatternUtils.replacePattern("id={{field1#join}}", PatternUtils.URL_ENCODED_CONTENT_TYPE, Map.of("field1", List.of("\"hello", 2, 3))));
    }

    @Test
    void test_replaceBodyPattern_default_content_type()
    {
        assertEquals("<fields><field>\"hello</field><field>2</field><field>3</field></fields>",
                PatternUtils.replacePattern("<fields>{{#field1}}<field>{{.}}</field>{{/field1}}</fields>", ContentType.TEXT_XML, Map.of("field1", List.of("\"hello", 2, 3))));
    }

    @Test
    void test_replaceBodyPattern_json()
    {
        assertEquals("[\"\\\"hello\";\"2\";\"3\"]",
                PatternUtils.replacePattern("[{{#field1}}\"{{.}}\"{{#iterHasNext}};{{/iterHasNext}}{{/field1}}]", ContentType.APPLICATION_JSON, Map.of("field1", List.of("\"hello", 2, 3))));
        assertEquals("[\"\\\"hello\",\"2\",\"3\",]", PatternUtils.replacePattern("[{{#field1}}\"{{.}}\",{{/field1}}]", ContentType.APPLICATION_JSON, Map.of("field1", List.of("\"hello", 2, 3))));

        assertEquals("""
                {
                    "query": {
                        "boolean": {
                            "should": [
                                {
                                    "boolean": {
                                        "must": [
                                            {
                                                "term": {
                                                    "id": "1"
                                                }
                                            }
                                                {
                                                "term": {
                                                    "type": "a"
                                                }
                                            }
                                        ]
                                    }
                                }<delim>
                                {
                                    "boolean": {
                                        "must": [
                                            {
                                                "term": {
                                                    "id": "2"
                                                }
                                            }
                                                {
                                                "term": {
                                                    "type": "b"
                                                }
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                }
                """, PatternUtils.replacePattern("""
                {
                    "query": {
                        "boolean": {
                            "should": [
                                {{#@index}}
                                {
                                    "boolean": {
                                        "must": [
                                            {
                                                "term": {
                                                    "id": "{{id}}"
                                                }
                                            }
                                                {
                                                "term": {
                                                    "type": "{{type}}"
                                                }
                                            }
                                        ]
                                    }
                                }{{#iterHasNext}}<delim>{{/iterHasNext}}
                                {{/@index}}
                            ]
                        }
                    }
                }
                """, ContentType.APPLICATION_JSON, Map.of("@index", List.of(Map.of("id", 1, "type", "a"), Map.of("id", 2, "type", "b")))));
    }
}
