package se.kuseman.payloadbuilder.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.core.JsonOutputWriter.JsonSettings;
import se.kuseman.payloadbuilder.core.expression.LiteralBooleanExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralNullExpression;

/** Test of {@link JsonOutputWriter} */
class JsonOutputWriterTest
{
    @Test
    void test_outputstream_escape()
    {
        Pair<ByteArrayOutputStream, JsonOutputWriter> p = outputstream(new JsonSettings());

        //@formatter:off
        // First result set
        p.getValue().initResult(new String[0]);
        p.getValue().startRow();
        p.getValue().startObject();
        p.getValue().writeFieldName("key");
        p.getValue().writeString(UTF8String.utf8(",\"#\\n".getBytes(StandardCharsets.UTF_8)));
        p.getValue().endObject();
        p.getValue().endResult();
        //@formatter:on

        p.getValue()
                .close();

        assertEquals("{\"key\":\",\\\"#\\\\n\"}", p.getKey()
                .toString());

    }

    @Test
    void test_allResultSetsAsOneArray_no_rows()
    {
        JsonSettings settings = new JsonSettings();
        settings.setAllResultSetsAsOneArray(true);

        Pair<StringWriter, JsonOutputWriter> p = writer(settings);
        p.getValue()
                .close();

        assertEquals("", p.getKey()
                .toString());
    }

    @Test
    void test_allResultSetsAsOneArray()
    {
        JsonSettings settings = new JsonSettings();
        settings.setAllResultSetsAsOneArray(true);

        Pair<StringWriter, JsonOutputWriter> p = writer(settings);

        //@formatter:off
        // First result set
        p.getValue().initResult(new String[0]);
        p.getValue().startRow();
        p.getValue().startObject();
        p.getValue().writeFieldName("key");
        p.getValue().writeInt(123);
        p.getValue().endObject();
        p.getValue().endResult();

        // Second
        p.getValue().initResult(new String[0]);
        p.getValue().startRow();
        p.getValue().startObject();
        p.getValue().writeFieldName("key2");
        p.getValue().writeInt(456);
        p.getValue().endObject();
        p.getValue().endResult();
        //@formatter:on

        p.getValue()
                .close();

        assertEquals("[{\"key\":123},{\"key2\":456}]", p.getKey()
                .toString());
    }

    @Test
    void test_resultSetsAsArrays()
    {
        JsonSettings settings = new JsonSettings();
        settings.setResultSetsAsArrays(true);
        settings.setResultSetSeparator("======");

        Pair<StringWriter, JsonOutputWriter> p = writer(settings);

        //@formatter:off
        // First result set
        p.getValue().initResult(new String[0]);
        p.getValue().startRow();
        p.getValue().startObject();
        p.getValue().writeFieldName("key");
        p.getValue().writeInt(123);
        p.getValue().endObject();
        p.getValue().endResult();
        p.getValue().flush();

        // Second
        p.getValue().initResult(new String[0]);
        p.getValue().startRow();
        p.getValue().startObject();
        p.getValue().writeFieldName("key2");
        p.getValue().writeInt(456);
        p.getValue().endObject();
        p.getValue().endResult();
        //@formatter:on

        p.getValue()
                .close();

        assertEquals("[{\"key\":123}]====== [{\"key2\":456}]", p.getKey()
                .toString());
    }

    @Test
    void test_row_and_resultset_separator()
    {
        JsonSettings settings = new JsonSettings();
        settings.setRowSeparator("####");
        settings.setResultSetSeparator("====");

        Pair<StringWriter, JsonOutputWriter> p = writer(settings);

        //@formatter:off
        // First result set
        p.getValue().initResult(new String[0]);
        p.getValue().startRow();
        p.getValue().startObject();
        p.getValue().writeFieldName("key");
        p.getValue().writeInt(123);
        p.getValue().endObject();
        p.getValue().endRow();
        p.getValue().endResult();
        p.getValue().flush();

        // Second
        p.getValue().initResult(new String[0]);
        p.getValue().startRow();
        p.getValue().startObject();
        p.getValue().writeFieldName("key2");
        p.getValue().writeInt(456);
        p.getValue().endObject();
        p.getValue().endRow();
        p.getValue().endResult();
        //@formatter:on

        p.getValue()
                .close();

        assertEquals("{\"key\":123}####==== {\"key2\":456}####", p.getKey()
                .toString());
    }

    @Test
    void test()
    {
        Pair<StringWriter, JsonOutputWriter> p = writer();
        p.getValue()
                .writeValue(null);
        p.getValue()
                .close();
        assertEquals("null", p.getKey()
                .toString());

        p = writer();
        p.getValue()
                .startRow();
        p.getValue()
                .startObject();
        p.getValue()
                .writeFieldName("col1");
        p.getValue()
                .writeValue(null);
        p.getValue()
                .writeFieldName("col2");
        p.getValue()
                .writeValue(true);
        p.getValue()
                .writeFieldName("col3");
        p.getValue()
                .writeValue(1);
        p.getValue()
                .writeFieldName("col4");
        p.getValue()
                .writeValue(1.10f);
        p.getValue()
                .writeFieldName("col5");
        p.getValue()
                .writeValue(false);
        p.getValue()
                .writeFieldName("col6");
        p.getValue()
                .writeValue("hello " + (char) 10 + " wor,ld");
        p.getValue()
                .writeFieldName("iter");
        p.getValue()
                .writeValue(Arrays.asList(0, 1, 2)
                        .iterator());
        p.getValue()
                .writeFieldName("col");
        p.getValue()
                .writeValue(Arrays.asList(3, 4, 5));
        p.getValue()
                .writeFieldName("map");
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("key", "value");
        m.put("key1", 10.20);
        p.getValue()
                .writeValue(m);
        p.getValue()
                .writeFieldName("reader");
        p.getValue()
                .writeValue(new StringReader("test"));
        p.getValue()
                .writeFieldName("sequence");
        p.getValue()
                .writeValue(new CharSequence()
                {
                    String value = "sequence-value";

                    @Override
                    public CharSequence subSequence(int start, int end)
                    {
                        return null;
                    }

                    @Override
                    public int length()
                    {
                        return value.length();
                    }

                    @Override
                    public char charAt(int index)
                    {
                        return value.charAt(index);
                    }
                });
        p.getValue()
                .endObject();
        p.getValue()
                .endRow();
        p.getValue()
                .close();

        // CSOFF
        assertEquals(
                "{\"col1\":null,\"col2\":true,\"col3\":1,\"col4\":1.1,\"col5\":false,\"col6\":\"hello \\n wor,ld\",\"iter\":[0,1,2],\"col\":[3,4,5],\"map\":{\"key\":\"value\",\"key1\":10.2},\"reader\":\"test\",\"sequence\":\"sequence-value\"}",
                p.getKey()
                        .toString());
        // CSON

        JsonSettings settings = new JsonSettings();
        settings.setResultSetSeparator("new resultSet\n\n\n");
        p = writer(settings);

        p.getValue()
                .startRow();
        p.getValue()
                .startObject();
        p.getValue()
                .writeFieldName("col1");
        p.getValue()
                .writeValue(1);
        p.getValue()
                .writeFieldName("col2");
        p.getValue()
                .writeString(UTF8String.from("value with, comma"));
        p.getValue()
                .endRow();
        p.getValue()
                .startRow();
        p.getValue()
                .writeFieldName("col3");
        p.getValue()
                .writeValue(2);
        p.getValue()
                .writeFieldName("col4");
        p.getValue()
                .writeValue("value with");
        p.getValue()
                .endRow();
        p.getValue()
                .startRow();
        p.getValue()
                .writeFieldName("col5");
        p.getValue()
                .writeValue(12.12);
        p.getValue()
                .writeFieldName("col6");
        p.getValue()
                .writeValue("Hello");
        p.getValue()
                .endObject();
        p.getValue()
                .endRow();
        p.getValue()
                .close();

        assertEquals("{\"col1\":1,\"col2\":\"value with, comma\",\"col3\":2,\"col4\":\"value with\",\"col5\":12.12,\"col6\":\"Hello\"}", p.getKey()
                .toString());
    }

    @Test
    void test_outputstream()
    {
        Pair<ByteArrayOutputStream, JsonOutputWriter> p = outputstream(new JsonSettings());
        p.getValue()
                .writeValue(null);
        p.getValue()
                .close();
        assertEquals("null", p.getKey()
                .toString());

        p = outputstream(new JsonSettings());
        p.getValue()
                .startRow();
        p.getValue()
                .startObject();
        p.getValue()
                .writeFieldName("col1");
        p.getValue()
                .writeValue(null);
        p.getValue()
                .writeFieldName("col2");
        p.getValue()
                .writeValue(true);
        p.getValue()
                .writeFieldName("col3");
        p.getValue()
                .writeValue(1);
        p.getValue()
                .writeFieldName("col4");
        p.getValue()
                .writeValue(1.10f);
        p.getValue()
                .writeFieldName("col5");
        p.getValue()
                .writeValue(false);
        p.getValue()
                .writeFieldName("col6");
        p.getValue()
                .writeString(UTF8String.utf8("h".getBytes(StandardCharsets.UTF_8)));
        p.getValue()
                .writeFieldName("col7");
        p.getValue()
                .writeString(UTF8String.utf8(("hello " + (char) 10 + " wor,ld").getBytes(StandardCharsets.UTF_8)));
        p.getValue()
                .writeFieldName("iter");
        p.getValue()
                .writeValue(Arrays.asList(0, 1, 2)
                        .iterator());
        p.getValue()
                .writeFieldName("col");
        p.getValue()
                .writeValue(Arrays.asList(3, 4, 5));
        p.getValue()
                .writeFieldName("map");
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("key", "value");
        m.put("key1", 10.20);
        p.getValue()
                .writeValue(m);
        p.getValue()
                .writeFieldName("reader");
        p.getValue()
                .writeValue(new StringReader("test"));
        p.getValue()
                .writeFieldName("sequence");
        p.getValue()
                .writeValue(new CharSequence()
                {
                    String value = "sequence-value";

                    @Override
                    public CharSequence subSequence(int start, int end)
                    {
                        return null;
                    }

                    @Override
                    public int length()
                    {
                        return value.length();
                    }

                    @Override
                    public char charAt(int index)
                    {
                        return value.charAt(index);
                    }
                });
        p.getValue()
                .endObject();
        p.getValue()
                .endRow();
        p.getValue()
                .close();

        // CSOFF
        assertEquals(
                "{\"col1\":null,\"col2\":true,\"col3\":1,\"col4\":1.1,\"col5\":false,\"col6\":\"h\",\"col7\":\"hello \\n wor,ld\",\"iter\":[0,1,2],\"col\":[3,4,5],\"map\":{\"key\":\"value\",\"key1\":10.2},\"reader\":\"test\",\"sequence\":\"sequence-value\"}",
                p.getKey()
                        .toString());
        // CSON

        JsonSettings settings = new JsonSettings();
        settings.setResultSetSeparator("new resultSet\n\n\n");
        p = outputstream(settings);

        p.getValue()
                .startRow();
        p.getValue()
                .startObject();
        p.getValue()
                .writeFieldName("col1");
        p.getValue()
                .writeValue(1);
        p.getValue()
                .writeFieldName("col2");
        p.getValue()
                .writeString(UTF8String.from("value with, comma"));
        p.getValue()
                .endRow();
        p.getValue()
                .startRow();
        p.getValue()
                .writeFieldName("col3");
        p.getValue()
                .writeValue(2);
        p.getValue()
                .writeFieldName("col4");
        p.getValue()
                .writeValue("value with");
        p.getValue()
                .endRow();
        p.getValue()
                .startRow();
        p.getValue()
                .writeFieldName("col5");
        p.getValue()
                .writeValue(12.12);
        p.getValue()
                .writeFieldName("col6");
        p.getValue()
                .writeValue("Hello");
        p.getValue()
                .endObject();
        p.getValue()
                .endRow();
        p.getValue()
                .close();

        assertEquals("{\"col1\":1,\"col2\":\"value with, comma\",\"col3\":2,\"col4\":\"value with\",\"col5\":12.12,\"col6\":\"Hello\"}", p.getKey()
                .toString());
    }

    @Test
    void test_settings_from_options()
    {
        JsonSettings settings = JsonSettings.fromOptions(null, List.of());

        settings = JsonSettings.fromOptions(null, List.of(new Option(QualifiedName.of("prettyPRINT"), LiteralBooleanExpression.FALSE)));
        assertFalse(settings.isPrettyPrint());
        settings = JsonSettings.fromOptions(null, List.of(new Option(QualifiedName.of("prettyPrint"), new LiteralNullExpression(ResolvedType.BOOLEAN))));
        assertFalse(settings.isPrettyPrint());
        settings = JsonSettings.fromOptions(null, List.of(new Option(QualifiedName.of("prettyprint"), LiteralBooleanExpression.TRUE)));
        assertTrue(settings.isPrettyPrint());
    }

    private Pair<StringWriter, JsonOutputWriter> writer(JsonSettings settings)
    {
        StringWriter sw = new StringWriter();
        JsonOutputWriter w = new JsonOutputWriter(sw, settings);
        return Pair.of(sw, w);
    }

    private Pair<StringWriter, JsonOutputWriter> writer()
    {
        return writer(new JsonSettings());
    }

    private Pair<ByteArrayOutputStream, JsonOutputWriter> outputstream(JsonSettings settings)
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonOutputWriter w = new JsonOutputWriter(baos, settings);
        return Pair.of(baos, w);
    }

}
