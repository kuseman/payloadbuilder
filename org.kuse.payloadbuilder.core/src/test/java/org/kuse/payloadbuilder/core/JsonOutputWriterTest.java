package org.kuse.payloadbuilder.core;

import java.io.StringWriter;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.kuse.payloadbuilder.core.JsonOutputWriter.JsonSettings;

/** Test of {@link JsonOutputWriter} */
public class JsonOutputWriterTest extends Assert
{
    @Test
    public void test()
    {
        Pair<StringWriter, JsonOutputWriter> p = writer();
        p.getValue().writeValue(null);
        p.getValue().close();
        assertEquals("null", p.getKey().toString());

        p = writer();
        p.getValue().startRow();
        p.getValue().startObject();
        p.getValue().writeFieldName("col1");
        p.getValue().writeValue(null);
        p.getValue().writeFieldName("col2");
        p.getValue().writeValue(true);
        p.getValue().writeFieldName("col3");
        p.getValue().writeValue(1);
        p.getValue().writeFieldName("col4");
        p.getValue().writeValue(1.10f);
        p.getValue().writeFieldName("col5");
        p.getValue().writeValue(false);
        p.getValue().writeFieldName("col6");
        p.getValue().writeValue("hello " + (char) 10 + " wor,ld");
        p.getValue().endObject();
        p.getValue().endRow();
        p.getValue().close();

        assertEquals("{\"col1\":null,\"col2\":true,\"col3\":1,\"col4\":1.1,\"col5\":false,\"col6\":\"hello \\n wor,ld\"}", p.getKey().toString());

        JsonSettings settings = new JsonSettings();
        settings.setResultSetSeparator("new resultSet\n\n\n");
        p = writer(settings);

        p.getValue().startRow();
        p.getValue().startObject();
        p.getValue().writeFieldName("col1");
        p.getValue().writeValue(1);
        p.getValue().writeFieldName("col2");
        p.getValue().writeValue("value with, comma");
        p.getValue().endRow();
        p.getValue().startRow();
        p.getValue().writeFieldName("col3");
        p.getValue().writeValue(2);
        p.getValue().writeFieldName("col4");
        p.getValue().writeValue("value with");
        p.getValue().endRow();
        p.getValue().startRow();
        p.getValue().writeFieldName("col5");
        p.getValue().writeValue(12.12);
        p.getValue().writeFieldName("col6");
        p.getValue().writeValue("Hello");
        p.getValue().endObject();
        p.getValue().endRow();
        p.getValue().close();

        assertEquals("{\"col1\":1,\"col2\":\"value with, comma\",\"col3\":2,\"col4\":\"value with\",\"col5\":12.12,\"col6\":\"Hello\"}", p.getKey().toString());
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
}
