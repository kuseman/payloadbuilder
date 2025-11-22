package se.kuseman.payloadbuilder.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.StringWriter;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

/** Test of {@link PlainTextOutputWriter} */
class PlainTextOutputWriterTest
{
    @Test
    void test_delayed_headers()
    {
        Pair<StringWriter, PlainTextOutputWriter> p = writer();

        p.getValue()
                .initResult(null);
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
                .endObject();
        p.getValue()
                .endRow();
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
                .writeValue(false);
        p.getValue()
                .writeFieldName("col3");
        p.getValue()
                .writeValue(2);
        p.getValue()
                .writeFieldName("col4");
        p.getValue()
                .writeValue(2.20f);
        p.getValue()
                .writeFieldName("col5");
        p.getValue()
                .writeValue(true);
        p.getValue()
                .writeFieldName("col6");
        p.getValue()
                .writeValue("some new " + (char) 10 + " sti,ng");
        p.getValue()
                .endObject();
        p.getValue()
                .endRow();

        p.getValue()
                .close();

        assertEquals("col1 col2 col3 col4 col5 col6" + "\r\n" + "null true 1 1.1 false hello \n wor,ld" + "\r\n" + "null false 2 2.2 true some new \n sti,ng" + "\r\n", p.getKey()
                .toString());
    }

    @Test
    void test()
    {
        Pair<StringWriter, PlainTextOutputWriter> p = writer();
        p.getValue()
                .writeValue(null);
        p.getValue()
                .close();
        assertEquals("null", p.getKey()
                .toString());

        p = writer();
        p.getValue()
                .writeValue(null);
        p.getValue()
                .writeValue(true);
        p.getValue()
                .writeValue(1);
        p.getValue()
                .writeValue(1.10f);
        p.getValue()
                .writeValue(false);
        p.getValue()
                .writeValue("hello " + (char) 10 + " wor,ld");

        p.getValue()
                .close();
        assertEquals("null true 1 1.1 false hello \n wor,ld", p.getKey()
                .toString());

        p = writer();

        p.getValue()
                .initResult(new String[] { "col1", "col2" });
        p.getValue()
                .startRow();
        p.getValue()
                .writeValue(1);
        p.getValue()
                .writeValue("value with, comma");
        p.getValue()
                .endRow();
        p.getValue()
                .startRow();
        p.getValue()
                .writeValue(2);
        p.getValue()
                .writeValue("value with");
        p.getValue()
                .endRow();
        p.getValue()
                .initResult(new String[] { "col3", "col4" });
        p.getValue()
                .startRow();
        p.getValue()
                .writeValue(12.12);
        p.getValue()
                .writeValue("Hello");
        p.getValue()
                .endRow();
        p.getValue()
                .close();

        assertEquals("col1 col2" + "\r\n" + "1 value with, comma" + "\r\n" + "2 value with" + "\r\n" + "\r\n" + "col3 col4" + "\r\n" + "12.12 Hello" + "\r\n", p.getKey()
                .toString());
    }

    private Pair<StringWriter, PlainTextOutputWriter> writer()
    {
        StringWriter sw = new StringWriter();
        PlainTextOutputWriter w = new PlainTextOutputWriter(sw);
        return Pair.of(sw, w);
    }
}
