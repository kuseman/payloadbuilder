package se.kuseman.payloadbuilder.core;

import java.io.StringWriter;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import se.kuseman.payloadbuilder.core.CsvOutputWriter.CsvSettings;

/** Test of {@link CsvOutputWriter} */
public class CsvOutputWriterTest extends Assert
{
    @Test
    public void test_delayed_headers()
    {
        Pair<StringWriter, CsvOutputWriter> p = writer();

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

        assertEquals("col1,col2,col3,col4,col5,col6" + "\n" + "null,true,1,1.1,false,hello \\n wor\\,ld" + "\n" + "null,false,2,2.2,true,some new \\n sti\\,ng" + "\n", p.getKey()
                .toString());
    }

    @Test
    public void test()
    {
        Pair<StringWriter, CsvOutputWriter> p = writer();
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
        assertEquals("null,true,1,1.1,false,hello \\n wor\\,ld", p.getKey()
                .toString());

        CsvSettings settings = new CsvSettings();
        settings.setResultSetSeparator("new resultSet\n\n\n");
        p = writer(settings);

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

        assertEquals("col1,col2" + "\n" + "1,value with\\, comma" + "\n" + "2,value with" + "\n" + "new resultSet" + "\n\n\n" + "col3,col4" + "\n" + "12.12,Hello" + "\n", p.getKey()
                .toString());
    }

    private Pair<StringWriter, CsvOutputWriter> writer(CsvSettings settings)
    {
        StringWriter sw = new StringWriter();
        CsvOutputWriter w = new CsvOutputWriter(sw, settings);
        return Pair.of(sw, w);
    }

    private Pair<StringWriter, CsvOutputWriter> writer()
    {
        return writer(new CsvSettings());
    }
}
