package org.kuse.payloadbuilder.core;

import java.io.StringWriter;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/** Test of {@link CsvOutputWriter} */
public class CsvOutputWriterTest extends Assert
{
    @Test
    public void test()
    {
        Pair<StringWriter, CsvOutputWriter> p = writer();
        p.getValue().writeValue(null);
        p.getValue().close();
        assertEquals("null", p.getKey().toString());

        p = writer();
        p.getValue().writeValue(null);
        p.getValue().writeValue(true);
        p.getValue().writeValue(1);
        p.getValue().writeValue(1.10f);
        p.getValue().writeValue(false);
        p.getValue().writeValue("hello " + (char) 10 + " wor,ld");

        p.getValue().close();
        assertEquals("null,true,1,1.1,false,hello \\n wor\\,ld", p.getKey().toString());
    }

    private Pair<StringWriter, CsvOutputWriter> writer()
    {
        StringWriter sw = new StringWriter();
        CsvOutputWriter w = new CsvOutputWriter(sw);
        return Pair.of(sw, w);
    }
}
