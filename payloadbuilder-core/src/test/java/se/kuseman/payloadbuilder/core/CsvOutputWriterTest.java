package se.kuseman.payloadbuilder.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.StringWriter;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.core.CsvOutputWriter.CsvSettings;
import se.kuseman.payloadbuilder.core.expression.LiteralNullExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;

/** Test of {@link CsvOutputWriter} */
class CsvOutputWriterTest
{
    @Test
    void test_delayed_headers()
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
    void test()
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

    @Test
    void test_empty_columns_dont_yield_initial_new_line()
    {
        CsvSettings settings = new CsvSettings();
        settings.setRowSeparator(System.lineSeparator());
        Pair<StringWriter, CsvOutputWriter> p = writer(settings);
        p.getValue()
                .initResult(new String[] { "" });
        p.getValue()
                .endResult();
        p.getValue()
                .close();
        assertEquals("", p.getKey()
                .toString());
    }

    @Test
    void test_settings_from_options()
    {
        CsvSettings settings = CsvSettings.fromOptions(null, List.of());

        settings = CsvSettings.fromOptions(null, List.of(new Option(QualifiedName.of("separatorchar"), new LiteralStringExpression(""))));
        assertEquals(',', settings.getSeparatorChar());
        settings = CsvSettings.fromOptions(null, List.of(new Option(QualifiedName.of("separatorchar"), new LiteralNullExpression(ResolvedType.STRING))));
        assertEquals(',', settings.getSeparatorChar());
        settings = CsvSettings.fromOptions(null, List.of(new Option(QualifiedName.of("separatorchar"), new LiteralStringExpression(";"))));
        assertEquals(';', settings.getSeparatorChar());
        settings = CsvSettings.fromOptions(null, List.of(new Option(QualifiedName.of("columnseparator"), new LiteralStringExpression("%"))));
        assertEquals('%', settings.getSeparatorChar());

        settings = CsvSettings.fromOptions(null, List.of(new Option(QualifiedName.of("escapechar"), new LiteralStringExpression(""))));
        assertEquals('\\', settings.getEscapeChar());
        settings = CsvSettings.fromOptions(null, List.of(new Option(QualifiedName.of("escapechar"), new LiteralNullExpression(ResolvedType.STRING))));
        assertEquals('\\', settings.getEscapeChar());
        settings = CsvSettings.fromOptions(null, List.of(new Option(QualifiedName.of("escapechar"), new LiteralStringExpression(";"))));
        assertEquals(';', settings.getEscapeChar());
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
