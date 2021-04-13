package org.kuse.payloadbuilder.core.parser;

import static java.util.Arrays.asList;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.Tuple;

/** Test {@link DereferenceExpression} */
public class DereferenceExpressionTest extends AParserTest
{
    @Test
    public void test_dereference_map() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("a", asList(
                ofEntries(entry("id", -1), entry("c", -10)),
                ofEntries(entry("id", 0), entry("c", 0)),
                ofEntries(entry("id", 1), entry("c", 10), entry("d", ofEntries(entry("key", "value")))),
                ofEntries(entry("id", 2), entry("c", 20))));

        assertExpression(null, values, "a.filter(b -> b.id > 0)[10].d");
        assertExpression(10, values, "a.filter(b -> b.id > 0)[0].c");
        assertExpression("value", values, "a.filter(b -> b.id > 0)[0].d.key");
        assertExpressionFail(IllegalArgumentException.class, "Cannot dereference String value: value", values, "a.filter(b -> b.id > 0)[0].d.key.missing");
    }

    @Test
    public void test_dereference_tuple() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("a", asList(new Tuple()
        {
            @Override
            public int getTupleOrdinal()
            {
                return 0;
            }

            @Override
            public int getColumnCount()
            {
                return 1;
            }

            @Override
            public String getColumn(int ordinal)
            {
                return "elite";
            }

            @Override
            public int getColmnOrdinal(String column)
            {
                if ("elite".equals(column))
                {
                    return 1337;
                }
                return -1;
            }

            @Override
            public Object getValue(int ordinal)
            {
                if (ordinal == 1337)
                {
                    return ordinal;
                }
                return null;
            }

            @Override
            public Tuple getTuple(int ordinal)
            {
                return null;
            }
        }));

        assertExpression(1337, values, "a.map(x -> x)[0].elite");
    }
}
