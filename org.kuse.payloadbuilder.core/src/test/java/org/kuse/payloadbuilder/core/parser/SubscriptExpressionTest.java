package org.kuse.payloadbuilder.core.parser;

import static java.util.Arrays.asList;

import java.util.stream.IntStream;

import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.utils.MapUtils;

/** Test {@link SubscriptExpression} */
public class SubscriptExpressionTest extends AParserTest
{
    TableAlias t = TableAlias.of(null, QualifiedName.of("table"), "t", new String[] {"a", "b", "c", "d"});
    @Test
    public void test()
    {
        Row row = Row.of(t, 0, new Object[] {asList(1, 2, 3), IntStream.range(1, 4).iterator(), new int[] {1, 2, 3}, MapUtils.ofEntries(MapUtils.entry("key", "value"))});
        context.setRow(row);

        assertNull(e("null[10]").eval(context));
        assertNull(e("a[null]").eval(context));

        assertEquals(2, e("a[1]").eval(context));
        assertNull(e("a[-1]").eval(context));
        assertNull(e("a[10]").eval(context));

        assertEquals(2, e("b[1]").eval(context));
        assertNull(e("b[-1]").eval(context));
        assertNull(e("b[10]").eval(context));

        assertEquals(2, e("c[1]").eval(context));
        assertNull(e("c[-1]").eval(context));
        assertNull(e("c[10]").eval(context));

        assertEquals("value", e("d['key']").eval(context));
        assertNull(e("d['key2']").eval(context));
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_invalid_value()
    {
        e("'test'[10]").eval(context);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_invalid_subscript_int()
    {
        Row row = Row.of(t, 0, new Object[] {asList(1, 2, 3), IntStream.range(1, 4).iterator(), new int[] {1, 2, 3}, MapUtils.ofEntries(MapUtils.entry("key", "value"))});
        context.setRow(row);
        e("a['string']").eval(context);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_invalid_subscript_string()
    {
        Row row = Row.of(t, 0, new Object[] {asList(1, 2, 3), IntStream.range(1, 4).iterator(), new int[] {1, 2, 3}, MapUtils.ofEntries(MapUtils.entry("key", "value"))});
        context.setRow(row);
        e("d[123]").eval(context);
    }
}
