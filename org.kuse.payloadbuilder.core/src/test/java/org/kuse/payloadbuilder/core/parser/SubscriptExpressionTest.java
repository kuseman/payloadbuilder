package org.kuse.payloadbuilder.core.parser;

import static java.util.Arrays.asList;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.Test;
import org.kuse.payloadbuilder.core.utils.MapUtils;

/** Test {@link SubscriptExpression} */
public class SubscriptExpressionTest extends AParserTest
{
    @Test
    public void test() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("a", asList(1, 2, 3));
        values.put("b", IntStream.range(1, 4).iterator());
        values.put("c", new int[] {1, 2, 3});
        values.put("d", MapUtils.ofEntries(MapUtils.entry("key", "value")));

        assertExpression(null, values, "null[10]");
        assertExpression(null, values, "a[null]");

        assertExpression(2, values, "a[1]");
        assertExpression(null, values, "a[-1]");
        assertExpression(null, values, "a[10]");

        assertExpression(2, values, "b[1]");
        assertExpression(null, values, "b[-1]");
        assertExpression(null, values, "b[10]");

        assertExpression(2, values, "c[1]");
        assertExpression(null, values, "c[-1]");
        assertExpression(null, values, "c[10]");

        assertExpression("value", values, "d['key']");
        assertExpression(null, values, "d['key2']");

        assertExpressionFail(IllegalArgumentException.class, "Cannot subscript value: test", values, "'test'[10]");
        assertExpressionFail(IllegalArgumentException.class, "Expected an integer subscript but got string", values, "a['string']");
        assertExpressionFail(IllegalArgumentException.class, "Expected a string subscript but got 123", values, "d[123]");
    }
}
