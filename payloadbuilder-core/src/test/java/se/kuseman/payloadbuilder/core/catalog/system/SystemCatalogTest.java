package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import java.io.StringReader;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;

import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.operator.Operator.TupleIterator;
import se.kuseman.payloadbuilder.api.operator.Operator.TupleList;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.api.utils.MapUtils;
import se.kuseman.payloadbuilder.core.parser.AParserTest;

/** Tests functions etc. in built in catalog */
public class SystemCatalogTest extends AParserTest
{
    @Test
    public void test_function_hash() throws Exception
    {
        assertExpression(null, null, "hash(null)");
        assertExpression(null, null, "hash(null,true)");
        assertExpression(1262, null, "hash(true)");
        assertExpression(1115, null, "hash(1,123)");
    }

    @Test
    public void test_function_coalesce() throws Exception
    {
        assertExpression(10, null, "coalesce(null, null, 10)");
        assertExpression("str", null, "coalesce('str', null, 10)");
    }

    @Test
    public void test_randomInt() throws Exception
    {
        assertExpression(true, null, "randomInt(10) + 1 > 0");
        assertExpression(null, null, "randomInt(null) + 1 > 0");
    }

    @Test
    public void test_function_filter() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("a", asList(-1, -2, -3, 0, 1, 2, 3));
        values.put("b", null);

        assertExpression(asList(1, 2, 3), values, "a.filter(a -> a > 0)");
        assertExpression(null, values, "b.filter(a -> a > 0)");
        assertExpression(asList(2), values, "filter(a.filter(a -> a > 0), a -> a = 2)");
        assertExpression(asList("1s", "2s", "3s"), values, "map(a.filter(a -> a > 0), a -> a + 's')");
    }

    @Test
    public void test_function_concat() throws Exception
    {
        assertExpression("110.1", null, "concat(null,1,10.1)");
        assertExpression("", null, "concat(null,null)");
    }

    @Test
    public void test_function_count() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("a", asList(-1, -2, -3, 0, 1, 2, 3));
        values.put("b", asList(-1, -2, -3, 0, 1, 2, 3).iterator());
        values.put("c", MapUtils.ofEntries(MapUtils.entry("key", "value"), MapUtils.entry("key1", "value1")));
        values.put("d", new TupleList()
        {
            @Override
            public int size()
            {
                return 3;
            }

            @Override
            public Tuple get(int index)
            {
                return Mockito.mock(Tuple.class);
            }
        });
        values.put("e", new TupleIterator()
        {
            int index;

            @Override
            public boolean hasNext()
            {
                return index < 5;
            }

            @Override
            public Tuple next()
            {
                index++;
                return Mockito.mock(Tuple.class);
            }
        });

        assertExpression(7, values, "count(a)");
        assertExpression(7, values, "count(b)");
        assertExpression(2, values, "count(c)");
        assertExpression(1, values, "count(1)");
        assertExpression(0, values, "count(null)");
        assertExpression(3, values, "count(d)");
        assertExpression(5, values, "count(e)");
    }

    @Test
    public void test_function_unionall() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("a", asList(-1, -2, -3, 0, 1, 2, 3));
        values.put("b", null);

        assertExpression(asList(-1, -2, -3, 0, 1, 2, 3, 1, 2, 3), values, "a.unionall(a.filter(x -> x > 0))");
        assertExpression(asList(-1, -2, -3, 1, 2, 3), values, "unionall(a.filter(x -> x < 0), a.filter(x -> x > 0))");
    }

    @Test
    public void test_function_map() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("a", asList(-1, -2, -3, 0, 1, 2, 3));
        values.put("b", null);

        assertExpression(null, values, "b.map(a -> a * 2)");
        assertExpression(asList(-2, -4, -6, 0, 2, 4, 6), values, "a.map(a -> a * 2)");
        assertExpression(asList(-1, -2, -3, 0, 1, 2, 3), values, "map(a.map(a -> a * 2), a -> a / 2)");
    }

    @Test
    public void test_function_contains() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("a", asList(-1, -2, -3, 0, 1, 2, 3));
        values.put("b", null);
        values.put("c", asList(-1, -2, -3, 0, 1, 2, 3).iterator());

        assertExpression(false, values, "contains(null, 1)");
        assertExpression(false, values, "contains(a, 10)");
        assertExpression(true, values, "contains(a, -2)");
        assertExpression(false, values, "contains(c, 10)");

        values.put("c", asList(-1, -2, -3, 0, 1, 2, 3).iterator());

        assertExpression(true, values, "contains(c, -2)");
    }

    @Test
    public void test_isCodegenSupported()
    {
        assertTrue(((ScalarFunctionInfo) session.getCatalogRegistry()
                .getSystemCatalog()
                .getFunction("contains")).isCodeGenSupported(asList(e("10"), e("20"))));
    }

    @Test
    public void test_function_match() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("a", asList());
        values.put("b", null);
        values.put("c", asList(1, 2));

        // Empty
        assertExpression(false, values, "a.any(x -> x > 0)");
        assertExpression(true, values, "a.all(x -> x > 0)");
        assertExpression(true, values, "a.none(x -> x > 0)");

        // Null
        assertExpression(null, values, "b.any(x -> x > 0)");
        assertExpression(null, values, "b.all(x -> x > 0)");
        assertExpression(null, values, "b.none(x -> x > 0)");

        // Values
        assertExpression(true, values, "c.any(x -> x > 0)");
        assertExpression(true, values, "c.all(x -> x > 0)");
        assertExpression(false, values, "c.none(x -> x > 0)");

        assertExpression(false, values, "c.any(x -> x < 0)");
        assertExpression(false, values, "c.all(x -> x < 0)");
        assertExpression(true, values, "c.none(x -> x < 0)");

        assertExpressionFail(IllegalArgumentException.class, "Expected boolean result but got: 2", values, "c.any(x -> x+1)");
    }

    @Test
    public void test_function_flatMap() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("a", asList(-1, -2, 3, null));
        values.put("b", null);

        assertExpression(asList(-1, -2, 3), values, "a.flatMap(a -> a)");
        assertExpression(null, values, "b.flatMap(a -> a)");
        assertExpression(asList(-1, -2, 3, null, -1, -2, 3, null, -1, -2, 3, null, -1, -2, 3, null), values, "a.flatMap(l -> a)");
        assertExpression(asList(-1, -2, 3, -1, -2, 3, -1, -2, 3, -1, -2, 3), values, "a.flatMap(l -> a).filter(a -> a IS NOT NULL)");
        assertExpression(asList(null, null, null, null), values, "a.flatMap(l -> a).filter(a -> a IS NULL)");
    }

    @Test
    public void test_function_json_value() throws Exception
    {
        assertExpression(null, null, "json_value(null)");
        assertExpression(emptyMap(), null, "json_value('{}')");
        assertExpression(emptyList(), null, "json_value('[]')");
    }

    @Test
    public void test_function_isnull() throws Exception
    {
        assertExpression(null, null, "isnull(null, null)");
        assertExpression(1, null, "isnull(null, 1)");
        assertExpression(1, null, "isnull(1, null)");
    }

    @Test
    public void test_function_isblank() throws Exception
    {
        assertExpression(1, null, "isblank(null, 1)");
        assertExpression(1, null, "isblank('', 1)");
        assertExpression("str", null, "isblank('str', null)");
    }

    @Test
    public void test_function_cast() throws Exception
    {
        assertExpression(null, null, "cast(null, integer)");
        assertExpression(null, null, "cast(1, null)");

        assertExpression(1, null, "cast(1, 'integer')");
        assertExpression(1L, null, "cast(1, 'long')");
        assertExpression(1.0f, null, "cast(1, 'float')");
        assertExpression(1.0d, null, "cast(1, 'double')");

        assertExpression(1, null, "cast('1', 'integer')");
        assertExpression(1L, null, "cast('1', 'long')");
        assertExpression(1.0f, null, "cast('1', 'float')");
        assertExpression(1.0d, null, "cast('1', 'double')");

        assertExpression("1.0", null, "cast(1.0, 'string')");

        assertExpression(true, null, "cast(1, 'boolean')");
        assertExpression(true, null, "cast(1.20, 'boolean')");
        assertExpression(false, null, "cast(0, 'boolean')");
        assertExpression(true, null, "cast('TRUE', 'boolean')");
        assertExpression(false, null, "cast('false', 'boolean')");
        assertExpression(false, null, "cast('hello', 'boolean')");

        assertExpression(LocalDateTime.parse("2000-10-10T00:00:00"), null, "cast('2000-10-10', 'datetime')");
        assertExpression(LocalDateTime.parse("2000-10-10T10:10:00"), null, "cast('2000-10-10 10:10', 'datetime')");
        assertExpression(ZonedDateTime.parse("2000-10-10T10:10Z")
                .withZoneSameInstant(ZoneId.systemDefault())
                .toLocalDateTime(), null, "cast('2000-10-10 10:10Z', 'datetime')");
        assertExpression(ZonedDateTime.parse("2000-10-10T10:10:10.123Z")
                .withZoneSameInstant(ZoneId.systemDefault())
                .toLocalDateTime(), null, "cast('2000-10-10 10:10:10.123Z', 'datetime')");

        // 2022-04-20T08:42:58.803381412+00:00
        assertExpression(ZonedDateTime.parse("2022-04-20T08:42:58.803381412+00:00")
                .withZoneSameInstant(ZoneId.systemDefault())
                .toLocalDateTime(), null, "cast('2022-04-20T08:42:58.803381412+00:00', 'datetime')");

        assertExpressionFail(DateTimeParseException.class, "Text 'jibberish' could not", null, "cast('jibberish', 'datetime')");
        assertExpressionFail(IllegalArgumentException.class, "Cannot cast", null, "cast(true, 'integer')");
        assertExpressionFail(IllegalArgumentException.class, "Cannot cast", null, "cast(true, 'long')");
        assertExpressionFail(IllegalArgumentException.class, "Cannot cast", null, "cast(true, 'float')");
        assertExpressionFail(IllegalArgumentException.class, "Cannot cast", null, "cast(true, 'double')");
    }

    @Test
    public void test_function_replace() throws Exception
    {
        assertExpression(null, null, "replace('hello xxx', null, 'world')");
        assertExpression(null, null, "replace('hello xxx', null, null)");
        assertExpression(null, null, "replace(null, 'xxx', null)");
        assertExpression(null, null, "replace(null, 'xxx', 'world')");
        assertExpression("hello world", null, "replace('hello xxx', 'xxx', 'world')");
        assertExpression("hello xxx", null, "replace('hello xxx', 'yyy', 'world')");

        // Test reader replacement

        Map<String, Object> values = new HashMap<>();
        values.put("a", new StringReader("hello xxx"));

        assertExpression("hello world", values, "replace(a, 'xxx', 'world')");
        values.put("a", new StringReader("hello xxx"));
        assertExpression("hello ", values, "replace(a, 'xxx', '')");
        values.put("a", new StringReader("hello xxx"));
        assertExpression("hello xxx", values, "replace(a, 'yyy', '')");
        values.put("a", new StringReader("hello xxx"));
        assertExpression("hello xxx", values, "replace(a, 'xyy', '')");
        values.put("a", new StringReader("hello xxx"));
        assertExpression("åäöello xxx", values, "replace(a, 'h', 'åäö')");
        values.put("a", new StringReader("hello xyz"));
        assertExpression("hello xyåäö", values, "replace(a, 'z', 'åäö')");
    }
}
