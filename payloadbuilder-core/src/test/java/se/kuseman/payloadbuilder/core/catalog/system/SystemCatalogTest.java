package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.utils.MapUtils;
import se.kuseman.payloadbuilder.core.catalog.CatalogRegistry;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.expression.AExpressionTest;

/** Tests functions etc. in built in catalog */
public class SystemCatalogTest extends AExpressionTest
{
    @Test
    public void test_parseduration() throws Exception
    {
        assertExpression(null, null, "parseduration(null)");
        assertExpression(10_000L, null, "parseduration('00:00:10')");
        assertExpression(100L, null, "parseduration('00:00:00.100')");
        assertExpression(86400000L, null, "parseduration('24:00:00')");
        assertExpression(86400000L * 2, null, "parseduration('48:00:00')");

        assertExpression(10_000L, null, "parseduration('PT10S')");
        assertExpression(86400000L, null, "parseduration('P1D')");
    }

    @Test
    public void test_parsedatasize() throws Exception
    {
        assertExpression(null, null, "parsedatasize(null)");
        assertExpression(10L, null, "parsedatasize('10b')");
        assertExpression(10_240L, null, "parsedatasize('10kb')");
        assertExpression(-11010048L, null, "parsedatasize('-10.5MB')");
        assertExpression(-1181116032L, null, "parsedatasize('-1.1gB')");
        assertExpression(2418925633536L, null, "parsedatasize('2.2tb')");

        try
        {
            assertExpression(null, null, "parsedatasize('1.mb')", true);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Could not parse data size: 1.mb"));
        }
        try
        {
            assertExpression(null, null, "parsedatasize('1.2hb')", true);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Unkown data size unit: hb"));
        }
    }

    @Test
    public void test_function_hash() throws Exception
    {
        assertExpression(null, null, "hash(null)");
        assertExpression(null, null, "hash(null,true)");
        assertExpression(629, null, "hash(true)");
        assertExpression(23433, null, "hash(1,123)");
    }

    @Test
    public void test_base64() throws Exception
    {
        assertExpression(null, null, "base64_decode(null)");
        assertExpression(null, null, "base64_encode(null)");
        assertExpression("aGVsbG8gd29ybGQ=", null, "base64_encode('hello world')");
        assertExpression(new byte[] { 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100 }, null, "base64_decode('aGVsbG8gd29ybGQ=')");
        assertExpression("hello world", null, "cast(base64_decode('aGVsbG8gd29ybGQ=') as string)");
        assertExpression("hello world", null, "cast(base64_decode(base64_encode('hello world')) as string)");
    }

    @Test
    public void test_reverse() throws Exception
    {
        assertExpression(null, null, "reverse(null)");
        assertExpression("olleh", null, "reverse('hello')");
    }

    @Test
    public void test_current_timezone() throws Exception
    {
        TimeZone timeZone = TimeZone.getDefault();
        try
        {
            TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
            assertExpression("America/Los_Angeles", null, "current_timezone()");
            assertExpression("2020-10-10T07:10:10-07:00", null, "cast('2020-10-10T10:10:10-04:00' as datetimeoffset) at time zone current_timezone()");
        }
        finally
        {
            TimeZone.setDefault(timeZone);
        }
    }

    @Test
    public void test_abs() throws Exception
    {
        assertExpression(null, null, "abs(null)");
        assertExpression(1, null, "abs(-1)");
        assertExpression(1L, null, "abs(-1L)");
        assertExpression(1F, null, "abs(-1F)");
        assertExpression(1D, null, "abs(-1D)");
        assertExpression(Decimal.from(1D), null, "abs(cast(-1D as decimal))");

        Map<String, Object> params = new HashMap<>();
        params.put("a", -123);
        assertExpression(123, params, "abs(@a)");

        try
        {
            assertExpression(null, null, "abs(true)", true);
            fail("Should fail because of invalid data type");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Function abs does not support type: Boolean"));
        }
    }

    @Test
    public void test_ceiling() throws Exception
    {
        assertExpression(null, null, "ceiling(null)");
        assertExpression(1, null, "ceiling(1)");
        assertExpression(1L, null, "ceiling(1L)");
        assertExpression(2F, null, "ceiling(1.5F)");
        assertExpression(3D, null, "ceiling(2.5D)");
        assertExpression(Decimal.from("4"), null, "ceiling(cast(3.5D as decimal))");

        Map<String, Object> params = new HashMap<>();
        params.put("a", 123.5);
        assertExpression(124.0, params, "ceiling(@a)");

        try
        {
            assertExpression(null, null, "ceiling(true)", true);
            fail("Should fail because of invalid data type");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Function ceiling does not support type: Boolean"));
        }
    }

    @Test
    public void test_floor() throws Exception
    {
        assertExpression(null, null, "floor(null)");
        assertExpression(1, null, "floor(1)");
        assertExpression(1L, null, "floor(1L)");
        assertExpression(1F, null, "floor(1.5F)");
        assertExpression(2D, null, "floor(2.5D)");
        assertExpression(Decimal.from("3"), null, "floor(cast(3.5D as decimal))");

        Map<String, Object> params = new HashMap<>();
        params.put("a", 123.5);
        assertExpression(123.0, params, "floor(@a)");

        try
        {
            assertExpression(null, null, "floor(true)", true);
            fail("Should fail because of invalid data type");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Function floor does not support type: Boolean"));
        }
    }

    @Test
    public void test_charindex() throws Exception
    {
        assertExpression(null, null, "charindex(null, null)");
        assertExpression(null, null, "charindex('hello', null)");
        assertExpression(null, null, "charindex(null, 'hello')");
        assertExpression(1L, null, "charindex('ell', 'hello')");
        assertExpression(2L, null, "charindex('l', 'hello')");
        assertExpression(3L, null, "charindex('l','hello', 3)");
        assertExpression(-1L, null, "charindex('l', 'hello', 4)");
    }

    @Test
    public void test_left_right() throws Exception
    {
        assertExpression(null, null, "right(null, null)");
        assertExpression(null, null, "right(null, 12)");
        assertExpression(null, null, "right('hello', null)");
        assertExpression("llo", null, "right('hello', 3)");

        assertExpression(null, null, "left(null, null)");
        assertExpression(null, null, "left(null, 12)");
        assertExpression(null, null, "left('hello', null)");
        assertExpression("hel", null, "left('hello', 3)");

        try
        {
            assertExpression("hel", null, "left('hello', -3)", true);
            fail("Expression should fail because of negative size");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Function left expects a positive integer value for argument 2"));
        }
    }

    @Test
    public void test_format() throws Exception
    {
        Locale current = Locale.getDefault();
        try
        {
            Locale.setDefault(Locale.US);
            assertExpression(null, null, "format(null, '###.##E0')");
            assertExpression(null, null, "format(100000000, null)");
            assertExpression(null, null, "format(100000000, null, null)");
            assertExpression(null, null, "format(null, null, 'de')");
            assertExpression(null, null, "format(null, null, null)");
            assertExpression(null, null, "format(100000000, '###.##E0', null)");

            Map<String, Object> params = new HashMap<>();
            params.put("a", 100000000);
            params.put("fmt", "###.###");
            params.put("loc", "se");

            // Decimal format
            assertExpression("100E6", params, "format(@a, '###.##E0')");
            assertExpression("100E6", null, "format(100000000, '###.##E0')");
            assertExpression("100E6", null, "format(100000000L, '###.##E0')");
            assertExpression("100E6", null, "format(100000000F, '###.##E0')");
            assertExpression("100E6", null, "format(100000000D, '###.##E0')");
            assertExpression("123.123", null, "format(cast('123.123456' as Decimal), '###.###')");
            assertExpression("123.123", params, "format(cast('123.123456' as Decimal), @fmt)");

            // Decimal format locale
            assertExpression("123,123", null, "format(cast('123.123456' as Decimal), '###.###', 'se')");
            assertExpression("123,123", params, "format(cast('123.123456' as Decimal), '###.###', @loc)");

            params = new HashMap<>();
            params.put("a", "2020-10-10 10:11:12");
            params.put("fmt", "YYYY-MM");
            params.put("loc", "us");
            params.put("b", EpochDateTime.from("2020-10-10 10:11:12"));
            params.put("c", EpochDateTimeOffset.from("2020-10-10 10:11:12"));
            params.put("d", LocalDateTime.parse("2020-10-10T10:11:12"));

            // DateTime format
            assertExpression("2020-10-10", params, "format(cast('2020-10-10 10:11:12' as datetime), 'YYYY-MM-dd')");
            assertExpression("2020-10-10", params, "format(cast('2020-10-10 10:11:12' as datetime), 'YYYY-MM-dd', 'se')");
            assertExpression("2020-10-10", params, "format(cast(@a as datetime), 'YYYY-MM-dd')");
            assertExpression("2020-10-10", params, "format(cast(@a as datetime), 'YYYY-MM-dd', 'se')");
            assertExpression("2020-10-10", params, "format(cast(@a as datetime), 'YYYY-MM-dd', @loc)");
            assertExpression("2020-10", params, "format(cast(@a as datetime), @fmt, @loc)");
            assertExpression("2020-10", params, "format(cast(@a as datetime), @fmt)");

            // DateTimeOffset format
            assertExpression("2020-10-10", params, "format(cast('2020-10-10 10:11:12' as DateTimeOffset), 'YYYY-MM-dd')");
            assertExpression("2020-10-10", params, "format(cast('2020-10-10 10:11:12' as DateTimeOffset), 'YYYY-MM-dd', 'se')");
            assertExpression("2020-10-10+0000", params, "format(cast(@a as DateTimeOffset), 'YYYY-MM-ddZ')");
            assertExpression("2020-10-10+0000", params, "format(cast(@a as DateTimeOffset), 'YYYY-MM-ddZ', 'se')");
            assertExpression("2020-10-10+0000", params, "format(cast(@a as DateTimeOffset), 'YYYY-MM-ddZ', @loc)");
            assertExpression("2020-10", params, "format(cast(@a as DateTimeOffset), @fmt, @loc)");
            assertExpression("2020-10", params, "format(cast(@a as DateTimeOffset), @fmt)");

            // DateTime/Offset with any
            assertExpression("2020-10-10", params, "format(@b, 'YYYY-MM-dd')");
            assertExpression("2020-10-10", params, "format(@c, 'YYYY-MM-dd', 'se')");
            assertExpression("2020-10-10", params, "format(@d, 'YYYY-MM-dd', 'se')");

            // Misc
            assertExpression("Some true value", params, "format(true, 'Some %s value')");
            assertExpression("Some true value", params, "format(true, 'Some %s value', 'se')");
        }
        finally
        {
            Locale.setDefault(current);
        }
    }

    @Test
    public void test_dereference() throws Exception
    {
        assertExpression(null, null, "(null).key");
    }

    @Test
    public void test_function_coalesce() throws Exception
    {
        assertExpression(10, null, "coalesce(null, null, 10)");
        assertExpression(20, null, "coalesce('20', null, 10)");
    }

    @Test
    public void test_function_trim() throws Exception
    {
        assertExpression("hello", null, "trim('  hello  ')");
        assertExpression("hello  ", null, "ltrim('  hello  ')");
        assertExpression("  hello", null, "rtrim('  hello  ')");

        assertExpression("ell", null, "trim('  hello  ', ' ho')");
        assertExpression("ello  ", null, "ltrim('  hello  ', ' ho')");
        assertExpression("  hell", null, "rtrim('  hello  ', ' ho')");

        assertExpression("hello", null, "trim('  hello\n\r  ', ' ' + char(10) + char(13))");
    }

    @Test
    public void test_function_stirng_split() throws Exception
    {
        assertExpression(null, null, "string_split(null, null)");
        assertExpression(null, null, "string_split('hello', null)");
        assertExpression(List.of(UTF8String.from("hello")), null, "string_split('hello', ';')");
        assertExpression(List.of(UTF8String.from("he"), UTF8String.from("o")), null, "string_split('hello', 'll')");
    }

    @Test
    public void test_randomInt() throws Exception
    {
        assertExpression(true, null, "randomInt(10) + 1 > 0");
        assertExpression(null, null, "randomInt(null) + 1 > 0");
    }

    @Test
    public void test_function_concat() throws Exception
    {
        assertExpression("110.1", null, "concat(null,1,10.1)");
        assertExpression("", null, "concat(null,null)");
        assertExpression("123", null, "concat(null,123)");
    }

    @Test
    public void test_function_string_agg() throws Exception
    {
        assertExpression(null, null, "string_agg(null, ',')");
        assertExpression("10", null, "string_agg(10, null)");
        assertExpression("10", null, "string_agg(10, ',')");
        assertExpression("10,20,30", null, "string_agg(array(10,20,30), ',')");
        assertExpression(null, null, "string_agg(array(null,null), ',')");
    }

    @Test
    public void test_function_array() throws Exception
    {
        assertExpression(asList(1, 2, 3), null, "array(1,2,3)");
        assertExpression(emptyList(), null, "array()");
        assertExpression(emptyList(), null, "array()");
        assertExpression(asList(123), MapUtils.ofEntries(MapUtils.entry("col", 123)), "array(@col)");
    }

    @Test
    public void test_function_contains() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("a", asList(-1, -2, -3, 0, 1, 2, 3));
        values.put("b", null);
        values.put("c", asList(-1, -2, -3, 0, 1, 2, 3));
        values.put("d", 10);

        assertExpression(false, values, "contains(null, 1)");
        assertExpression(false, values, "contains(cast(@a as array), 10)");
        assertExpression(true, values, "contains(cast(@a as array), -2)");
        assertExpression(false, values, "contains(cast(@c as array), 10)");
        assertExpression(true, values, "contains(cast(@d as int), 10)");
        assertExpression(false, values, "contains(cast(@d as int), 20)");
        assertExpression(false, values, "contains(@d, 20)");
        assertExpression(true, values, "contains(@d, 10)");
        assertExpression(true, values, "contains(array(null), null)");
        // Reflective array
        assertExpression(true, values, "contains(@a, 3)");

        values.put("c", asList(-1, -2, -3, 0, 1, 2, 3));

        assertExpression(true, values, "contains(cast(@c as array), -2)");
    }

    @Test
    public void test_function_distinct() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("a", asList(-1, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4));
        values.put("b", null);
        values.put("c", asList("a", "A", "A", "hello", "hello", "world"));

        assertExpression(13, values, "distinct(13)");
        assertExpression(null, values, "distinct(null)");
        assertExpression(null, values, "distinct(@b)");
        assertExpression(asList(-1, 3, 4, 1, 0, 2), values, "distinct(cast(@a as array))");
        assertExpression(asList("a", "world", "hello", "A"), values, "distinct(cast(@c as array))");

        values.put("c", asList(-1.0f, -1.0f, -3, 0, 1, 1, 3));

        assertExpression(asList(-1.0f, 3, -3, 1, 0), values, "distinct(cast(@c as array))");
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
        assertExpression("1", null, "isblank(null, 1)");
        assertExpression(null, null, "isblank(null, null)");
        assertExpression("1", null, "isblank('', 1)");
        assertExpression("str", null, "isblank('str', null)");
        assertExpression("20", null, "isblank('20', 10)");
    }

    @Test
    public void test_template_string() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("a", "hello world");
        assertExpression("hello world", values, "`${@a}`");
    }

    @Test
    public void test_getdate() throws Exception
    {
        ExecutionContext context = new ExecutionContext(new QuerySession(new CatalogRegistry()));

        long now = context.getStatementContext()
                .getNow();
        long nowUtc = context.getStatementContext()
                .getNowUtc();

        LocalDateTime expectedNow = Instant.ofEpochMilli(now)
                .atZone(ZoneId.of("UTC"))
                .toLocalDateTime();
        LocalDateTime expectedNowUtc = Instant.ofEpochMilli(nowUtc)
                .atZone(ZoneId.of("UTC"))
                .toLocalDateTime();

        assertExpression(context, expectedNow, null, "getdate()");
        assertExpression(context, expectedNowUtc, null, "getutcdate()");
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
    }
}
