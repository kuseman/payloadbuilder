package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import se.kuseman.payloadbuilder.core.QuerySession;
import se.kuseman.payloadbuilder.core.catalog.CatalogRegistry;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.expression.AExpressionTest;

/** Tests functions etc. in built in catalog */
public class SystemCatalogTest extends AExpressionTest
{
    @Test
    public void test_function_hash() throws Exception
    {
        assertExpression(null, null, "hash(null)");
        assertExpression(null, null, "hash(null,true)");
        assertExpression(629, null, "hash(true)");
        assertExpression(23433, null, "hash(1,123)");
    }

    @Test
    public void test_function_coalesce() throws Exception
    {
        assertExpression(10, null, "coalesce(null, null, 10)");
        assertExpression(20, null, "coalesce('20', null, 10)");
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
    }

    @Test
    public void test_function_contains() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("a", asList(-1, -2, -3, 0, 1, 2, 3));
        values.put("b", null);
        values.put("c", asList(-1, -2, -3, 0, 1, 2, 3));

        assertExpression(false, values, "contains(null, 1)");
        assertExpression(false, values, "contains(cast(@a as valuevector), 10)");
        assertExpression(true, values, "contains(cast(@a as valuevector), -2)");
        assertExpression(false, values, "contains(cast(@c as valuevector), 10)");

        values.put("c", asList(-1, -2, -3, 0, 1, 2, 3));

        assertExpression(true, values, "contains(cast(@c as valuevector), -2)");
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
        assertExpression(asList(-1, 3, 4, 1, 0, 2), values, "distinct(cast(@a as valuevector))");
        assertExpression(asList("a", "world", "hello", "A"), values, "distinct(cast(@c as valuevector))");

        values.put("c", asList(-1.0f, -1.0f, -3, 0, 1, 1, 3));

        assertExpression(asList(-1.0f, 3, -3, 1, 0), values, "distinct(cast(@c as valuevector))");
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
        assertExpression(null, null, "isblank(null, null)");
        assertExpression(1, null, "isblank('', 1)");
        assertExpression("str", null, "isblank('str', null)");
        assertExpression("20", null, "isblank('20', 10)");
    }

    @Test
    public void test_getdate() throws Exception
    {
        ExecutionContext context = new ExecutionContext(new QuerySession(new CatalogRegistry()));
        ZonedDateTime now = context.getStatementContext()
                .getNow()
                .withZoneSameInstant(ZoneOffset.UTC);

        ZonedDateTime nowLocal = now.withZoneSameInstant(ZoneId.systemDefault());

        assertExpression(context, now, null, "getutcdate()", false, true);
        assertExpression(context, nowLocal, null, "getdate()", false, true);
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
