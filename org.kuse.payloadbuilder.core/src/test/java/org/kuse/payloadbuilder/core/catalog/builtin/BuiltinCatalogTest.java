package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.Iterator;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.Assert;
import org.junit.Test;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.catalog.TableAlias;
import org.kuse.payloadbuilder.core.codegen.CodeGenerator;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QueryParser;

/** Tests functions etc. in built in catalog */
public class BuiltinCatalogTest extends Assert
{
    private final CodeGenerator codeGenerator = new CodeGenerator();
    private final QueryParser parser = new QueryParser();
    private final QuerySession session = new QuerySession(new CatalogRegistry());

    @Test
    public void test_function_hash()
    {
        assertFunction(null, null, "hash(null)");
        assertFunction(null, null, "hash(null,true)");
        assertFunction(1262, null, "hash(true)");
        assertFunction(1115, null, "hash(1,123)");
    }

    @Test
    public void test_function_coalesce()
    {
        assertFunction(10, null, "coalesce(null, null, 10)");
        assertFunction("str", null, "coalesce('str', null, 10)");
    }
    
    @Test
    public void test_randomInt()
    {
        assertFunction(true, null, "randomInt(10) + 1 > 0");
        assertFunction(null, null, "randomInt(null) + 1 > 0");
    }

    @Test
    public void test_function_filter()
    {
        TableAlias alias = TableAlias.of(null, "table", "t");
        alias.setColumns(new String[] {"a", "b"});
        Row row = Row.of(alias, 0, new Object[] {asList(-1, -2, -3, 0, 1, 2, 3), null});
        assertFunction(asList(1, 2, 3), row, "a.filter(a -> a > 0)");
        assertFunction(null, row, "b.filter(a -> a > 0)");
        assertFunction(asList(2), row, "filter(a.filter(a -> a > 0), a -> a = 2)");
        assertFunction(asList("1s", "2s", "3s"), row, "map(a.filter(a -> a > 0), a -> a + 's')");
    }

    @Test
    public void test_function_concat()
    {
        TableAlias alias = TableAlias.of(null, "table", "t");
        alias.setColumns(new String[] {"a", "b"});
        Row row = Row.of(alias, 0, new Object[] {asList(-1, -2, -3, 0, 1, 2, 3), null});
        assertFunction("110.1", row, "concat(null,1,10.1)");
        assertFunction("", row, "concat(null,null)");
        assertFunction(asList(-1, -2, -3, 0, 1, 2, 3, 1, 2, 3), row, "a.concat(a.filter(x -> x > 0))");
        assertFunction(asList(-1, -2, -3, 1, 2, 3), row, "concat(a.filter(x -> x < 0), a.filter(x -> x > 0))");
    }

    @Test
    public void test_function_map()
    {
        TableAlias alias = TableAlias.of(null, "table", "t");
        alias.setColumns(new String[] {"a", "b"});
        Row row = Row.of(alias, 0, new Object[] {asList(-1, -2, -3, 0, 1, 2, 3), null});
        assertFunction(null, row, "b.map(a -> a * 2)");
        assertFunction(asList(-2, -4, -6, 0, 2, 4, 6), row, "a.map(a -> a * 2)");
        assertFunction(asList(-1, -2, -3, 0, 1, 2, 3), row, "map(a.map(a -> a * 2), a -> a / 2)");
    }

    @Test
    public void test_function_match()
    {
        TableAlias alias = TableAlias.of(null, "article", "a");
        alias.setColumns(new String[] {"a", "b", "c"});
        Row row = Row.of(alias, 0, new Object[] {asList(), null, asList(1, 2)});
        
        // Empty 
        assertFunction(false, row, "a.any(x -> x > 0)");
        assertFunction(true, row, "a.all(x -> x > 0)");
        assertFunction(true, row, "a.none(x -> x > 0)");

        // Null 
        assertFunction(null, row, "b.any(x -> x > 0)");
        assertFunction(null, row, "b.all(x -> x > 0)");
        assertFunction(null, row, "b.none(x -> x > 0)");
        
        // Values
        assertFunction(true, row, "c.any(x -> x > 0)");
        assertFunction(true, row, "c.all(x -> x > 0)");
        assertFunction(false, row, "c.none(x -> x > 0)");

        assertFunction(false, row, "c.any(x -> x < 0)");
        assertFunction(false, row, "c.all(x -> x < 0)");
        assertFunction(true, row, "c.none(x -> x < 0)");
        
        assertFail(IllegalArgumentException.class, "Expected boolean result but got: 2", row, "c.any(x -> x+1)");
    }

    @Test
    public void test_function_flatMap()
    {
        TableAlias alias = TableAlias.of(null, "table", "t");
        alias.setColumns(new String[] {"a", "b"});
        Row row = Row.of(alias, 0, new Object[] {asList(-1, -2, 3, null), null});
        assertFunction(asList(-1, -2, 3, null), row, "a.flatMap(a -> a)");
        assertFunction(null, row, "b.flatMap(a -> a)");
        assertFunction(asList(-1, -2, 3, null, -1, -2, 3, null, -1, -2, 3, null, -1, -2, 3, null), row, "a.flatMap(l -> a)");
        assertFunction(asList(-1, -2, 3, -1, -2, 3, -1, -2, 3, -1, -2, 3), row, "a.flatMap(l -> a).filter(a -> a IS NOT NULL)");
        assertFunction(asList(null, null, null, null), row, "a.flatMap(l -> a).filter(a -> a IS NULL)");
    }

    @Test
    public void test_function_json_value()
    {
        assertFunction(null, null, "json_value(null)");
        assertFunction(emptyMap(), null, "json_value('{}')");
        assertFunction(emptyList(), null, "json_value('[]')");
    }
    
    @Test
    public void test_function_isnull()
    {
        assertFunction(null, null, "isnull(null, null)");
        assertFunction(1, null, "isnull(null, 1)");
        assertFunction(1, null, "isnull(1, null)");
    }
    
    @Test
    public void test_function_isblank()
    {
        assertFunction(1, null, "isblank(null, 1)");
        assertFunction(1, null, "isblank('', 1)");
        assertFunction("str", null, "isblank('str', null)");
    }
    
    @Test
    public void test_function_cast()
    {
        assertFunction(null, null, "cast(null, integer)");
        assertFunction(null, null, "cast(1, null)");

        assertFunction(1, null, "cast(1, integer)");
        assertFunction(1l, null, "cast(1, long)");
        assertFunction(1.0f, null, "cast(1, float)");
        assertFunction(1.0d, null, "cast(1, double)");

        assertFunction(1, null, "cast('1', integer)");
        assertFunction(1l, null, "cast('1', long)");
        assertFunction(1.0f, null, "cast('1', float)");
        assertFunction(1.0d, null, "cast('1', double)");
        
        assertFunction("1.0", null, "cast(1.0, string)");
        assertFunction("1.0", null, "cast(1.0, 'string')");

        assertFunction(true, null, "cast(1, boolean)");
        assertFunction(true, null, "cast(1.20, boolean)");
        assertFunction(false, null, "cast(0, 'boolean')");
        assertFunction(true, null, "cast('TRUE', boolean)");
        assertFunction(false, null, "cast('false', 'boolean')");
        assertFunction(false, null, "cast('hello', 'boolean')");

        assertFunction(LocalDateTime.parse("2000-10-10T00:00:00"), null, "cast('2000-10-10', 'datetime')");
        assertFunction(LocalDateTime.parse("2000-10-10T10:10:00"), null, "cast('2000-10-10 10:10', 'datetime')");
        assertFunction(LocalDateTime.parse("2000-10-10T12:10:00"), null, "cast('2000-10-10 10:10Z', 'datetime')");
        assertFunction(LocalDateTime.parse("2000-10-10T12:10:10.123"), null, "cast('2000-10-10 10:10:10.123Z', 'datetime')");
        
        assertFail(DateTimeParseException.class, "Text 'jibberish' could not", null, "cast('jibberish', 'datetime')");
        assertFail(IllegalArgumentException.class, "Cannot cast", null, "cast(true, integer)");
        assertFail(IllegalArgumentException.class, "Cannot cast", null, "cast(true, long)");
        assertFail(IllegalArgumentException.class, "Cannot cast", null, "cast(true, float)");
        assertFail(IllegalArgumentException.class, "Cannot cast", null, "cast(true, double)");
    }

    @SuppressWarnings("unchecked")
    private void assertFunction(Object expected, Row row, String expression)
    {
        TableAlias alias = row == null ? TableAlias.of(null, "table", "t") : row.getTableAlias();
        row = row != null ? row : Row.of(alias, 0, new Object[0]);
        Expression e = parser.parseExpression(expression);
        Object actual = null;
        
        try
        {
            actual = codeGenerator.generateFunction(alias, e).apply(row);
            if (actual instanceof Iterator)
            {
                actual = IteratorUtils.toList((Iterator<Object>) actual);
            }
            assertEquals("Code gen", expected, actual);
        }
        catch (NotImplementedException ee)
        {
            System.out.println("Missing implementation: " + ee.getMessage());
        }
        

        ExecutionContext context = new ExecutionContext(session);
        context.setRow(row);
        actual = e.eval(context);
        if (actual instanceof Iterator)
        {
            actual = IteratorUtils.toList((Iterator<Object>) actual);
        }

        assertEquals("Eval", expected, actual);
    }
    
    private void assertFail(Class<? extends Exception> e, String messageContains, Row row, String expression)
    {
        Expression expr = null;
        try
        {
            expr = parser.parseExpression(expression);
            codeGenerator.generateFunction(null, expr).apply(row);
            fail(expression + " should fail.");
        }
        catch (NotImplementedException ee)
        {
            System.out.println("Implement. " + ee.getMessage());
        }
        catch (Exception ee)
        {
            assertEquals(e, ee.getClass());
            assertTrue("Expected expcetion message to contain " + messageContains + " but was: " + ee.getMessage(), ee.getMessage().contains(messageContains));
        }
        
        if (expr == null)
        {
            return;
        }
        
        try
        {
            ExecutionContext context = new ExecutionContext(session);
            context.setRow(row);
            expr.eval(context);
            fail(expression + " should fail.");
        }
        catch (Exception ee)
        {
            assertEquals(e, ee.getClass());
            assertTrue("Expected expcetion message to contain " + messageContains + " but was: " + ee.getMessage(), ee.getMessage().contains(messageContains));
        }
    }
}
