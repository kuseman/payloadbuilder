package org.kuse.payloadbuilder.core.parser;

import static java.util.Arrays.asList;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression.ResolvePath;
import org.kuse.payloadbuilder.core.utils.MapUtils;

/** Test of {@link QualifiedReferenceExpression} */
public class QualifiedReferenceExpressionTest extends AParserTest
{
    @Test
    public void test_no_resolve_path_no_lambda() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("col1", 1337);
        values.put("mapCol", ofEntries(entry("key", "value")));
        values.put("date", new Date());

        // No row in context => null
        assertExpression(null, null, "a");
        assertExpression(1337, values, "col1");
        assertExpression("value", values, "mapCol.key");
        assertExpression(ofEntries(entry("key", "value")), values, "mapCol");

        assertExpressionFail(IllegalArgumentException.class, "Cannot dereference value ", values, "date.value");
    }

    @Test
    public void test_no_resolve_path_with_lambda() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("col1", 1337);
        values.put("mapCol", ofEntries(entry("key", "value")));

        QualifiedReferenceExpression e = new QualifiedReferenceExpression(QualifiedName.of("x", "col1"), 0, null);

        // No Lambda or null => null
        assertEquals(null, e.eval(context));

        context.setLambdaValue(0, values);

        assertEquals(1337, e.eval(context));

        e = new QualifiedReferenceExpression(QualifiedName.of("x", "mapCol", "key"), 0, null);
        assertEquals("value", e.eval(context));

        e = new QualifiedReferenceExpression(QualifiedName.of("x", "mapCol"), 0, null);
        assertEquals(ofEntries(entry("key", "value")), e.eval(context));

        context.setLambdaValue(0, new Date());
        e = new QualifiedReferenceExpression(QualifiedName.of("x", "value"), 0, null);
        try
        {
            e.eval(context);
            fail();
        }
        catch (IllegalArgumentException ee)
        {
            assertTrue(ee.getMessage().contains("Cannot dereference value "));
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void test_resolve_path_no_lambda()
    {
        QualifiedReferenceExpression e = new QualifiedReferenceExpression(QualifiedName.of(), -1, null);

        // Tuple access no column
        e.setResolvePaths(asList(new ResolvePath(-1, 0, asList())));

        // To tuple in context
        assertNull(e.eval(context));

        Map<Integer, Tuple> tupleByOrdinal = new HashMap<>();
        List<String> columns = new ArrayList<>();
        List<Object> values = new ArrayList<>();

        Tuple tuple = new Tuple()
        {
            @Override
            public Tuple getTuple(int ordinal)
            {
                return tupleByOrdinal.get(ordinal);
            }

            @Override
            public int getTupleOrdinal()
            {
                return 0;
            }

            @Override
            public int getColumnCount()
            {
                return columns.size();
            }

            @Override
            public String getColumn(int ordinal)
            {
                return columns.get(ordinal);
            }

            @Override
            public int getColmnOrdinal(String column)
            {
                return Collections.binarySearch(columns, column);
            }

            @Override
            public Object getValue(int ordinal)
            {
                if (ordinal <= -1)
                {
                    return null;
                }
                return values.get(ordinal);
            }
        };

        context.setTuple(tuple);

        // No tuple in target ordinal
        assertNull(e.eval(context));

        // Tuple access
        tupleByOrdinal.put(0, tuple);
        assertSame(tuple, e.eval(context));

        e = new QualifiedReferenceExpression(QualifiedName.of(), -1, null);

        // Tuple access with column
        e.setResolvePaths(asList(new ResolvePath(-1, 0, asList("col"))));

        // Null value in path
        assertNull(e.eval(context));

        Map<String, Object> map = MapUtils.ofEntries(MapUtils.entry("subkey", 1337));
        columns.addAll(asList("col", "date"));
        values.addAll(asList(map, new Date()));

        assertEquals(map, e.eval(context));

        // Tuple access with column and un map access
        e = new QualifiedReferenceExpression(QualifiedName.of(), -1, null);
        e.setResolvePaths(asList(new ResolvePath(-1, 0, asList("col", "subkey"))));

        assertEquals(1337, e.eval(context));

        // Tuple access with column and un map access
        e = new QualifiedReferenceExpression(QualifiedName.of(), -1, null);
        e.setResolvePaths(asList(new ResolvePath(-1, 0, asList("date", "subkey"))));

        try
        {
            e.eval(context);
            fail();
        }
        catch (IllegalArgumentException ee)
        {
            assertTrue(ee.getMessage().contains("Cannot dereference value "));
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void test_resolve_path_with_lambda()
    {
        QualifiedReferenceExpression e = new QualifiedReferenceExpression(QualifiedName.of(), 0, null);
        // Identity lambda 'x -> x'
        e.setResolvePaths(asList(new ResolvePath(-1, -1, asList())));

        // Null in lambda
        assertNull(e.eval(context));

        context.setLambdaValue(0, "value");

        assertEquals("value", e.eval(context));

        e = new QualifiedReferenceExpression(QualifiedName.of(), 0, null);
        // Unresolved lambda path => map access
        e.setResolvePaths(asList(new ResolvePath(-1, -1, asList("key"))));

        context.setLambdaValue(0, MapUtils.ofEntries(MapUtils.entry("key", "value")));

        assertEquals("value", e.eval(context));

        e = new QualifiedReferenceExpression(QualifiedName.of(), 0, null);
        // Tuple access from lambda with non Tuple value
        e.setResolvePaths(asList(new ResolvePath(-1, 0, asList())));

        assertEquals(MapUtils.ofEntries(MapUtils.entry("key", "value")), e.eval(context));

        // Set up tuple
        Map<Integer, Tuple> tupleByOrdinal = new HashMap<>();
        List<String> columns = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        Tuple tuple = new Tuple()
        {
            @Override
            public Tuple getTuple(int ordinal)
            {
                return tupleByOrdinal.get(ordinal);
            }

            @Override
            public int getTupleOrdinal()
            {
                return 0;
            }

            @Override
            public int getColumnCount()
            {
                return columns.size();
            }

            @Override
            public String getColumn(int ordinal)
            {
                return columns.get(ordinal);
            }

            @Override
            public int getColmnOrdinal(String column)
            {
                return Collections.binarySearch(columns, column);
            }

            @Override
            public Object getValue(int ordinal)
            {
                if (ordinal == -1)
                {
                    return null;
                }
                return values.get(ordinal);
            }
        };

        context.setLambdaValue(0, tuple);

        // Tuple target ordinal is null
        assertNull(e.eval(context));

        tupleByOrdinal.put(0, tuple);

        assertSame(tuple, e.eval(context));

        e = new QualifiedReferenceExpression(QualifiedName.of(), 0, null);
        // Tuple lambda 'x -> x.a' with target ordinal
        e.setResolvePaths(asList(new ResolvePath(-1, 0, asList())));

        assertSame(tuple, e.eval(context));

        e = new QualifiedReferenceExpression(QualifiedName.of(), 0, null);
        // Unresolved path lambda 'x -> x.a.col' with target ordinal
        e.setResolvePaths(asList(new ResolvePath(-1, 0, asList("col"))));

        assertNull(e.eval(context));

        columns.add("col");
        values.add(666);

        assertEquals(666, e.eval(context));
    }
}
