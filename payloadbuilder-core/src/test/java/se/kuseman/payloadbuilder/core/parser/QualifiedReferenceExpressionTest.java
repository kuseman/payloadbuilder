package se.kuseman.payloadbuilder.core.parser;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.TableMeta.DataType;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.api.utils.MapUtils;
import se.kuseman.payloadbuilder.core.parser.QualifiedReferenceExpression.ResolvePath;

/** Test of {@link QualifiedReferenceExpression} */
public class QualifiedReferenceExpressionTest extends AParserTest
{
    @Test
    public void test_code_gen()
    {
        QualifiedReferenceExpression e = new QualifiedReferenceExpression(QualifiedName.of("a", "col1"), -1, new ResolvePath[] { new ResolvePath(-1, 1, asList("col"), -1, DataType.INT) }, null);
        assertEquals(DataType.INT, e.getDataType());

        e = new QualifiedReferenceExpression(QualifiedName.of("a", "col1"), -1, new ResolvePath[] { new ResolvePath(-1, 1, asList("col"), -1, DataType.FLOAT) }, null);
        assertEquals(DataType.FLOAT, e.getDataType());

        e = new QualifiedReferenceExpression(QualifiedName.of("a", "col1"), -1, new ResolvePath[] { new ResolvePath(-1, 0, asList("col"), -1) }, null);
        assertEquals(DataType.ANY, e.getDataType());
    }

    @Test
    public void test_isCodeGenSupported()
    {
        // No lambda support
        QualifiedReferenceExpression e = new QualifiedReferenceExpression(QualifiedName.of("x", "col1"), 0, new ResolvePath[] { new ResolvePath(-1, 1, emptyList(), 0) }, null);
        assertFalse(e.isCodeGenSupported());
        e = new QualifiedReferenceExpression(QualifiedName.of("x", "col1"), 0, new ResolvePath[0], null);
        assertFalse(e.isCodeGenSupported());

        // No multi source
        e = new QualifiedReferenceExpression(QualifiedName.of("col1"), -1, new ResolvePath[] { new ResolvePath(0, 1, emptyList(), 0), new ResolvePath(1, 1, emptyList(), 0) }, null);
        assertFalse(e.isCodeGenSupported());

        // No multi part path
        e = new QualifiedReferenceExpression(QualifiedName.of("map", "key"), -1, new ResolvePath[] { new ResolvePath(0, 1, asList("map", "key"), -1) }, null);
        assertFalse(e.isCodeGenSupported());

        e = new QualifiedReferenceExpression(QualifiedName.of("map", "key"), -1, new ResolvePath[] { new ResolvePath(0, 1, asList("map", "key"), 0) }, null);
        assertFalse(e.isCodeGenSupported());

        // No tuple access
        e = new QualifiedReferenceExpression(QualifiedName.of("alias"), -1, new ResolvePath[] { new ResolvePath(0, 1, emptyList(), -1) }, null);
        assertFalse(e.isCodeGenSupported());

        // Column ordinal access
        e = new QualifiedReferenceExpression(QualifiedName.of("t", "col"), -1, new ResolvePath[] { new ResolvePath(0, 1, emptyList(), 0) }, null);
        assertTrue(e.isCodeGenSupported());

        // Column access
        e = new QualifiedReferenceExpression(QualifiedName.of("col"), -1, new ResolvePath[] { new ResolvePath(0, 1, asList("col"), -1) }, null);
        assertTrue(e.isCodeGenSupported());
    }

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

        assertExpressionFail(IllegalArgumentException.class, "Cannot dereference 'value' ", values, "date.value");
    }

    @Test
    public void test_no_resolve_path_with_lambda() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("col1", 1337);
        values.put("mapCol", ofEntries(entry("key", "value")));

        QualifiedReferenceExpression e = new QualifiedReferenceExpression(QualifiedName.of("x", "col1"), 0, new ResolvePath[0], null);

        // No Lambda or null => null
        assertEquals(null, e.eval(context));

        context.getStatementContext()
                .setLambdaValue(0, values);

        assertEquals(1337, e.eval(context));

        e = new QualifiedReferenceExpression(QualifiedName.of("x", "mapCol", "key"), 0, new ResolvePath[0], null);
        assertEquals("value", e.eval(context));

        e = new QualifiedReferenceExpression(QualifiedName.of("x", "mapCol"), 0, new ResolvePath[0], null);
        assertEquals(ofEntries(entry("key", "value")), e.eval(context));

        context.getStatementContext()
                .setLambdaValue(0, new Date());
        e = new QualifiedReferenceExpression(QualifiedName.of("x", "value"), 0, new ResolvePath[0], null);
        try
        {
            e.eval(context);
            fail();
        }
        catch (IllegalArgumentException ee)
        {
            assertTrue(ee.getMessage()
                    .contains("Cannot dereference 'value' "));
        }
    }

    @Test
    public void test_resolve_path_no_lambda()
    {
        QualifiedReferenceExpression e = new QualifiedReferenceExpression(QualifiedName.of(), -1, new ResolvePath[] {
                // Tuple access no column
                new ResolvePath(-1, 1, asList(), -1) }, null);

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
            public String getColumn(int columnOrdinal)
            {
                return columns.get(columnOrdinal);
            }

            @Override
            public int getColumnOrdinal(String column)
            {
                return Collections.binarySearch(columns, column);
            }

            @Override
            public Object getValue(int columnOrdinal)
            {
                if (columnOrdinal <= -1)
                {
                    return null;
                }
                return values.get(columnOrdinal);
            }
        };

        context.getStatementContext()
                .setTuple(tuple);

        // No tuple in target ordinal
        assertNull(e.eval(context));

        // Tuple access
        tupleByOrdinal.put(1, tuple);
        assertSame(tuple, e.eval(context));

        e = new QualifiedReferenceExpression(QualifiedName.of(), -1, new ResolvePath[] {
                // Tuple access with column
                new ResolvePath(-1, 0, asList("col"), -1) }, null);

        // Null value in path
        assertNull(e.eval(context));

        Map<String, Object> map = MapUtils.ofEntries(MapUtils.entry("subkey", 1337));
        columns.addAll(asList("col", "date"));
        values.addAll(asList(map, new Date()));

        assertEquals(map, e.eval(context));

        // Tuple access with column and a map access
        e = new QualifiedReferenceExpression(QualifiedName.of(), -1, new ResolvePath[] { new ResolvePath(-1, 0, asList("col", "subkey"), -1) }, null);

        assertEquals(1337, e.eval(context));

        // Tuple access with column and a map access
        e = new QualifiedReferenceExpression(QualifiedName.of(), -1, new ResolvePath[] { new ResolvePath(-1, 0, asList("date", "subkey"), -1) }, null);

        try
        {
            e.eval(context);
            fail();
        }
        catch (IllegalArgumentException ee)
        {
            assertTrue(ee.getMessage(), ee.getMessage()
                    .contains("Cannot dereference 'subkey' "));
        }
    }

    @Test
    public void test_resolve_path_with_lambda()
    {
        QualifiedReferenceExpression e = new QualifiedReferenceExpression(QualifiedName.of(), 0, new ResolvePath[] {
                // Identity lambda 'x -> x'
                new ResolvePath(-1, -1, asList(), -1) }, null);

        // Null in lambda
        assertNull(e.eval(context));

        context.getStatementContext()
                .setLambdaValue(0, "value");

        assertEquals("value", e.eval(context));

        e = new QualifiedReferenceExpression(QualifiedName.of(), 0, new ResolvePath[] {
                // Unresolved lambda path => map access
                new ResolvePath(-1, -1, asList("key"), -1) }, null);
        context.getStatementContext()
                .setLambdaValue(0, MapUtils.ofEntries(MapUtils.entry("key", "value")));
        assertEquals("value", e.eval(context));

        // Set up tuple
        Map<Integer, Tuple> tupleByOrdinal = new HashMap<>();
        List<String> columns = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        Tuple tuple = new Tuple()
        {
            @Override
            public Tuple getTuple(int tupleOrdinal)
            {
                if (tupleOrdinal == 666)
                {
                    return this;
                }

                return tupleByOrdinal.get(tupleOrdinal);
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
            public String getColumn(int columnOrdinal)
            {
                return columns.get(columnOrdinal);
            }

            @Override
            public int getColumnOrdinal(String column)
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

        // Map access with a tuple in context
        // This to make sure we don't traverse the tuple in context
        // when we have a non tuple in a lambda
        e = new QualifiedReferenceExpression(QualifiedName.of(), 0, new ResolvePath[] {
                // Unresolved lambda path => map access
                new ResolvePath(-1, -1, asList("key"), -1) }, null);
        context.getStatementContext()
                .setTuple(tuple);
        context.getStatementContext()
                .setLambdaValue(0, MapUtils.ofEntries(MapUtils.entry("key", "value")));
        assertEquals("value", e.eval(context));

        e = new QualifiedReferenceExpression(QualifiedName.of(), 0, new ResolvePath[] {
                // Tuple access from lambda with non Tuple value
                new ResolvePath(-1, 1, asList(), -1) }, null);
        assertEquals(MapUtils.ofEntries(MapUtils.entry("key", "value")), e.eval(context));

        context.getStatementContext()
                .setLambdaValue(0, tuple);

        // Tuple target ordinal is null
        assertNull(e.eval(context));

        tupleByOrdinal.put(1, tuple);

        assertSame(tuple, e.eval(context));

        e = new QualifiedReferenceExpression(QualifiedName.of(), 0, new ResolvePath[] {
                // Tuple lambda 'x -> x.a' with target ordinal
                new ResolvePath(-1, 0, asList(), -1) }, null);

        assertSame(tuple, e.eval(context));

        e = new QualifiedReferenceExpression(QualifiedName.of(), 0, new ResolvePath[] {
                // Unresolved path lambda 'x -> x.a.col' with target ordinal
                new ResolvePath(-1, 0, asList("col"), -1) }, null);

        assertNull(e.eval(context));

        columns.add("col");
        values.add(666);

        assertEquals(666, e.eval(context));

        e = new QualifiedReferenceExpression(QualifiedName.of(), 0, new ResolvePath[] {
                // Identity lambda 'x -> x' which is a tuple
                new ResolvePath(-1, -1, asList(), -1) }, null);
        assertSame(tuple, e.eval(context));
    }
}
