package se.kuseman.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToIntBiFunction;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.TableMeta.DataType;
import se.kuseman.payloadbuilder.api.operator.IOrdinalValues;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.api.utils.MapUtils;
import se.kuseman.payloadbuilder.core.parser.AParserTest;
import se.kuseman.payloadbuilder.core.parser.Expression;

/** Code generator test of {@link IOrdinalValuesFactory} */
public class OrdinalValuesFactoryTest extends AParserTest
{
    @Test
    public void test_ne()
    {
        IOrdinalValuesFactory a = CODE_GENERATOR.generateOrdinalValuesFactory(asList(e("10"), e("true")));
        IOrdinalValuesFactory b = CODE_GENERATOR.generateOrdinalValuesFactory(asList(e("10")));
        assertNotEquals(a.create(context, NoOpTuple.NO_OP), b.create(context, NoOpTuple.NO_OP));
    }

    @Test
    public void test()
    {
        List<Expression> es = asList(e("10"), e("10l"), e("10f"), e("10d"), e("true"), e("'string'"));
        IOrdinalValuesFactory f = CODE_GENERATOR.generateOrdinalValuesFactory(es);

        IOrdinalValues values = f.create(context, NoOpTuple.NO_OP);
        assertEquals(6, values.size());
        assertEquals(DataType.INT, values.getType(0));
        assertEquals(DataType.LONG, values.getType(1));
        assertEquals(DataType.FLOAT, values.getType(2));
        assertEquals(DataType.DOUBLE, values.getType(3));
        assertEquals(DataType.BOOLEAN, values.getType(4));
        assertEquals(DataType.ANY, values.getType(5));

        // Verify that wrong type getters throw
        for (int i = 0; i < 5; i++)
        {
            try
            {
                if (i == 0)
                {
                    values.getLong(i);
                }
                else
                {
                    values.getInt(i);
                }
                fail();
            }
            catch (IllegalArgumentException e)
            {
            }
        }
        assertEquals(10, values.getInt(0));
        assertEquals(10L, values.getLong(1));
        assertEquals(10F, values.getFloat(2), 0);
        assertEquals(10D, values.getDouble(3), 0);
        assertEquals(true, values.getBool(4));
        assertEquals("string", values.getValue(5));
        assertEquals(-164035311, values.hashCode());

        // Verify hashCode/equals are equal for generated and expression functions
        ToIntBiFunction<ExecutionContext, Tuple> hashFunction = CODE_GENERATOR.generateHashFunction(es);
        assertEquals(values.hashCode(), hashFunction.applyAsInt(context, NoOpTuple.NO_OP));

        hashFunction = new ExpressionHashFunction(es);
        assertEquals(values.hashCode(), hashFunction.applyAsInt(context, NoOpTuple.NO_OP));

        ExpressionOrdinalValuesFactory ef = new ExpressionOrdinalValuesFactory(es);
        assertEquals(values.hashCode(), ef.create(context, NoOpTuple.NO_OP)
                .hashCode());

        assertEquals(values, ef.create(context, NoOpTuple.NO_OP));
        assertEquals(ef.create(context, NoOpTuple.NO_OP), values);

        IOrdinalValues other = f.create(context, NoOpTuple.NO_OP);
        assertEquals(values, other);
        assertEquals(values.hashCode(), other.hashCode());

        f = CODE_GENERATOR.generateOrdinalValuesFactory(es);
        other = f.create(context, NoOpTuple.NO_OP);

        Map<IOrdinalValues, String> map = new HashMap<>();
        map.put(values, "hello");
        assertEquals("hello", map.get(other));
    }

    @Test
    public void test_numbers_equals_with_boolean()
    {
        Tuple tuple = mock(Tuple.class);
        Expression a = e("a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(true, DataType.ANY))));

        List<Expression> es = asList(e("1"), e("1l"), e("1f"), e("1d"), e("true"), a);
        List<IOrdinalValuesFactory> fs = es.stream()
                .map(e -> CODE_GENERATOR.generateOrdinalValuesFactory(singletonList(e)))
                .collect(toList());

        int size = fs.size();
        for (int i = 0; i < size; i++)
        {
            IOrdinalValues valuesA = fs.get(i)
                    .create(context, tuple);
            for (int j = 0; j < size; j++)
            {
                IOrdinalValues valuesB = fs.get(j)
                        .create(context, tuple);
                assertEquals("Expected " + es.get(i)
                        .getDataType()
                             + " equals "
                             + es.get(j)
                                     .getDataType(),
                        valuesA, valuesB);
            }
        }

        // Test not equals
        Tuple tupleA = mock(Tuple.class);
        a = e("a", tupleA, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(false, DataType.ANY))));

        List<Expression> es1 = asList(e("0"), e("0l"), e("0f"), e("0d"), e("false"), a);
        List<IOrdinalValuesFactory> fs1 = es1.stream()
                .map(e -> CODE_GENERATOR.generateOrdinalValuesFactory(singletonList(e)))
                .collect(toList());

        for (int i = 0; i < size; i++)
        {
            IOrdinalValues valuesA = fs.get(i)
                    .create(context, tuple);
            for (int j = 0; j < size; j++)
            {
                IOrdinalValues valuesB = fs1.get(j)
                        .create(context, tupleA);
                assertNotEquals("Expected " + es.get(i)
                                + " ("
                                + es.get(i)
                                        .getDataType()
                                + ")"
                                + " NOT equals "
                                + es1.get(j)
                                + " ("
                                + es1.get(j)
                                        .getDataType()
                                + ")",
                        valuesA, valuesB);
            }
        }
    }

    @Test
    public void test_numbers_equals()
    {
        Tuple tuple = mock(Tuple.class);
        Expression a = e("a", tuple, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(100000, DataType.ANY))));

        // Use high values here to avoid primitive caches
        List<Expression> es = asList(e("100000"), e("100000l"), e("100000f"), e("100000d"), e("'100000'"), a);
        List<IOrdinalValuesFactory> fs = es.stream()
                .map(e -> CODE_GENERATOR.generateOrdinalValuesFactory(singletonList(e)))
                .collect(toList());

        int size = fs.size();
        for (int i = 0; i < size; i++)
        {
            IOrdinalValues valuesA = fs.get(i)
                    .create(context, tuple);
            for (int j = 0; j < size; j++)
            {
                IOrdinalValues valuesB = fs.get(j)
                        .create(context, tuple);
                assertEquals("Expected " + es.get(i)
                        .getDataType()
                             + " equals "
                             + es.get(j)
                                     .getDataType(),
                        valuesA, valuesB);
            }
        }

        // Test not equals
        Tuple tupleA = mock(Tuple.class);
        a = e("a", tupleA, MapUtils.ofEntries(MapUtils.entry("a", Pair.of(100001, DataType.ANY))));

        List<Expression> es1 = asList(e("100001"), e("100001l"), e("100001f"), e("100001d"), e("'100001'"), a);
        List<IOrdinalValuesFactory> fs1 = es1.stream()
                .map(e -> CODE_GENERATOR.generateOrdinalValuesFactory(singletonList(e)))
                .collect(toList());

        for (int i = 0; i < size; i++)
        {
            IOrdinalValues valuesA = fs.get(i)
                    .create(context, tuple);
            for (int j = 0; j < size; j++)
            {
                IOrdinalValues valuesB = fs1.get(j)
                        .create(context, tupleA);
                assertNotEquals("Expected " + es.get(i)
                                + " ("
                                + es.get(i)
                                        .getDataType()
                                + ")"
                                + " NOT equals "
                                + es1.get(j)
                                + " ("
                                + es1.get(j)
                                        .getDataType()
                                + ")",
                        valuesA, valuesB);
            }
        }
    }
}
