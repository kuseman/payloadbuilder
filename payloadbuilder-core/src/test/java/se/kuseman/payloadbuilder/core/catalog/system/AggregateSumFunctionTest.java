package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.List;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo.AggregateMode;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IAggregator;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link AggregateSumFunction} */
public class AggregateSumFunctionTest extends APhysicalPlanTest
{
    IExpression col1 = ce("col1");
    private ScalarFunctionInfo function = SystemCatalog.get()
            .getScalarFunction("sum");

    @Test
    public void test_result_type()
    {
        IExpression e = ce("col", ResolvedType.of(Type.Int));
        assertEquals(ResolvedType.of(Type.Int), function.getType(asList(e)));

        e = ce("col", ResolvedType.of(Type.Float));
        assertEquals(ResolvedType.of(Type.Float), function.getType(asList(e)));

        e = ce("col", ResolvedType.array(ResolvedType.of(Type.Float)));
        assertEquals(ResolvedType.of(Type.Float), function.getType(asList(e)));
    }

    @Test
    public void test_object()
    {
        //@formatter:off
        ValueVector one = VectorTestUtils.vv(Type.Any,
                10, 10, 10, 10,
                10_000_000,10_000_000,10_000_000,10_000_000,
                null,null,null,null
                );
    
        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Any)), List.of(one));
        
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 9, 10, 11)),
                context);
        //@formatter:on

        ValueVector actual = aggregator.combine(context);

        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(3, actual.size());
        assertVectorsEquals(vv(Type.Any, 40, 40_000_000, null), actual);
    }

    @Test
    public void test_object_multi_vector()
    {
        //@formatter:off
        ValueVector one = VectorTestUtils.vv(Type.Any,
                10, 10, 10, 10,
                10_000_000,10_000_000,10_000_000,10_000_000,
                null,null,null,null
                );

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Any)), List.of(one));
        
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 9, 10, 11)),
                context);

        aggregator.appendGroup(input, vv(Type.Int, 1, 2, 3), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 9, 10, 11)),
                context);
        //@formatter:on

        ValueVector actual = aggregator.combine(context);

        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(4, actual.size());
        assertVectorsEquals(vv(Type.Any, 40, 40_000_040, 40_000_000, null), actual);
    }

    @Test
    public void test_decimal()
    {
        //@formatter:off
        ValueVector one = vv(ResolvedType.DECIMAL,
                Decimal.from(10),Decimal.from(10),Decimal.from(10),Decimal.from(10),
                Decimal.from(10_000_000),Decimal.from(10_000_000),Decimal.from(10_000_000),Decimal.from(10_000_000),
                null,null,null,null
                );
    
        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Decimal)), List.of(one));
        
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 9, 10, 11)),
                context);
        //@formatter:on

        ValueVector actual = aggregator.combine(context);
        assertEquals(ResolvedType.of(Type.Decimal), actual.type());
        assertEquals(3, actual.size());
        assertVectorsEquals(vv(Type.Decimal, Decimal.from(40), Decimal.from(40_000_000), null), actual);
    }

    @Test
    public void test_decimal_multi_vectors()
    {
        //@formatter:off
        ValueVector one = vv(ResolvedType.DECIMAL,
                Decimal.from(10),Decimal.from(10),Decimal.from(10),Decimal.from(10),
                Decimal.from(10_000_000),Decimal.from(10_000_000),Decimal.from(10_000_000),Decimal.from(10_000_000),
                null,null,null,null
                );

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Decimal)), List.of(one));
        
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 9, 10, 11)),
                context);

        aggregator.appendGroup(input, vv(Type.Int, 3, 1, 0), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 9, 10, 11)),
                context);
        //@formatter:on

        ValueVector actual = aggregator.combine(context);
        assertEquals(ResolvedType.of(Type.Decimal), actual.type());
        assertEquals(4, actual.size());
        assertVectorsEquals(vv(Type.Decimal, Decimal.from(40), Decimal.from(80_000_000), null, Decimal.from(40)), actual);
    }

    @Test
    public void test_object_scalar()
    {
        ValueVector mediumInts = ValueVector.literalAny(4, 10_000_000);
        Schema schema = schema(new Type[] { Type.Any }, "col1");
        TupleVector input = TupleVector.of(schema, asList(mediumInts));

        ValueVector actual = function.evalScalar(context, input, "", asList(col1));

        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(4, actual.size());
        assertVectorsEquals(vv(Type.Any, 10_000_000, 10_000_000, 10_000_000, 10_000_000), actual);
    }

    @Test
    public void test_object_scalar_value_vector()
    {
        ValueVector smallInts = ValueVector.literalInt(10, 4);
        ValueVector mediumInts = ValueVector.literalInt(10_000_000, 4);
        ValueVector nulls = ValueVector.literalNull(ResolvedType.of(Type.Int), 4);

        Schema schema = Schema.of(Column.of("col1", ResolvedType.array(ResolvedType.of(Type.Any))));

        TupleVector input = TupleVector.of(schema, asList(vv(ResolvedType.array(ResolvedType.of(Type.Any)), smallInts, mediumInts, nulls)));

        ValueVector actual = function.evalScalar(context, input, "", asList(col1));

        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(3, actual.size());
        assertVectorsEquals(vv(Type.Any, 40, 40_000_000, null), actual);
    }

    @Test
    public void test_int()
    {
        //@formatter:off
        ValueVector one = vv(Type.Int,
                10,10,10,10,
                10_000_000,10_000_000,10_000_000,10_000_000,
                null,null,null,null
                );

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Int)), List.of(one));
        
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 9, 10, 11)),
                context);
        //@formatter:on

        ValueVector actual = aggregator.combine(context);
        assertEquals(ResolvedType.of(Type.Int), actual.type());
        assertEquals(3, actual.size());
        assertVectorsEquals(vv(Type.Int, 40, 40_000_000, null), actual);
    }

    @Test
    public void test_int_all_null()
    {
        //@formatter:off
        ValueVector one = vv(Type.Int,
                null,null,null,null,
                null,null,null,null,
                null,null,null,null
                );

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Int)), List.of(one));
        
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 9, 10, 11)),
                context);
        //@formatter:on

        ValueVector actual = aggregator.combine(context);
        assertEquals(ResolvedType.of(Type.Int), actual.type());
        assertEquals(3, actual.size());
        assertVectorsEquals(vv(Type.Int, null, null, null), actual);
    }

    @Test
    public void test_long()
    {
        //@formatter:off
        ValueVector one = vv(Type.Long,
                10L,10L,10L,10L,
                10_000_000L,10_000_000L,10_000_000L,10_000_000L,
                null,null,null,null
                );

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Long)), List.of(one));
        
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 9, 10, 11)),
                context);
        //@formatter:on

        ValueVector actual = aggregator.combine(context);
        assertEquals(ResolvedType.of(Type.Long), actual.type());
        assertEquals(3, actual.size());
        assertVectorsEquals(vv(Type.Long, 40L, 40_000_000L, null), actual);
    }

    @Test
    public void test_float()
    {
        //@formatter:off
        ValueVector one = vv(Type.Float,
                10F,10F,10F,10F,
                10_000_000,10_000_000,10_000_000,10_000_000,
                null,null,null,null
                );

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Float)), List.of(one));
        
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 9, 10, 11)),
                context);
        //@formatter:on

        ValueVector actual = aggregator.combine(context);
        assertEquals(ResolvedType.of(Type.Float), actual.type());
        assertEquals(3, actual.size());
        assertVectorsEquals(vv(Type.Float, 40F, 40_000_000F, null), actual);
    }

    @Test
    public void test_double()
    {
        //@formatter:off
        ValueVector one = vv(Type.Double,
                10_000_000,10_000_000,10_000_000,10_000_000,
                10,10,10,10,
                null,null,null,null
                );

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Double)), List.of(one));
        
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 9, 10, 11)),
                context);
        //@formatter:on

        ValueVector actual = aggregator.combine(context);
        assertEquals(ResolvedType.of(Type.Double), actual.type());
        assertEquals(3, actual.size());
        assertVectorsEquals(vv(Type.Double, 40_000_000D, 40D, null), actual);
    }

    @Test(
            expected = ArithmeticException.class)
    public void test_long_overflow()
    {
        ValueVector longs = ValueVector.literalLong(Long.MAX_VALUE, Integer.MAX_VALUE);
        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Long)), List.of(longs));
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0), vv(ResolvedType.array(Type.Int), vv(Type.Int, 0, 1)), context);
    }

    @Test(
            expected = ArithmeticException.class)
    public void test_overflow_int()
    {
        //@formatter:off
        ValueVector one = vv(Type.Int,
                10,10,10,10,
                Integer.MAX_VALUE / 3,Integer.MAX_VALUE / 3,Integer.MAX_VALUE / 3,Integer.MAX_VALUE / 3,
                10_000_000,10_000_000,10_000_000,10_000_000,
                null,null,null,null
                );

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Int)), List.of(one));
        
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 9, 10, 11)),
                context);
        //@formatter:on
    }
}
