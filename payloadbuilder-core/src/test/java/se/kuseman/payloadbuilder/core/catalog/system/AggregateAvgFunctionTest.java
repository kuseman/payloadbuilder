package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.math.BigDecimal;
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

/** Test of {@link AggregateAvgFunction} */
public class AggregateAvgFunctionTest extends APhysicalPlanTest
{
    private IExpression col1 = ce("col1");
    private ScalarFunctionInfo function = SystemCatalog.get()
            .getScalarFunction("avg");

    @Test
    public void test_object()
    {
        //@formatter:off
        ValueVector one = VectorTestUtils.vv(Type.Any,
                10, 20, -20F, -200D,
                10_000_000,10_000_000,10_000_000,10_000_000,
                null,null,null,null,
                new BigDecimal("100.10"), new BigDecimal("-200.10"), new BigDecimal("2000.10"), null
                );

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Any)), List.of(one));
        
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2, 3), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 9, 10, 11),
                vv(Type.Int, 12, 13, 14, 15)),
                context);
        //@formatter:on

        ValueVector actual = aggregator.combine(context);
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(4, actual.size());
        assertVectorsEquals(vv(Type.Any, -47.5D, 10_000_000, null, Decimal.from("633.37")), actual);
    }

    @Test
    public void test_object_multi_vector()
    {
        //@formatter:off
        ValueVector one = VectorTestUtils.vv(Type.Any,
                10, 20, -20F, -200D,
                10_000_000,10_000_000,10_000_000,10_000_000,
                null,null,null,null,
                new BigDecimal("100.10"), new BigDecimal("-200.10"), new BigDecimal("2000.10"), null
                );

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Any)), List.of(one));
        
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2, 3), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 9, 10, 11),
                vv(Type.Int, 12, 13, 14, 15)),
                context);
        
        aggregator.appendGroup(input, vv(Type.Int, 3, 1, 0, 4), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 9, 10, 11),
                vv(Type.Int, 12, 13, 14, 15)),
                context);
        //@formatter:on

        ValueVector actual = aggregator.combine(context);
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(5, actual.size());
        assertVectorsEquals(vv(Type.Any, -47.5D, 10_000_000, null, 244.29999999999998D, Decimal.from("633.37")), actual);
    }

    @Test
    public void test_decimal()
    {
        //@formatter:off
        ValueVector one = vv(ResolvedType.DECIMAL,
                Decimal.from("10.00"), Decimal.from(20), Decimal.from(-20F), Decimal.from(-200D),
                Decimal.from(10_000_000),Decimal.from(10_000_000),Decimal.from(10_000_000),Decimal.from(10_000_000),
                null,null,null,null,
                Decimal.from(new BigDecimal("100.10")), Decimal.from(new BigDecimal("-200.10")), Decimal.from(new BigDecimal("2000.10")), null
                );

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Decimal)), List.of(one));
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2, 3), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 9, 10, 11),
                vv(Type.Int, 12, 13, 14, 15)),
                context);
        //@formatter:on

        ValueVector actual = aggregator.combine(context);
        assertEquals(ResolvedType.of(Type.Decimal), actual.type());
        assertEquals(4, actual.size());
        assertVectorsEquals(vv(Type.Decimal, Decimal.from("-47.500000"), Decimal.from(10_000_000), null, Decimal.from(new BigDecimal("633.37"))), actual);
    }

    @Test
    public void test_decimal_multi_vector()
    {
        //@formatter:off
        ValueVector one = vv(ResolvedType.DECIMAL,
                Decimal.from("10.00"), Decimal.from(20), Decimal.from(-20F), Decimal.from(-200D),
                Decimal.from(10_000_000),Decimal.from(10_000_000),Decimal.from(10_000_000),Decimal.from(10_000_000),
                null,null,null,null,
                Decimal.from(new BigDecimal("100.10")), Decimal.from(new BigDecimal("-200.10")), Decimal.from(new BigDecimal("2000.10")), null
                );

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Decimal)), List.of(one));
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2, 3), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 9, 10, 11),
                vv(Type.Int, 12, 13, 14, 15)),
                context);
        aggregator.appendGroup(input, vv(Type.Int, 3, 2, 0, 1), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 9, 10, 11),
                vv(Type.Int, 12, 13, 14, 15)),
                context);
        //@formatter:on

        ValueVector actual = aggregator.combine(context);
        assertEquals(ResolvedType.of(Type.Decimal), actual.type());
        assertEquals(4, actual.size());
        assertVectorsEquals(vv(Type.Decimal, Decimal.from("-47.500000"), Decimal.from("5714557.157143"), Decimal.from("10000000.000000"), Decimal.from(new BigDecimal("244.300000"))), actual);
    }

    @Test
    public void test_int()
    {
        //@formatter:off
        ValueVector one = vv(Type.Int,
                -10, 30, 200, -200,
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
        assertVectorsEquals(vv(Type.Int, 5, 10_000_000, null), actual);
    }

    @Test
    public void test_long()
    {
        //@formatter:off
        ValueVector one = vv(Type.Long,
                10L, -1L, 100L, -200L,
                10_000_000,10_000_000,10_000_000,10_000_000,
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
        assertVectorsEquals(vv(Type.Long, -22L, 10_000_000L, null), actual);
    }

    @Test
    public void test_long_scalar()
    {
        ValueVector smallLongs = vv(Type.Long, 10L, -1L, 100L, -200L);

        Schema schema = schema(new Type[] { Type.Long }, "col1");

        TupleVector input = TupleVector.of(schema, asList(smallLongs));

        ValueVector actual = function.evalScalar(context, input, "", asList(col1));
        assertEquals(ResolvedType.of(Type.Long), actual.type());
        assertEquals(4, actual.size());
        assertVectorsEquals(vv(Type.Long, 10L, -1L, 100L, -200L), actual);
    }

    @Test
    public void test_long_scalar_value_vector()
    {
        ValueVector smallLongs = vv(Type.Long, 10L, -1L, 100L, -200L);
        ValueVector mediumLongs = ValueVector.literalLong(10_000_000L, 4);
        ValueVector nulls = ValueVector.literalNull(ResolvedType.of(Type.Long), 4);

        Schema schema = Schema.of(Column.of("col1", ResolvedType.array(ResolvedType.of(Type.Long))));

        TupleVector input = TupleVector.of(schema, asList(vv(ResolvedType.array(ResolvedType.of(Type.Long)), smallLongs, mediumLongs, nulls)));

        ValueVector actual = function.evalScalar(context, input, "", asList(col1));
        assertEquals(ResolvedType.of(Type.Long), actual.type());
        assertEquals(3, actual.size());
        assertVectorsEquals(vv(Type.Long, -22L, 10_000_000L, null), actual);
    }

    @Test
    public void test_float()
    {
        //@formatter:off
        ValueVector one = vv(Type.Float,
                10F, -1F, 100F, -200F,
                10,10,10,10,
                10_000_000,10_000_000,10_000_000,10_000_000,
                null,null,null,null
                );

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Float)), List.of(one));
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2, 3), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 9, 10, 11),
                vv(Type.Int, 12, 13, 14, 15)),
                context);
        //@formatter:on

        ValueVector actual = aggregator.combine(context);
        assertEquals(ResolvedType.of(Type.Float), actual.type());
        assertEquals(4, actual.size());
        assertVectorsEquals(vv(Type.Float, -22.75F, 10F, 10_000_000F, null), actual);
    }

    @Test
    public void test_double()
    {
        //@formatter:off
        ValueVector one = vv(Type.Double,
                10D, -1D, 100D, -200D,
                10,10,10,10,
                10_000_000,10_000_000,10_000_000,10_000_000,
                null,null,null,null
                );

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Double)), List.of(one));
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2, 3), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 9, 10, 11),
                vv(Type.Int, 12, 13, 14, 15)),
                context);
        //@formatter:on

        ValueVector actual = aggregator.combine(context);
        assertEquals(ResolvedType.of(Type.Double), actual.type());
        assertEquals(4, actual.size());
        assertVectorsEquals(vv(Type.Double, -22.75D, 10D, 10_000_000D, null), actual);
    }
}
