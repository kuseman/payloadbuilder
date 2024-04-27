package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.math.BigDecimal;
import java.util.List;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.FunctionInfo.Arity;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo.AggregateMode;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IAggregator;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link AggregateMinMaxFunction} */
public class AggregateMinFunctionTest extends APhysicalPlanTest
{
    private ScalarFunctionInfo function = SystemCatalog.get()
            .getScalarFunction("min");

    @Test
    public void test_object()
    {
        assertEquals(Arity.ONE, function.arity());

        IExpression col1 = ce("col1", ResolvedType.of(Type.Any));

        //@formatter:off
        ValueVector one = ValueVector.literalAny(
                10, 20, -20F, -200D,
                10_000_000,10_000_000,10_000_000,10_000_000,
                null,null,null,null,
                "one", "two", "three", "four",
                new BigDecimal("100.10"), new BigDecimal("-200.10"), new BigDecimal("2000.10"), null
                
                );

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Any)), List.of(one));
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2, 3, 4), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 7),
                vv(Type.Int, 8, 11),
                vv(Type.Int, 12, 13, 14, 15),
                vv(Type.Int, 16, 17, 18, 19)),
                context);
        //@formatter:on

        ValueVector actual = aggregator.combine(context);
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(5, actual.size());
        assertVectorsEquals(vv(Type.Any, -200D, 10_000_000, null, "four", new BigDecimal("-200.10")), actual);
    }

    @Test
    public void test_int()
    {
        IExpression col1 = ce("col1", ResolvedType.of(Type.Int));

        //@formatter:off
        ValueVector one = vv(Type.Int,
                -10, 10, 200, -200,
                10_000_000,10_000_000,10_000_000,10_000_000,
                null,null,null,null,
                null, 10, null, -200
                );

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Int)), List.of(one));
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2, 3), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 11),
                vv(Type.Int, 12, 13, 14, 15)),
                context);
        //@formatter:on

        ValueVector actual = aggregator.combine(context);
        assertEquals(ResolvedType.of(Type.Int), actual.type());
        assertEquals(4, actual.size());
        assertVectorsEquals(vv(Type.Int, -200, 10_000_000, null, -200), actual);
    }

    @Test
    public void test_int_multi_vectors()
    {
        IExpression col1 = ce("col1", ResolvedType.of(Type.Int));

        //@formatter:off
        ValueVector one = vv(Type.Int,
                -10, 10, 200, -200,
                10_000_000,10_000_000,10_000_000,10_000_000,
                null,null,null,null,
                null, 10, null, -200
                );

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Int)), List.of(one));
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2, 3), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 11),
                vv(Type.Int, 12, 13, 14, 15)),
                context);
        
        one = vv(Type.Int,
                -10, 10, 200, -200,
                -10_000_000,-10_000_000,-10_000_000,-10_000_000,
                null,null,null,null,
                null, 10, null, -200
                );

        input = TupleVector.of(Schema.of(Column.of("col1", Type.Int)), List.of(one));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2, 3), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 11),
                vv(Type.Int, 12, 13, 14, 15)),
                context);

        ValueVector actual = aggregator.combine(context);
        assertEquals(ResolvedType.of(Type.Int), actual.type());
        assertVectorsEquals(vv(Type.Int, -200, -10_000_000, null, -200), actual);
    }

    @Test
    public void test_long()
    {
        IExpression col1 = ce("col1", ResolvedType.of(Type.Long));

        //@formatter:off
        ValueVector one = vv(Type.Long,
                10L, -1L, 100L, -200L,
                10_000_000L,10_000_000L,10_000_000L,10_000_000L,
                null,null,null,null
                );

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Long)), List.of(one));
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 11)),
                context);
        //@formatter:on

        ValueVector actual = aggregator.combine(context);
        assertEquals(ResolvedType.of(Type.Long), actual.type());
        assertEquals(3, actual.size());
        assertVectorsEquals(vv(Type.Long, -200L, 10_000_000L, null), actual);
    }

    @Test
    public void test_float()
    {
        IExpression col1 = ce("col1", ResolvedType.of(Type.Float));

        //@formatter:off
        ValueVector one = vv(Type.Float,
                10F, -1F, 100F, -200F,
                10F,10F,10F,10F,
                10_000_000,10_000_000,10_000_000,10_000_000,
                null,null,null,null
                );

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Float)), List.of(one));
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2, 3), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 11),
                vv(Type.Int, 12, 14)),
                context);
        //@formatter:on

        ValueVector actual = aggregator.combine(context);
        assertEquals(ResolvedType.of(Type.Float), actual.type());
        assertEquals(4, actual.size());
        assertVectorsEquals(vv(Type.Float, -200F, 10F, 10_000_000F, null), actual);
    }

    @Test
    public void test_float_scalar()
    {
        IExpression col1 = ce("col1", ResolvedType.of(Type.Float));

        ValueVector one = vv(Type.Float, 10F, -1F, 100F, -200F);

        Schema schema = schema(new Type[] { Type.Float }, "col1");

        TupleVector input = TupleVector.of(schema, asList(one));

        ValueVector actual = function.evalScalar(context, input, "", asList(col1));
        assertEquals(ResolvedType.of(Type.Float), actual.type());
        assertEquals(4, actual.size());
        assertVectorsEquals(vv(Type.Float, 10F, -1F, 100F, -200F), actual);
    }

    @Test
    public void test_float_scalar_value_vector()
    {
        IExpression col1 = ce("col1", ResolvedType.array(ResolvedType.of(Type.Float)));

        ValueVector one = vv(Type.Float, 10F, -1F, 100F, -200F);
        ValueVector two = ValueVector.literalFloat(10, 4);
        ValueVector three = ValueVector.literalFloat(10_000_000, 4);
        ValueVector nulls = ValueVector.literalNull(ResolvedType.of(Type.Float), 4);

        Schema schema = Schema.of(Column.of("col1", ResolvedType.array(ResolvedType.of(Type.Float))));

        TupleVector input = TupleVector.of(schema, asList(vv(ResolvedType.array(ResolvedType.of(Type.Float)), one, two, three, nulls)));

        ValueVector actual = function.evalScalar(context, input, "", asList(col1));
        assertEquals(ResolvedType.of(Type.Float), actual.type());
        assertEquals(4, actual.size());
        assertVectorsEquals(vv(Type.Float, -200F, 10F, 10_000_000F, null), actual);
    }

    @Test
    public void test_double()
    {
        IExpression col1 = ce("col1", ResolvedType.of(Type.Double));

        //@formatter:off
        ValueVector one = vv(Type.Double,
                10D, -1D, 100D, -200D,
                10 ,10,10,10,
                10_000_000,10_000_000,10_000_000,10_000_000,
                null,null,null,null
                );

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Double)), List.of(one));
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2, 3), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 6, 7),
                vv(Type.Int, 8, 11),
                vv(Type.Int, 12, 14)),
                context);
        //@formatter:on

        ValueVector actual = aggregator.combine(context);
        assertEquals(ResolvedType.of(Type.Double), actual.type());
        assertEquals(4, actual.size());
        assertVectorsEquals(vv(Type.Double, -200D, 10D, 10_000_000D, null), actual);
    }
}
