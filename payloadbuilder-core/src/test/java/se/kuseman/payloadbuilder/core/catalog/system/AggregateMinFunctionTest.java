package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.math.BigDecimal;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo.AggregateMode;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
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
        IExpression col1 = ce("col1", ResolvedType.of(Type.Any));

        ValueVector one = ValueVector.literalAny(10, 20, -20F, -200D);
        ValueVector two = ValueVector.literalAny(4, 10_000_000);
        ValueVector three = ValueVector.literalNull(ResolvedType.of(Type.Any), 4);
        ValueVector four = ValueVector.literalAny("one", "two", "three");
        ValueVector five = ValueVector.literalAny(new BigDecimal("100.10"), new BigDecimal("-200.10"), new BigDecimal("2000.10"));

        Schema schema = schema(new Type[] { Type.Any }, "col1");

        //@formatter:off
        ValueVector v = ValueVector.literalTable(
                TupleVector.of(schema, asList(one)),
                TupleVector.of(schema, asList(two)),
                TupleVector.of(schema, asList(three)),
                TupleVector.of(schema, asList(four)),
                TupleVector.of(schema, asList(five)));
        //@formatter:on

        ValueVector actual = function.evalAggregate(context, AggregateMode.ALL, v, "", asList(col1));
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(5, actual.size());
        assertVectorsEquals(vv(Type.Any, -200D, 10_000_000, null, "one", new BigDecimal("-200.10")), actual);
    }

    @Test
    public void test_int()
    {
        IExpression col1 = ce("col1", ResolvedType.of(Type.Int));

        ValueVector smallInts = vv(Type.Int, -10, 10, 200, -200);
        ValueVector mediumInts = ValueVector.literalInt(10_000_000, 4);
        ValueVector nulls = ValueVector.literalNull(ResolvedType.of(Type.Int), 4);
        ValueVector oneNull = vv(Type.Int, null, 10, null, -200);

        Schema schema = schema(new Type[] { Type.Int }, "col1");

        //@formatter:off
        ValueVector v = ValueVector.literalTable(
                TupleVector.of(schema, asList(smallInts)),
                TupleVector.of(schema, asList(mediumInts)),
                TupleVector.of(schema, asList(nulls)),
                TupleVector.of(schema, asList(oneNull)));
        //@formatter:on

        ValueVector actual = function.evalAggregate(context, AggregateMode.ALL, v, "", asList(col1));
        assertEquals(ResolvedType.of(Type.Int), actual.type());
        assertVectorsEquals(vv(Type.Int, -200, 10_000_000, null, -200), actual);
    }

    @Test
    public void test_long()
    {
        IExpression col1 = ce("col1", ResolvedType.of(Type.Long));

        ValueVector smallLongs = vv(Type.Long, 10L, -1L, 100L, -200L);
        ValueVector mediumLongs = ValueVector.literalLong(10_000_000L, 4);
        ValueVector nulls = ValueVector.literalNull(ResolvedType.of(Type.Long), 4);

        Schema schema = schema(new Type[] { Type.Long }, "col1");

        //@formatter:off
        ValueVector v = ValueVector.literalTable(
                TupleVector.of(schema, asList(smallLongs)),
                TupleVector.of(schema, asList(mediumLongs)),
                TupleVector.of(schema, asList(nulls)));
        //@formatter:on

        ValueVector actual = function.evalAggregate(context, AggregateMode.ALL, v, "", asList(col1));
        assertEquals(ResolvedType.of(Type.Long), actual.type());
        assertEquals(3, actual.size());
        assertVectorsEquals(vv(Type.Long, -200L, 10_000_000L, null), actual);
    }

    @Test
    public void test_float()
    {
        IExpression col1 = ce("col1", ResolvedType.of(Type.Float));

        ValueVector one = vv(Type.Float, 10F, -1F, 100F, -200F);
        ValueVector two = ValueVector.literalFloat(10, 4);
        ValueVector three = ValueVector.literalFloat(10_000_000, 4);
        ValueVector nulls = ValueVector.literalNull(ResolvedType.of(Type.Float), 4);

        Schema schema = schema(new Type[] { Type.Float }, "col1");

        //@formatter:off
        ValueVector v = ValueVector.literalTable(
                TupleVector.of(schema, asList(one)),
                TupleVector.of(schema, asList(two)),
                TupleVector.of(schema, asList(three)),
                TupleVector.of(schema, asList(nulls)));
        //@formatter:on

        ValueVector actual = function.evalAggregate(context, AggregateMode.ALL, v, "", asList(col1));
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

        ValueVector one = vv(Type.Double, 10D, -1D, 100D, -200D);
        ValueVector two = ValueVector.literalDouble(10, 4);
        ValueVector three = ValueVector.literalDouble(10_000_000, 4);
        ValueVector nulls = ValueVector.literalNull(ResolvedType.of(Type.Double), 4);

        Schema schema = schema(new Type[] { Type.Double }, "col1");

        //@formatter:off
        ValueVector v = ValueVector.literalTable(
                TupleVector.of(schema, asList(one)),
                TupleVector.of(schema, asList(two)),
                TupleVector.of(schema, asList(three)),
                TupleVector.of(schema, asList(nulls)));
      //@formatter:on

        ValueVector actual = function.evalAggregate(context, AggregateMode.ALL, v, "", asList(col1));
        assertEquals(ResolvedType.of(Type.Double), actual.type());
        assertEquals(4, actual.size());
        assertVectorsEquals(vv(Type.Double, -200D, 10D, 10_000_000D, null), actual);
    }
}
