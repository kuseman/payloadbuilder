package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo.AggregateMode;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

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

        e = ce("col", ResolvedType.valueVector(ResolvedType.of(Type.Float)));
        assertEquals(ResolvedType.of(Type.Float), function.getType(asList(e)));
    }

    @Test
    public void test_object()
    {
        ValueVector smallInts = ValueVector.literalObject(ResolvedType.of(Type.Any), 10, 4);
        ValueVector mediumInts = ValueVector.literalObject(ResolvedType.of(Type.Any), 10_000_000, 4);
        ValueVector nulls = ValueVector.literalNull(ResolvedType.of(Type.Any), 4);

        Schema schema = schema(new Type[] { Type.Any }, "col1");

        //@formatter:off
        ValueVector v = ValueVector.literalObject(ResolvedType.tupleVector(schema), 
                TupleVector.of(schema, asList(smallInts)), 
                TupleVector.of(schema, asList(mediumInts)),
                TupleVector.of(schema, asList(nulls)));
        //@formatter:on

        ValueVector actual = function.evalAggregate(context, AggregateMode.ALL, v, "", asList(col1));
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(3, actual.size());
        assertVectorsEquals(vv(Type.Any, 40, 40_000_000, null), actual);
    }

    @Test
    public void test_object_scalar()
    {
        ValueVector mediumInts = ValueVector.literalObject(ResolvedType.of(Type.Any), 10_000_000, 4);
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

        Schema schema = Schema.of(Column.of("col1", ResolvedType.valueVector(ResolvedType.of(Type.Any))));

        TupleVector input = TupleVector.of(schema, asList(vv(ResolvedType.valueVector(ResolvedType.of(Type.Any)), smallInts, mediumInts, nulls)));

        ValueVector actual = function.evalScalar(context, input, "", asList(col1));

        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(3, actual.size());
        assertVectorsEquals(vv(Type.Any, 40, 40_000_000, null), actual);
    }

    @Test
    public void test_int()
    {
        ValueVector smallInts = ValueVector.literalInt(10, 4);
        ValueVector mediumInts = ValueVector.literalInt(10_000_000, 4);
        ValueVector nulls = ValueVector.literalNull(ResolvedType.of(Type.Int), 4);

        Schema schema = schema(new Type[] { Type.Int }, "col1");

        ValueVector v = ValueVector.literalObject(ResolvedType.tupleVector(schema), TupleVector.of(schema, asList(smallInts)), TupleVector.of(schema, asList(mediumInts)),
                TupleVector.of(schema, asList(nulls)));

        ValueVector actual = function.evalAggregate(context, AggregateMode.ALL, v, "", asList(col1));
        assertEquals(ResolvedType.of(Type.Int), actual.type());
        assertEquals(3, actual.size());
        assertVectorsEquals(vv(Type.Int, 40, 40_000_000, null), actual);
    }

    @Test
    public void test_long()
    {
        ValueVector smallLongs = ValueVector.literalLong(10L, 4);
        ValueVector mediumLongs = ValueVector.literalLong(10_000_000L, 4);
        ValueVector nulls = ValueVector.literalNull(ResolvedType.of(Type.Long), 4);

        Schema schema = schema(new Type[] { Type.Long }, "col1");

        ValueVector v = ValueVector.literalObject(ResolvedType.tupleVector(schema), TupleVector.of(schema, asList(smallLongs)), TupleVector.of(schema, asList(mediumLongs)),
                TupleVector.of(schema, asList(nulls)));

        ValueVector actual = function.evalAggregate(context, AggregateMode.ALL, v, "", asList(col1));
        assertEquals(ResolvedType.of(Type.Long), actual.type());
        assertEquals(3, actual.size());
        assertVectorsEquals(vv(Type.Long, 40L, 40_000_000L, null), actual);
    }

    @Test
    public void test_float()
    {
        ValueVector smallInts = ValueVector.literalFloat(10, 4);
        ValueVector mediumInts = ValueVector.literalFloat(10_000_000, 4);
        ValueVector nulls = ValueVector.literalNull(ResolvedType.of(Type.Float), 4);

        Schema schema = schema(new Type[] { Type.Float }, "col1");

        ValueVector v = ValueVector.literalObject(ResolvedType.tupleVector(schema), TupleVector.of(schema, asList(smallInts)), TupleVector.of(schema, asList(mediumInts)),
                TupleVector.of(schema, asList(nulls)));

        ValueVector actual = function.evalAggregate(context, AggregateMode.ALL, v, "", asList(col1));
        assertEquals(ResolvedType.of(Type.Float), actual.type());
        assertEquals(3, actual.size());
        assertVectorsEquals(vv(Type.Float, 40F, 40_000_000F, null), actual);
    }

    @Test
    public void test_double()
    {
        ValueVector smallInts = ValueVector.literalDouble(10, 4);
        ValueVector mediumInts = ValueVector.literalDouble(10_000_000, 4);
        ValueVector nulls = ValueVector.literalNull(ResolvedType.of(Type.Double), 4);

        Schema schema = schema(new Type[] { Type.Double }, "col1");

        //@formatter:off
        ValueVector v = ValueVector.literalObject(ResolvedType.tupleVector(schema),
                TupleVector.of(schema, asList(mediumInts)),
                TupleVector.of(schema, asList(smallInts)),
                TupleVector.of(schema, asList(nulls)));
        //@formatter:on

        ValueVector actual = function.evalAggregate(context, AggregateMode.ALL, v, "", asList(col1));
        assertEquals(ResolvedType.of(Type.Double), actual.type());
        assertEquals(3, actual.size());
        assertVectorsEquals(vv(Type.Double, 40_000_000D, 40D, null), actual);
    }

    @Test(
            expected = ArithmeticException.class)
    public void test_long_overflow()
    {
        ValueVector longs = ValueVector.literalLong(Long.MAX_VALUE, Integer.MAX_VALUE);

        Schema schema = schema(new Type[] { Type.Long }, "col1");

        ValueVector v = ValueVector.literalObject(ResolvedType.tupleVector(schema), TupleVector.of(schema, asList(longs)));
        function.evalAggregate(context, AggregateMode.ALL, v, "", asList(col1));
    }

    @Test(
            expected = ArithmeticException.class)
    public void test_overflow_int()
    {
        ValueVector smallInts = ValueVector.literalInt(10, 4);
        ValueVector largeInts = ValueVector.literalInt(Integer.MAX_VALUE / 3, 4);
        ValueVector mediumInts = ValueVector.literalInt(10_000_000, 4);
        ValueVector nulls = ValueVector.literalNull(ResolvedType.of(Type.Int), 4);

        Schema schema = schema(new Type[] { Type.Int }, "col1");

        //@formatter:off
        ValueVector v = ValueVector.literalObject(ResolvedType.tupleVector(schema),
                TupleVector.of(schema, asList(smallInts)),
                TupleVector.of(schema, asList(largeInts)),
                TupleVector.of(schema, asList(mediumInts)),
                TupleVector.of(schema, asList(nulls)));
        //@formatter:on
        function.evalAggregate(context, AggregateMode.ALL, v, "", asList(col1));
    }
}
