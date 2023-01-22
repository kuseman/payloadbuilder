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
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link AggregateCountFunction} */
public class AggregateCountFunctionTest extends APhysicalPlanTest
{
    private IExpression col1 = ce("col1");
    private ScalarFunctionInfo function = SystemCatalog.get()
            .getScalarFunction("count");

    @Test
    public void test_type()
    {
        assertEquals(ResolvedType.of(Type.Int), function.getType(asList(col1)));
    }

    @Test
    public void test_aggregate()
    {
        ValueVector one = ValueVector.literalObject(ResolvedType.of(Type.Any), 10, 20, -20F, -200D);
        ValueVector two = ValueVector.literalObject(ResolvedType.of(Type.Any), 10_000_000, 4);
        ValueVector three = ValueVector.literalNull(ResolvedType.of(Type.Any), 4);
        ValueVector four = ValueVector.literalObject(ResolvedType.of(Type.Any), "one", "two", "three");
        ValueVector five = ValueVector.literalObject(ResolvedType.of(Type.Any), new BigDecimal("100.10"), new BigDecimal("-200.10"), new BigDecimal("2000.10"));

        Schema schema = schema(new Type[] { Type.Any }, "col1");

        //@formatter:off
        ValueVector v = ValueVector.literalObject(ResolvedType.tupleVector(schema),
                TupleVector.of(schema, asList(one)),
                TupleVector.of(schema, asList(two)),
                TupleVector.of(schema, asList(three)),
                TupleVector.of(schema, asList(four)),
                TupleVector.of(schema, asList(five)));
        //@formatter:on

        ValueVector actual = function.evalAggregate(context, AggregateMode.ALL, v, "", asList(col1));

        assertEquals(ResolvedType.of(Type.Int), actual.type());
        assertEquals(5, actual.size());
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 4, 4, 0, 3, 3), actual);
    }

    @Test
    public void test_scalar()
    {
        ValueVector one = ValueVector.literalObject(ResolvedType.of(Type.Any), 10, 20, null, -200D);

        Schema schema = schema(new Type[] { Type.Any }, "col1");

        TupleVector input = TupleVector.of(schema, asList(one));

        ValueVector actual = function.evalScalar(context, input, "", asList(col1));

        assertEquals(ResolvedType.of(Type.Int), actual.type());
        assertEquals(4, actual.size());
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 1, 1, 0, 1), actual);
    }

    @Test
    public void test_scalar_value_vector()
    {
        ValueVector one = ValueVector.literalObject(ResolvedType.of(Type.Any), 10, 20, -20F, -200D);
        ValueVector two = ValueVector.literalObject(ResolvedType.of(Type.Any), 10_000_000, 4);
        ValueVector three = ValueVector.literalNull(ResolvedType.of(Type.Any), 4);
        ValueVector four = ValueVector.literalObject(ResolvedType.of(Type.Any), "one", "two", "three");
        ValueVector five = ValueVector.literalObject(ResolvedType.of(Type.Any), new BigDecimal("100.10"), new BigDecimal("-200.10"), new BigDecimal("2000.10"));

        Schema schema = Schema.of(Column.of("col1", ResolvedType.valueVector(ResolvedType.of(Type.Any))));

        TupleVector input = TupleVector.of(schema, asList(vv(ResolvedType.valueVector(ResolvedType.of(Type.Any)), one, two, three, four, five)));

        ValueVector actual = function.evalScalar(context, input, "", asList(col1));

        assertEquals(ResolvedType.of(Type.Int), actual.type());
        assertEquals(5, actual.size());
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 4, 4, 0, 3, 3), actual);
    }
}
