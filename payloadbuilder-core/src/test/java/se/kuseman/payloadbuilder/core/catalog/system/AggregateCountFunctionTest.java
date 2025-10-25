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
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

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
        assertEquals(Arity.ONE, function.arity());

        //@formatter:off
        ValueVector one = VectorTestUtils.vv(Type.Any,
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

        assertEquals(ResolvedType.of(Type.Int), actual.type());
        assertEquals(5, actual.size());
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 4, 3, 0, 4, 3), actual);
    }

    @Test
    public void test_aggregate_asterisk()
    {
        assertEquals(Arity.ONE, function.arity());

        //@formatter:off
        ValueVector one = VectorTestUtils.vv(Type.Any,
                10, 20, -20F, -200D,
                10_000_000,10_000_000,10_000_000,10_000_000,
                null,null,null,null,
                "one", "two", "three", "four",
                new BigDecimal("100.10"), new BigDecimal("-200.10"), new BigDecimal("2000.10"), null
                
                );

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Any)), List.of(one));
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(new AsteriskExpression(null)));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2, 3, 4), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 7),
                vv(Type.Int, 8, 11),
                vv(Type.Int, 12, 13, 14, 15),
                vv(Type.Int, 16, 17, 18, 19)),
                context);
        //@formatter:on

        ValueVector actual = aggregator.combine(context);

        assertEquals(ResolvedType.of(Type.Int), actual.type());
        assertEquals(5, actual.size());
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 4, 3, 2, 4, 4), actual);
    }

    @Test
    public void test_aggregate_constant()
    {
        assertEquals(Arity.ONE, function.arity());

        //@formatter:off
        ValueVector one = VectorTestUtils.vv(Type.Any,
                10, 20, -20F, -200D,
                10_000_000,10_000_000,10_000_000,10_000_000,
                null,null,null,null,
                "one", "two", "three", "four",
                new BigDecimal("100.10"), new BigDecimal("-200.10"), new BigDecimal("2000.10"), null
                
                );

        TupleVector input = TupleVector.of(Schema.of(Column.of("col1", Type.Any)), List.of(one));
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "", asList(intLit(10)));
        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2, 3, 4), vv(ResolvedType.array(Type.Int),
                vv(Type.Int, 0, 1, 2, 3),
                vv(Type.Int, 4, 5, 7),
                vv(Type.Int, 8, 11),
                vv(Type.Int, 12, 13, 14, 15),
                vv(Type.Int, 16, 17, 18, 19)),
                context);
        //@formatter:on

        ValueVector actual = aggregator.combine(context);

        assertEquals(ResolvedType.of(Type.Int), actual.type());
        assertEquals(5, actual.size());
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 4, 3, 2, 4, 4), actual);
    }

    @Test
    public void test_scalar()
    {
        ValueVector one = VectorTestUtils.vv(Type.Any, 10, 20, null, -200D);

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
        ValueVector one = VectorTestUtils.vv(Type.Any, 10, 20, -20F, -200D);
        ValueVector two = ValueVector.literalAny(4, 10_000_000);
        ValueVector three = ValueVector.literalNull(ResolvedType.of(Type.Any), 4);
        ValueVector four = VectorTestUtils.vv(Type.Any, "one", "two", "three");
        ValueVector five = VectorTestUtils.vv(Type.Any, new BigDecimal("100.10"), new BigDecimal("-200.10"), new BigDecimal("2000.10"));

        Schema schema = Schema.of(Column.of("col1", ResolvedType.array(ResolvedType.of(Type.Any))));

        TupleVector input = TupleVector.of(schema, asList(vv(ResolvedType.array(ResolvedType.of(Type.Any)), one, two, three, four, five)));

        ValueVector actual = function.evalScalar(context, input, "", asList(col1));

        assertEquals(ResolvedType.of(Type.Int), actual.type());
        assertEquals(5, actual.size());
        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 4, 4, 0, 3, 3), actual);
    }
}
