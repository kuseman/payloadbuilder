package se.kuseman.payloadbuilder.core.catalog.system;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.List;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.FunctionInfo.Arity;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo.AggregateMode;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.expression.IAggregator;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link StringAggFunction} */
class StringAggFunctionTest extends APhysicalPlanTest
{
    private IExpression col1 = ce("col1");
    private ScalarFunctionInfo function = SystemCatalog.get()
            .getScalarFunction("string_agg");

    @Test
    void test_misc()
    {
        assertEquals(ResolvedType.of(Type.String), function.getType(List.of(col1)));
        assertEquals(ResolvedType.of(Type.String), function.getAggregateType(List.of(col1)));
        assertEquals(Arity.TWO, function.arity());

        try
        {
            function.evalScalar(context, AggregateMode.DISTINCT, TupleVector.CONSTANT, "sys", List.of(col1));
            fail("");
        }
        catch (UnsupportedOperationException e)
        {
            assertTrue(e.getMessage()
                    .contains("string_agg DISTINCT is unsupported"), e.getMessage());
        }

        try
        {
            function.createAggregator(AggregateMode.DISTINCT, "sys", List.of(col1));
            fail("");
        }
        catch (UnsupportedOperationException e)
        {
            assertTrue(e.getMessage()
                    .contains("string_agg DISTINCT is unsupported"), e.getMessage());
        }

        try
        {
            function.createAggregator(AggregateMode.ALL, "sys", List.of(col1, col1));
            fail("");
        }
        catch (QueryException e)
        {
            assertTrue(e.getMessage()
                    .contains("Delimiter to function string_agg in aggregate mode must be a constant of a variable expression"), e.getMessage());
        }
    }

    @Test
    void test_aggregate()
    {
        IAggregator aggregator = function.createAggregator(AggregateMode.ALL, "sys", List.of(col1, new LiteralStringExpression("|")));

        //@formatter:off
        TupleVector input = TupleVector.of(Schema.of(
                Column.of("col1", Type.Any),
                Column.of("col2", Type.Any)
                ), List.of(
                        vv(Type.Any, 1,2,3,4,5,6,null,null,null),
                        vv(Type.Any, 10,20,30,40,50,60,null,null,null)
                        ));
        //@formatter:on

        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2), vv(ResolvedType.array(Type.Int), vv(Type.Int, 0, 1, 2), vv(Type.Int, 3, 4, 5), vv(Type.Int, 6, 7, 8)), context);

        VectorTestUtils.assertVectorsEquals(vv(Type.String, "1|2|3", "4|5|6", null), aggregator.combine(context));

        aggregator.appendGroup(input, vv(Type.Int, 0, 1, 2), vv(ResolvedType.array(Type.Int), vv(Type.Int, 0, 1, 2), vv(Type.Int, 3, 4, 5), vv(Type.Int, 6, 7, 8)), context);

        VectorTestUtils.assertVectorsEquals(vv(Type.String, "1|2|3|1|2|3", "4|5|6|4|5|6", null), aggregator.combine(context));
    }
}
