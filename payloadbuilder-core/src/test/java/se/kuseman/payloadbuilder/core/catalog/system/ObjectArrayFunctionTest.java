package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.OperatorFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo.AggregateMode;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IAggregator;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link AggregateObjectArrayFunction} and {@link OperatorObjectArrayFunction} */
public class ObjectArrayFunctionTest extends APhysicalPlanTest
{
    private final ScalarFunctionInfo scalar = SystemCatalog.get()
            .getScalarFunction("object_array");
    private final OperatorFunctionInfo operator = SystemCatalog.get()
            .getOperatorFunction("object_array");
    private final IExpression col1 = ce("col1");
    private final IExpression col2 = ce("col2");

    @Test
    public void test_aggregate()
    {
        TupleVector input;
        ValueVector actual;
        Schema schema;

        // No input rows
        input = TupleVector.EMPTY;

        //@formatter:off
        IAggregator aggregator = scalar.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int), vv(ResolvedType.array(Type.Int), vv(Type.Int)), context);
        
        //@formatter:on

        actual = aggregator.combine(context);
        schema = Schema.of(col("col1", Column.Type.Any, null));
        assertEquals(ResolvedType.table(schema), scalar.getAggregateType(asList(col1)));
        assertVectorsEquals(vv(ResolvedType.table(schema)), actual);

        input = TupleVector.of(schema("col1", "col2"), asList(vv(Type.Any, 1, 2, 3), vv(Type.Any, 4, 5, 6)));

        //@formatter:off
        aggregator = scalar.createAggregator(AggregateMode.ALL, "", asList(col1));
        aggregator.appendGroup(input, vv(Type.Int, 0), vv(ResolvedType.array(Type.Int), vv(Type.Int, 0, 1, 2)), context);
        //@formatter:on

        actual = aggregator.combine(context);
        assertVectorsEquals(vv(ResolvedType.table(schema), TupleVector.of(schema, asList(vv(Type.Any, 1, 2, 3)))), actual);

        //@formatter:off
        aggregator = scalar.createAggregator(AggregateMode.ALL, "", asList(col1, col2));
        aggregator.appendGroup(input, vv(Type.Int, 0), vv(ResolvedType.array(Type.Int), vv(Type.Int, 0, 1, 2)), context);
        //@formatter:on

        actual = aggregator.combine(context);
        schema = Schema.of(col("col1", Column.Type.Any, null), col("col2", Column.Type.Any, null));
        assertVectorsEquals(vv(ResolvedType.table(schema), TupleVector.of(schema, asList(vv(Type.Any, 1, 2, 3), vv(Type.Any, 4, 5, 6)))), actual);

        // Multi vector
        //@formatter:off
        aggregator = scalar.createAggregator(AggregateMode.ALL, "", asList(col1, col2));
        aggregator.appendGroup(input, vv(Type.Int, 0), vv(ResolvedType.array(Type.Int), vv(Type.Int, 0, 1, 2)), context);

        aggregator.appendGroup(input, vv(Type.Int, 0), vv(ResolvedType.array(Type.Int), vv(Type.Int, 0, 1, 2)), context);
        //@formatter:on

        actual = aggregator.combine(context);
        schema = Schema.of(col("col1", Column.Type.Any, null), col("col2", Column.Type.Any, null));
        assertVectorsEquals(vv(ResolvedType.table(schema), TupleVector.of(schema, asList(vv(Type.Any, 1, 2, 3, 1, 2, 3), vv(Type.Any, 4, 5, 6, 4, 5, 6)))), actual);
    }

    @Test
    public void test_operator()
    {
        TupleVector input;
        ValueVector actual;

        Schema schema = Schema.of(Column.of("col1", Column.Type.Any), Column.of("col2", Column.Type.Any));

        // No input rows
        input = TupleVector.EMPTY;
        actual = operator.eval(context, "", input);
        assertVectorsEquals(vv(ResolvedType.table(Schema.EMPTY), (TupleVector) null), actual);

        input = TupleVector.of(schema, asList(vv(Type.Any, 1, 2, 3), vv(Type.Any, 4, 5, 6)));
        assertEquals(operator.getType(input.getSchema()), ResolvedType.table(schema));

        actual = operator.eval(context, "", input);
        assertVectorsEquals(vv(ResolvedType.table(schema), TupleVector.of(schema, asList(vv(Type.Any, 1, 2, 3), vv(Type.Any, 4, 5, 6)))), actual);
    }
}
