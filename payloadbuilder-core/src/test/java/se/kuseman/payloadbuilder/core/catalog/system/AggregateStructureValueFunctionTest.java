package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.io.StringWriter;
import java.util.List;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo.AggregateMode;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector.OutputWritable;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.JsonOutputWriter;
import se.kuseman.payloadbuilder.core.expression.ComparisonExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link AggregateStructureValueFunction} */
public class AggregateStructureValueFunctionTest extends APhysicalPlanTest
{
    private IExpression col1 = ce("col1");
    private IExpression col2 = ce("col2");

    @Test
    public void test_null_when_no_rows()
    {
        List<IExpression> list = asList(col1);
        assertNull(getValue(AStructureValueFunction.OutputType.OBJECT_ARRAY, TupleVector.EMPTY, list));
    }

    @Test
    public void test_object_array()
    {
        List<IExpression> list = asList(col1, col2);
        assertEquals("[{\"col1\":1,\"col2\":4},{\"col1\":2,\"col2\":5},{\"col1\":3,\"col2\":6}]",
                toJson(getValue(AStructureValueFunction.OutputType.OBJECT_ARRAY, TupleVector.of(schema("col1", "col2"), asList(vv(Type.Any, 1, 2, 3), vv(Type.Any, 4, 5, 6))), list)));
    }

    @Test
    public void test_object()
    {
        List<IExpression> list = asList(col1, col2);
        assertEquals("{\"col1\":1,\"col2\":4}",
                toJson(getValue(AStructureValueFunction.OutputType.OBJECT, TupleVector.of(schema("col1", "col2"), asList(vv(Type.Any, 1, 2, 3), vv(Type.Any, 4, 5, 6))), list)));
    }

    @Test
    public void test_object_asterisk()
    {
        List<IExpression> list = asList(col1, col2, col1);
        assertEquals("{\"col1\":1,\"col2\":4,\"col1\":1}",
                toJson(getValue(AStructureValueFunction.OutputType.OBJECT, TupleVector.of(schema("col1", "col2"), asList(vv(Type.Any, 1, 2, 3), vv(Type.Any, 4, 5, 6))), list)));
    }

    @Test
    public void test_array()
    {
        List<IExpression> list = asList(col1, col2);
        assertEquals("[1,4,2,5,3,6]", toJson(getValue(AStructureValueFunction.OutputType.ARRAY, TupleVector.of(schema("col1", "col2"), asList(vv(Type.Any, 1, 2, 3), vv(Type.Any, 4, 5, 6))), list)));
    }

    @Test
    public void test_object_array_1()
    {
        List<IExpression> list = asList(col1, col2, new ComparisonExpression(IComparisonExpression.Type.GREATER_THAN_EQUAL, col1, col2));
        assertEquals("[{\"col1\":1,\"col2\":4,\"\":false},{\"col1\":2,\"col2\":5,\"\":false},{\"col1\":3,\"col2\":6,\"\":false}]",
                toJson(getValue(AStructureValueFunction.OutputType.OBJECT_ARRAY, TupleVector.of(schema("col1", "col2"), asList(vv(Type.Any, 1, 2, 3), vv(Type.Any, 4, 5, 6))), list)));
    }

    private OutputWritable getValue(AStructureValueFunction.OutputType type, TupleVector input, List<IExpression> expressions)
    {
        ScalarFunctionInfo function = SystemCatalog.get()
                .getScalarFunction(type.name());
        assertEquals(Type.OutputWritable, function.getType(expressions)
                .getType());
        ValueVector vv = ValueVector.literalObject(ResolvedType.tupleVector(input.getSchema()), input, 1);
        ValueVector actual = function.evalAggregate(context, AggregateMode.ALL, vv, "", expressions);

        assertEquals(ResolvedType.of(Type.OutputWritable), actual.type());
        assertEquals(1, actual.size());
        assertEquals(true, actual.isNullable());
        assertEquals(input.getRowCount() == 0, actual.isNull(0));
        return (OutputWritable) actual.getValue(0);
    }

    private String toJson(OutputWritable value)
    {
        StringWriter writer = new StringWriter();
        JsonOutputWriter w = new JsonOutputWriter(writer);
        value.write(w, null);
        w.close();
        return writer.toString();
    }
}
