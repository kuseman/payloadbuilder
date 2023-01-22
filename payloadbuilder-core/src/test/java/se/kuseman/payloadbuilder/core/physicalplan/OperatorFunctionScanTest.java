package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.io.StringWriter;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.OperatorFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector.OutputWritable;
import se.kuseman.payloadbuilder.core.JsonOutputWriter;
import se.kuseman.payloadbuilder.core.catalog.system.SystemCatalog;

/** Test of {@link OperatorFunctionScan} */
public class OperatorFunctionScanTest extends APhysicalPlanTest
{
    private final Schema schema = schema("col1", "col2");

    @Test
    public void test_object_array()
    {
        assertEquals("[{\"col1\":1,\"col2\":4},{\"col1\":2,\"col2\":5},{\"col1\":3,\"col2\":6}]",
                toJson(getValue("OBJECT_ARRAY", schema, TupleVector.of(schema, asList(vv(ResolvedType.of(Type.Any), 1, 2, 3), vv(ResolvedType.of(Type.Any), 4, 5, 6))))));
    }

    @Test
    public void test_object()
    {
        assertEquals("{\"col1\":1,\"col2\":4}", toJson(getValue("OBJECT", schema, TupleVector.of(schema, asList(vv(ResolvedType.of(Type.Any), 1, 2, 3), vv(ResolvedType.of(Type.Any), 4, 5, 6))))));
    }

    @Test
    public void test_array()
    {
        assertEquals("[1,4,2,5,3,6]", toJson(getValue("ARRAY", schema, TupleVector.of(schema, asList(vv(ResolvedType.of(Type.Any), 1, 2, 3), vv(ResolvedType.of(Type.Any), 4, 5, 6))))));
    }

    @Test
    public void test_object_array_1()
    {
        Schema schema = schema(new Type[] { Type.Any, Type.Any, Type.Boolean }, "col1", "col2", "col1 >= col2");

        assertEquals("[{\"col1\":1,\"col2\":4,\"col1 >= col2\":false},{\"col1\":2,\"col2\":5,\"col1 >= col2\":false},{\"col1\":3,\"col2\":6,\"col1 >= col2\":false}]", toJson(getValue("OBJECT_ARRAY",
                schema, TupleVector.of(schema, asList(vv(ResolvedType.of(Type.Any), 1, 2, 3), vv(ResolvedType.of(Type.Any), 4, 5, 6), vv(ResolvedType.of(Type.Boolean), false, false, false))))));
    }

    private OutputWritable getValue(String output, Schema schema, TupleVector input)
    {
        OperatorFunctionInfo function = SystemCatalog.get()
                .getOperatorFunction(output);

        MutableBoolean closed = new MutableBoolean();
        IDatasource inner = schemaDS(() -> closed.setTrue(), input);

        IPhysicalPlan plan = new OperatorFunctionScan(1, new TableScan(0, schema, new TableSourceReference("", QualifiedName.of("table"), "a"), false, inner, emptyList()), function, "",
                schema(new Type[] { Type.OutputWritable }, "expr1"));

        assertEquals(schema(new Type[] { Type.OutputWritable }, "expr1"), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(it);
        assertEquals(schema(new Type[] { Type.OutputWritable }, "expr1"), actual.getSchema());
        assertEquals(1, actual.getRowCount());

        ValueVector vv = actual.getColumn(0);
        assertEquals(ResolvedType.of(Type.OutputWritable), vv.type());
        assertEquals(1, vv.size());

        OutputWritable sv = (OutputWritable) vv.getValue(0);
        assertTrue(closed.getValue());
        return sv;
    }

    private String toJson(OutputWritable value)
    {
        StringWriter writer = new StringWriter();
        JsonOutputWriter w = new JsonOutputWriter(writer);
        value.write(w, context);
        w.close();
        return writer.toString();
    }
}
