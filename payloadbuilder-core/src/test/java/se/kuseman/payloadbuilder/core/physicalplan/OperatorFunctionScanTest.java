package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.io.StringWriter;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.OperatorFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.JsonOutputWriter;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.catalog.system.SystemCatalog;
import se.kuseman.payloadbuilder.core.execution.OutputWriterUtils;

/** Test of {@link OperatorFunctionScan} */
public class OperatorFunctionScanTest extends APhysicalPlanTest
{
    private final TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "a");
    private final Schema schema = Schema.of(col("col1", ResolvedType.of(Type.Any), table), col("col2", ResolvedType.of(Type.Any), table));

    @Test
    public void test_object_array()
    {
        assertEquals("[{\"col1\":1,\"col2\":4},{\"col1\":2,\"col2\":5},{\"col1\":3,\"col2\":6}]",
                toJson(getValue("OBJECT_ARRAY", schema, TupleVector.of(schema, asList(vv(ResolvedType.of(Type.Any), 1, 2, 3), vv(ResolvedType.of(Type.Any), 4, 5, 6))))));
    }

    // @Test
    // public void test_object()
    // {
    // assertEquals("{\"col1\":1,\"col2\":4}", toJson(getValue("OBJECT", schema, TupleVector.of(schema, asList(vv(ResolvedType.of(Type.Any), 1, 2, 3), vv(ResolvedType.of(Type.Any), 4, 5, 6))))));
    // }
    //
    // @Test
    // public void test_array()
    // {
    // assertEquals("[1,4,2,5,3,6]", toJson(getValue("ARRAY", schema, TupleVector.of(schema, asList(vv(ResolvedType.of(Type.Any), 1, 2, 3), vv(ResolvedType.of(Type.Any), 4, 5, 6))))));
    // }

    @Test
    public void test_object_array_1()
    {
        Schema schema = Schema.of(col("col1", ResolvedType.of(Type.Any), table), col("col2", ResolvedType.of(Type.Any), table), col("col1 >= col2", ResolvedType.of(Type.Boolean), table));

        assertEquals("[{\"col1\":1,\"col2\":4,\"col1 >= col2\":false},{\"col1\":2,\"col2\":5,\"col1 >= col2\":false},{\"col1\":3,\"col2\":6,\"col1 >= col2\":false}]", toJson(getValue("OBJECT_ARRAY",
                schema, TupleVector.of(schema, asList(vv(ResolvedType.of(Type.Any), 1, 2, 3), vv(ResolvedType.of(Type.Any), 4, 5, 6), vv(ResolvedType.of(Type.Boolean), false, false, false))))));
    }

    private TupleVector getValue(String output, Schema schema, TupleVector input)
    {
        OperatorFunctionInfo function = SystemCatalog.get()
                .getOperatorFunction(output);

        MutableBoolean closed = new MutableBoolean();
        IDatasource inner = schemaDS(() -> closed.setTrue(), input);

        IPhysicalPlan plan = new OperatorFunctionScan(1, new TableScan(0, schema, table, "", inner, emptyList()), function, "", Schema.of(Column.of("expr1", ResolvedType.table(schema))));

        assertEquals(Schema.of(Column.of("expr1", ResolvedType.table(schema))), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);
        assertEquals(Schema.of(Column.of("expr1", ResolvedType.table(schema))), actual.getSchema());
        assertEquals(1, actual.getRowCount());

        ValueVector vv = actual.getColumn(0);
        assertEquals(ResolvedType.table(schema), vv.type());
        assertEquals(1, vv.size());

        TupleVector sv = vv.getTable(0);
        assertTrue(closed.get());
        return sv;
    }

    private String toJson(TupleVector value)
    {
        StringWriter writer = new StringWriter();
        JsonOutputWriter w = new JsonOutputWriter(writer);
        OutputWriterUtils.write(value, w, context, false);
        w.close();
        return writer.toString();
    }
}
