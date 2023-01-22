package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;

/** Test of {@link Concatenation} */
public class ConcatenationTest extends APhysicalPlanTest
{
    @Test
    public void test()
    {
        MutableBoolean closed1 = new MutableBoolean();
        MutableBoolean closed2 = new MutableBoolean();
        MutableBoolean closed3 = new MutableBoolean();

        Schema schema1 = schema("col1", "col2");
        Schema schema2 = schema("col3", "col4");
        Schema schema3 = schema("col5", "col6");

        IPhysicalPlan input1 = scan(schemaLessDS(() -> closed1.setTrue(), TupleVector.of(schema1, asList(vv(ResolvedType.of(Type.Any), 1, 2, 3), vv(ResolvedType.of(Type.Any), 4, 5, 6)))), table,
                Schema.EMPTY);
        IPhysicalPlan input2 = scan(schemaLessDS(() -> closed2.setTrue(), TupleVector.of(schema2, asList(vv(ResolvedType.of(Type.Any), 10, 20, 30), vv(ResolvedType.of(Type.Any), 40, 50, 60)))), table,
                Schema.EMPTY);
        IPhysicalPlan input3 = scan(schemaLessDS(() -> closed3.setTrue(), TupleVector.of(schema3, asList(vv(ResolvedType.of(Type.Any), 100, 200, 300), vv(ResolvedType.of(Type.Any), 400, 500, 600)))),
                table, Schema.EMPTY);

        IPhysicalPlan plan = new Concatenation(1, asList(input1, input2, input3));

        assertEquals(Schema.EMPTY, plan.getSchema());

        TupleVector actual = PlanUtils.concat(context.getBufferAllocator(), plan.execute(context));

        assertEquals(Schema.of(CoreColumn.of(table.column("col1"), ResolvedType.of(Type.Any)), CoreColumn.of(table.column("col2"), ResolvedType.of(Type.Any))), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 1, 2, 3, 10, 20, 30, 100, 200, 300), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 5, 6, 40, 50, 60, 400, 500, 600), actual.getColumn(1));

        assertTrue(closed1.booleanValue());
        assertTrue(closed2.booleanValue());
        assertTrue(closed3.booleanValue());
    }
}
