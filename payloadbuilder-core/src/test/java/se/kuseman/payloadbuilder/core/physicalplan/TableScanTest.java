package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ColumnReference;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.core.QueryException;

/** Test {@link TableScan} */
public class TableScanTest extends APhysicalPlanTest
{
    @Test
    public void test_that_a_table_source_is_attched_on_schema()
    {
        MutableBoolean closed = new MutableBoolean(false);
        Schema schema = Schema.of(Column.of("col", Type.Int));
        IPhysicalPlan plan = scan(
                schemaLessDS(() -> closed.setTrue(), TupleVector.of(schema, asList(ValueVector.literalInt(100, 100))), TupleVector.of(schema, asList(ValueVector.literalInt(100, 100)))), table,
                schema);

        assertEquals(schema, plan.getSchema());

        TupleVector vector = PlanUtils.concat(plan.execute(context));

        assertEquals(Schema.of(Column.of(table.column("col"), ResolvedType.of(Type.Int))), vector.getSchema());
    }

    @Test
    public void test_that_a_table_source_is_rejecting_asterisk_in_actual_schema()
    {
        MutableBoolean closed = new MutableBoolean(false);
        // Asterisk schema
        Schema schema = Schema.of(Column.of(new ColumnReference(table, "t", ColumnReference.Type.ASTERISK), ResolvedType.of(Type.Int)));
        IPhysicalPlan plan = scan(
                schemaLessDS(() -> closed.setTrue(), TupleVector.of(schema, asList(ValueVector.literalInt(100, 100))), TupleVector.of(schema, asList(ValueVector.literalInt(100, 100)))), table,
                schema);

        assertEquals(schema, plan.getSchema());

        try
        {
            PlanUtils.concat(plan.execute(context));
            fail("Should fail because of asterisks");
        }
        catch (QueryException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Runtime tuple vectors cannot contain asterisk columns"));
        }
    }
}
