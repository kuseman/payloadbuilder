package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.execution.TemporaryTable;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link SelectIntoTempTableSink} */
class SelectIntoTempTableSinkTest extends APhysicalPlanTest
{
    @Test
    void test_vector_is_materialised_not_raw_scan_vector()
    {
        // Regression: SelectIntoTempTableSink previously used PlanUtils.concat which has a
        // single-batch fast-path returning the raw TupleVector (e.g. TableScan$1$1) directly.
        // That anonymous class holds this$1 -> outer iterator -> val$context -> ExecutionContext
        // -> QuerySession -> temporaryTables, retaining the entire execution context chain for
        // the lifetime of the cached TemporaryTable.
        // The fix forces materialisation through TupleVectorBuilder so the vector is a
        // TupleVector$4 (from TupleVector.of) with no reference to any ExecutionContext.
        Schema schema = Schema.of(col("col1", Type.Int));
        TupleVector data = TupleVector.of(schema, List.of(VectorTestUtils.vv(Type.Int, 1, 2, 3)));

        SelectIntoTempTableSink sink = new SelectIntoTempTableSink(QualifiedName.of("#", "table"), emptyList(), true);
        sink.execute(context, () -> new se.kuseman.payloadbuilder.api.execution.TupleIterator()
        {
            boolean hasNext = true;

            @Override
            public boolean hasNext()
            {
                return hasNext;
            }

            @Override
            public TupleVector next()
            {
                hasNext = false;
                return data;
            }
        });

        TemporaryTable table = ((QuerySession) context.getSession()).getTemporaryTable(QualifiedName.of("table"));
        TupleVector vector = table.getTupleVector();

        // The vector must be a materialised TupleVector (from TupleVector.of / TupleVectorBuilder),
        // NOT the raw TupleVector passed in (which could be a scan anonymous class holding a context).
        assertFalse(vector == data, "TemporaryTable must not store the raw input TupleVector — it must be materialised");

        // The enclosing class of the materialised vector is TupleVector (from TupleVector.of()).
        // A raw TableScan$1$1 would have enclosing class TableScan$1 -> TableScan.
        assertSame(TupleVector.class, vector.getClass()
                .getEnclosingClass(), "TemporaryTable vector must be a TupleVector.of() instance with no external context references");

        // Data is intact
        assertEquals(3, vector.getRowCount());
        VectorTestUtils.assertVectorsEquals(VectorTestUtils.vv(Type.Int, 1, 2, 3), vector.getColumn(0));
    }
}
