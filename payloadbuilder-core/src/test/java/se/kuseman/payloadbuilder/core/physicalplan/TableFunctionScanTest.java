package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.core.catalog.system.SystemCatalog;

/** Test {@link TableFunctionScan} */
public class TableFunctionScanTest extends APhysicalPlanTest
{
    @Test
    public void test_that_a_table_source_is_attched_on_schema()
    {
        Schema schema = Schema.of(col("Value", ResolvedType.of(Type.Int), table));
        IPhysicalPlan plan = new TableFunctionScan(0, schema, table, "", "System", SystemCatalog.get()
                .getTableFunction("range"), asList(intLit(1), intLit(10)), emptyList());

        TupleVector vector = PlanUtils.concat(context, plan.execute(context));

        assertEquals(schema, vector.getSchema());
    }
}
