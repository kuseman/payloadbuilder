package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.NoSuchElementException;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.QueryException;

/** Test of {@link Assert} */
class AssertTest extends APhysicalPlanTest
{
    @Test
    void test_max_row_count()
    {
        MutableBoolean closed = new MutableBoolean();
        Schema schema = Schema.of(Column.of("col", ResolvedType.of(Type.Int)));
        IPhysicalPlan plan = scan(schemaLessDS(() -> closed.setTrue(), TupleVector.of(schema, asList(ValueVector.literalInt(100, 100)))), table, Schema.EMPTY);

        Assert assertPlan = Assert.maxRowCount(1, plan, 99);
        TupleIterator it = assertPlan.execute(context);

        try
        {
            it.next();
            fail("Plan should fail with too many rows");
        }
        catch (QueryException e)
        {
            assertTrue(e.getMessage()
                    .contains("Query returned too many rows. Expected 99 row(s) to be returned."), e.getMessage());
        }

        try
        {
            it.next();
            fail("Should fail with no such element");
        }
        catch (NoSuchElementException e)
        {
        }
        it.close();

        assertTrue(closed.isTrue());
    }
}
