package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Test of {@link Limit} */
class LimitTest extends APhysicalPlanTest
{
    @Test
    void test()
    {
        MutableBoolean closed = new MutableBoolean();
        Schema schema = Schema.of(Column.of("col", Type.Int));
        IPhysicalPlan plan = scan(
                schemaLessDS(() -> closed.setTrue(), TupleVector.of(schema, asList(ValueVector.literalInt(100, 100))), TupleVector.of(schema, asList(ValueVector.literalInt(100, 100)))), table,
                Schema.EMPTY, 1);

        Limit top = new Limit(1, plan, e("150"));

        int iterations = 0;
        int total = 0;
        TupleIterator it = top.execute(context);
        while (it.hasNext())
        {
            TupleVector vector = it.next();
            int rowCount = vector.getRowCount();
            assertEquals(rowCount, vector.getColumn(0)
                    .size());

            Assertions.assertThat(vector.getSchema())
                    .usingRecursiveComparison()
                    .isEqualTo(Schema.of(col("col", ResolvedType.of(Type.Int), table)));
            assertEquals(Schema.of(col("col", ResolvedType.of(Type.Int), table)), vector.getSchema());
            assertEquals(ResolvedType.of(Type.Int), vector.getColumn(0)
                    .type());

            total += rowCount;
            iterations++;
        }
        assertFalse(it.hasNext());
        it.close();

        assertEquals(150, total);
        assertEquals(2, iterations);
        assertTrue(closed.isTrue());
    }
}
