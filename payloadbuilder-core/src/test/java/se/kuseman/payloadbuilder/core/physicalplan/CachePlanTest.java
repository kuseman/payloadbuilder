package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link CachePlan} */
public class CachePlanTest extends APhysicalPlanTest
{
    @Test
    public void test_empty()
    {
        AtomicInteger executeCount = new AtomicInteger();
        AtomicInteger closeCount = new AtomicInteger();
        IPhysicalPlan cache = new CachePlan(1, scanVectors(schemaLessDS(() -> closeCount.incrementAndGet(), new TupleVector[0]), Schema.EMPTY, () -> executeCount.incrementAndGet()));
        TupleVector actual = PlanUtils.concat(context, cache.execute(context));

        VectorTestUtils.assertTupleVectorsEquals(TupleVector.EMPTY, actual);
        assertEquals(1, executeCount.get());
        assertEquals(1, closeCount.get());

        actual = PlanUtils.concat(context, cache.execute(context));
        VectorTestUtils.assertTupleVectorsEquals(TupleVector.EMPTY, actual);
        // Still 1
        assertEquals(1, executeCount.get());
        assertEquals(1, closeCount.get());
    }

    @Test
    public void test()
    {
        AtomicInteger executeCount = new AtomicInteger();
        AtomicInteger closeCount = new AtomicInteger();

        Schema schema = schema(new Type[] { Type.Int, Type.Any }, "col1", "col2");

        IPhysicalPlan cache = new CachePlan(1, scanVectors(schemaLessDS(() -> closeCount.incrementAndGet(), TupleVector.of(schema, asList(vv(Type.Int, null, 20, 10, 30), vv(Type.Any, -1, 0, 1, 2))),
                TupleVector.of(schema, asList(vv(Type.Int, 20, null), vv(Type.Any, 3, 4)))), Schema.EMPTY, () -> executeCount.incrementAndGet()));

        TupleVector actual = PlanUtils.concat(context, cache.execute(context));

        VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(schema, List.of(vv(Type.Int, null, 20, 10, 30, 20, null), vv(Type.Any, -1, 0, 1, 2, 3, 4))), actual);
        assertEquals(1, executeCount.get());
        assertEquals(1, closeCount.get());

        actual = PlanUtils.concat(context, cache.execute(context));
        VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(schema, List.of(vv(Type.Int, null, 20, 10, 30, 20, null), vv(Type.Any, -1, 0, 1, 2, 3, 4))), actual);
        // Still 1
        assertEquals(1, executeCount.get());
        assertEquals(1, closeCount.get());
    }
}
