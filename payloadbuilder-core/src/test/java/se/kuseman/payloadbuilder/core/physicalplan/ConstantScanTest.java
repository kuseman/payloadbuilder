package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;

/** Test of {@link ConstantScan} */
public class ConstantScanTest extends APhysicalPlanTest
{
    @Test
    public void test()
    {
        IPhysicalPlan plan = new Projection(1, new ConstantScan(0, TupleVector.CONSTANT), asList(new AliasExpression(e("10"), "TEN"), new AliasExpression(e("'hello'"), "HELLO")), null);
        Schema expectedSchema = Schema.of(col("TEN", ResolvedType.of(Type.Int), null), col("HELLO", ResolvedType.of(Type.String), null));

        assertEquals(expectedSchema, plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = it.next();

        assertVectorsEquals(vv(ResolvedType.of(Type.Int), 10), actual.getColumn(0));
        assertVectorsEquals(vv(ResolvedType.of(Type.String), "hello"), actual.getColumn(1));

        assertFalse(it.hasNext());
    }
}
