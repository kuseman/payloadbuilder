package se.kuseman.payloadbuilder.core.planning;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.List;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.Index.ColumnsType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate.ISeekKey;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate.SeekType;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link SeekPredicate} */
public class SeekPredicateTest extends APhysicalPlanTest
{
    @Test
    public void test()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col1", "col2"), ColumnsType.ANY);

        IExpression colValue = ce("value");

        SeekPredicate p = new SeekPredicate(index, asList("col1"), asList(colValue));

        assertEquals(index, p.getIndex());
        assertEquals(asList("col1"), p.getIndexColumns());

        // Verify that nulls and duplicates are removed
        TupleVector tv = TupleVector.of(Schema.of(Column.of("value", Type.Int)), asList(vv(Type.Int, 1, 1, 2, 3, 4, 5, null)));
        context.getStatementContext()
                .setIndexSeekTupleVector(tv);

        List<ISeekKey> keys = p.getSeekKeys(context);

        assertEquals(1, keys.size());
        assertEquals(SeekType.EQ, keys.get(0)
                .getType());
        assertVectorsEquals(vv(Type.Int, 1, 2, 3, 4, 5), keys.get(0)
                .getValue());
        assertEquals("col1 = value", p.toString());
    }
}
