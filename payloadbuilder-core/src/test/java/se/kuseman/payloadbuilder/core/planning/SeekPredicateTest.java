package se.kuseman.payloadbuilder.core.planning;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.List;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.Index.ColumnsType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate.ISeekKey;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link SeekPredicate} */
class SeekPredicateTest extends APhysicalPlanTest
{
    @Test
    void test_fail_with_empty_items()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col1", "col2"), ColumnsType.ANY);
        assertThrows(IllegalArgumentException.class, () -> new SeekPredicate(0, index, emptyList()));
    }

    @Test
    void test()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col1", "col2"), ColumnsType.ANY);

        IExpression colValue = ce("value");

        List<SeekPredicate.SeekPredicateItem> items = List.of(new SeekPredicate.SeekPredicateItem("col1", colValue, List.of(colValue)));

        SeekPredicate p = new SeekPredicate(0, index, items);

        assertEquals(index, p.getIndex());
        assertEquals(asList("col1"), p.getIndexColumns());

        // Verify that nulls and duplicates are removed
        TupleVector tv = TupleVector.of(Schema.of(Column.of("value", Type.Int)), asList(vv(Type.Int, null, 1, 1, 2, 3, 4, 5)));
        context.getStatementContext()
                .setIndexSeekTupleVector(tv);

        List<ISeekKey> keys = p.getSeekKeys(context);

        assertEquals(1, keys.size());
        assertVectorsEquals(vv(Type.Int, 1, 2, 3, 4, 5), keys.get(0)
                .getValue());
        assertEquals("col1 = [value]", p.toString());
    }

    @Test
    void test_push_down_equal()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col1", "col2"), ColumnsType.ANY);

        IExpression colValue = ce("value");

        List<SeekPredicate.SeekPredicateItem> items = List.of(new SeekPredicate.SeekPredicateItem("col1", colValue, List.of(intLit(10))));

        SeekPredicate p = new SeekPredicate(0, index, items, true);

        assertEquals(index, p.getIndex());
        assertEquals(asList("col1"), p.getIndexColumns());

        List<ISeekKey> keys = p.getSeekKeys(context);

        assertEquals(1, keys.size());
        assertVectorsEquals(vv(Type.Int, 10), keys.get(0)
                .getValue());
        assertEquals("col1 = [10]", p.toString());
    }

    @Test
    void test_push_down_in()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col1", "col2"), ColumnsType.ANY);

        IExpression colValue = ce("value");

        List<SeekPredicate.SeekPredicateItem> items = List.of(new SeekPredicate.SeekPredicateItem("col1", colValue, List.of(stringLit("10"), intLit(20), intLit(30))));

        SeekPredicate p = new SeekPredicate(0, index, items, true);

        assertEquals(index, p.getIndex());
        assertEquals(asList("col1"), p.getIndexColumns());

        List<ISeekKey> keys = p.getSeekKeys(context);

        assertEquals(1, keys.size());
        assertVectorsEquals(vv(Type.Int, 10, 20, 30), keys.get(0)
                .getValue());
        assertEquals("col1 = ['10', 20, 30]", p.toString());
    }
}
