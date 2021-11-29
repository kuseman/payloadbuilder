package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;

import org.apache.commons.collections4.IteratorUtils;
import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.CompositeTupleTest.TestTuple;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.operator.TableAlias.Type;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Test of {@link CollectionTuple} */
public class CollectionTupleTest extends AOperatorTest
{
    private final TableAlias alias = TableAliasBuilder.of(0, Type.TABLE, QualifiedName.of("table"), "a").build();

    @Test(expected = IllegalArgumentException.class)
    public void test_bad_input_RowCollectionTuple()
    {
        new RowCollectionTuple(alias, new String[] {"col"}, 1, null, null);
    }

    @Test
    public void test_optimize()
    {
        TestTuple t0 = new TestTuple(1);
        CollectionTuple ct = new CollectionTuple(t0, 1);
        assertFalse(t0.optimized);
        ct.optimize(context);
        assertTrue(t0.optimized);
    }

    @Test
    public void test_no_pull_up_with_different_column_sets()
    {
        Row r1 = Row.of(alias, 0, new String[] {"col1", "col2", "col3"}, new Object[] {1, 2, 3});
        Row r2 = Row.of(alias, 1, new String[] {"col1", "col2", "col3"}, new Object[] {4, 5, 6});
        CollectionTuple ct = new CollectionTuple(r1, 0);
        ct.addTuple(r2);

        assertSame(ct, ct.optimize(context));
    }

    @Test
    public void test_pullUp_of_single_row()
    {
        String[] columns = new String[] {"col1", "col2", "col3"};
        Row r1 = Row.of(alias, 0, columns, new Object[] {1, 2, 3});
        CollectionTuple ct = new CollectionTuple(r1, 0);
        RowCollectionTuple rct = (RowCollectionTuple) ct.optimize(context);
        assertEquals(0, rct.getTupleOrdinal());
        assertSame(rct, rct.getTuple(0));
        assertNull(rct.getTuple(1));
        assertEquals(asList(r1), IteratorUtils.toList(rct.iterator()));
        assertEquals(3, rct.getColumnCount());

        assertEquals(0, rct.getColumnOrdinal("col1"));
        assertEquals(1, rct.getColumnOrdinal("col2"));
        assertEquals(2, rct.getColumnOrdinal("col3"));

        assertEquals("col1", rct.getColumn(0));
        assertEquals("col2", rct.getColumn(1));
        assertEquals("col3", rct.getColumn(2));

        assertNull(rct.getValue(-10));
        assertEquals(1, rct.getValue(0));
        assertEquals(2, rct.getValue(1));
        assertEquals(3, rct.getValue(2));
    }

    @Test
    public void test_pullUp_of_multiple_row()
    {
        String[] columns = new String[] {"col1", "col2", "col3"};
        Row r1 = Row.of(alias, 0, columns, new Object[] {1, 2, 3});
        Row r2 = Row.of(alias, 1, columns, new Object[] {4, 2, 6});
        CollectionTuple ct = new CollectionTuple(r1, 0);
        ct.addTuple(r2);

        RowCollectionTuple rct = (RowCollectionTuple) ct.optimize(context);
        assertEquals(0, rct.getTupleOrdinal());
        assertSame(rct, rct.getTuple(0));
        assertNull(rct.getTuple(1));
        assertEquals(asList(r1, r2), IteratorUtils.toList(rct.iterator()));
        assertEquals(3, rct.getColumnCount());

        assertEquals(0, rct.getColumnOrdinal("col1"));
        assertEquals(1, rct.getColumnOrdinal("col2"));
        assertEquals(2, rct.getColumnOrdinal("col3"));

        assertEquals("col1", rct.getColumn(0));
        assertEquals("col2", rct.getColumn(1));
        assertEquals("col3", rct.getColumn(2));

        assertNull(rct.getValue(-10));
        assertEquals(1, rct.getValue(0));
        assertEquals(2, rct.getValue(1));
        assertEquals(3, rct.getValue(2));
    }

    @Test
    public void test()
    {
        TestTuple t0 = new TestTuple(1);
        TestTuple t1 = new TestTuple(1);
        TestTuple t2 = new TestTuple(1);

        CollectionTuple ct = new CollectionTuple(t0, 1);

        assertEquals(1, ct.getTupleOrdinal());

        assertSame(t0, ct.getSingleTuple());
        assertEquals(asList(t0), IteratorUtils.toList(ct.iterator()));
        assertNull(ct.getTuple(2));
        // Delegate to first tuple
        assertSame(t0, ct.getTuple(1));
        assertEquals(1, ct.getColumnCount());
        assertEquals(1, ct.getColumnOrdinal("col"));
        assertEquals("c0_1", ct.getColumn(0));
        assertEquals("v0_1", ct.getValue(0));

        // Test multi tuples
        ct.addTuple(t1);
        ct.addTuple(t2);

        assertNull(ct.getSingleTuple());
        assertEquals(asList(t0, t1, t2), IteratorUtils.toList(ct.iterator()));

        assertNull(ct.getTuple(2));
        // Delegate to first tuple
        assertSame(t0, ct.getTuple(1));
        assertEquals(1, ct.getColumnCount());
        assertEquals(1, ct.getColumnOrdinal("col"));
        assertEquals("c0_1", ct.getColumn(0));
        assertEquals("v0_1", ct.getValue(0));

        assertFalse(t0.optimized);
        assertFalse(t1.optimized);
        assertFalse(t2.optimized);
        ct.optimize(context);
        assertTrue(t0.optimized);
        assertTrue(t1.optimized);
        assertTrue(t2.optimized);
    }
}
