package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;

import java.util.Iterator;

import org.apache.commons.collections4.IteratorUtils;
import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.CompositeTupleTest.TestTuple;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

/** Test of {@link GroupedRow} */
public class GroupedRowTest extends AOperatorTest
{
    @Test
    public void test()
    {
        try
        {
            new GroupedRow(asList(), new Int2ObjectOpenHashMap<>());
            fail();
        }
        catch (IllegalArgumentException e)
        {
        }

        TestTuple t1 = new TestTuple(1);
        TestTuple t1_1 = new TestTuple(1);

        Int2ObjectMap<IntSet> columnOrdinals = new Int2ObjectOpenHashMap<>();
        columnOrdinals.put(1, new IntOpenHashSet(asList(0)));

        GroupedRow gr = new GroupedRow(asList(t1, t1_1), columnOrdinals);

        assertRow(gr);
        GroupedRow grO = (GroupedRow) gr.optimize(context);
        assertNotSame(gr, grO);
        assertRow(grO);
    }

    @SuppressWarnings("unchecked")
    private void assertRow(GroupedRow gr)
    {
        assertEquals(1, gr.getTupleOrdinal());
        assertEquals(1, gr.getColumnCount());
        assertEquals("c0_1", gr.getColumn(0));
        assertEquals(1, gr.getColumnOrdinal("col"));

        // Columns
        assertEquals(asList("v1_1", "v1_1"), IteratorUtils.toList((Iterator<Object>) gr.getTuple(1).getValue(1)));
        assertEquals("v0_1", gr.getTuple(1).getValue(0));
        assertEquals("v0_1", gr.getValue(0));

        assertEquals(asList(null, null), gr.getTuple(0));
    }
}
