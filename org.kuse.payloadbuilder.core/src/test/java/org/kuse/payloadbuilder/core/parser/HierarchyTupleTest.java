package org.kuse.payloadbuilder.core.parser;

import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.CompositeTupleTest.TestTuple;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.parser.SubQueryExpression.HierarchyTuple;

/** Test of{@link SubQueryExpression.HierarchyTuple} */
public class HierarchyTupleTest extends AParserTest
{
    @Test
    public void test()
    {
        HierarchyTuple ht = new HierarchyTuple(null);

        try
        {
            ht.getColumn(0);
            fail();
        }
        catch (IllegalArgumentException e)
        {
        }

        try
        {
            ht.getColumnCount();
            fail();
        }
        catch (IllegalArgumentException e)
        {
        }

        try
        {
            ht.getTupleOrdinal();
            fail();
        }
        catch (IllegalArgumentException e)
        {
        }

        assertEquals(-1, ht.getColumnOrdinal("col"));
        assertNull(ht.getValue(0));
        assertNull(ht.getTuple(1));

        TestTuple current = new TestTuple(15)
        {
            @Override
            public Tuple getTuple(int tupleOrdinal)
            {
                if (tupleOrdinal == 17)
                {
                    return new TestTuple(tupleOrdinal);
                }
                return null;
            };
        };
        ht.setCurrent(current);

        assertSame(current, ht.getTuple(-1));
        assertSame(current, ht.getTuple(15));
        assertEquals(17, ht.getTuple(17).getTupleOrdinal());

        assertEquals(15, ht.getColumnOrdinal("col"));
        assertEquals("v0_15", ht.getValue(0));
        assertSame(current, ht.getTuple(15));

        TestTuple parent = new TestTuple(12)
        {
            @Override
            public Tuple getTuple(int tupleOrdinal)
            {
                if (tupleOrdinal == 18)
                {
                    return new TestTuple(tupleOrdinal);
                }
                return super.getTuple(tupleOrdinal);
            };
        };
        ht = new HierarchyTuple(parent);
        ht.setCurrent(current);

        // Test search in current and not found and then search in parent
        assertEquals(18, ht.getTuple(18).getTupleOrdinal());
        assertSame(parent, ht.getTuple(12));

        assertEquals(15, ht.getColumnOrdinal("col"));
        assertEquals("v0_15", ht.getValue(0));
        assertSame(parent, ht.getTuple(12));
    }
}
