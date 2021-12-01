package org.kuse.payloadbuilder.core.operator;

import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.CompositeTupleTest.TestTuple;

/** Test of {@link JoinTuple} */
public class JoinTupleTest extends AOperatorTest
{
    @Test
    public void test()
    {
        JoinTuple jt = new JoinTuple(null);

        try
        {
            jt.getTupleOrdinal();
            fail();
        }
        catch (IllegalArgumentException e)
        {
        }

        assertEquals(-1, jt.getColumnOrdinal("col"));
        assertEquals(null, jt.getValue(0));

        TestTuple ct = new TestTuple(0);
        jt = new JoinTuple(ct);

        assertNull(jt.getTuple(-1));

        assertEquals(-1, jt.getColumnOrdinal("col"));
        assertEquals(null, jt.getValue(0));

        TestTuple inner = new TestTuple(666);
        jt.setInner(inner);

        assertSame(null, jt.getTuple(-1));
        assertSame(inner, jt.getTuple(666));
        assertSame(ct, jt.getTuple(0));

        assertEquals(666, jt.getColumnOrdinal("col"));
        assertEquals("v0_666", jt.getValue(0));

        TestTuple outer = new TestTuple(333);
        jt.setOuter(outer);

        assertSame(outer, jt.getTuple(333));
        assertSame(inner, jt.getTuple(666));

        assertEquals(666, jt.getColumnOrdinal("col"));
        assertEquals("v0_666", jt.getValue(0));

        Tuple t = jt.getTuple(333);

        assertEquals(333, t.getColumnOrdinal("col"));
        assertEquals("v0_333", t.getValue(0));
    }
}
