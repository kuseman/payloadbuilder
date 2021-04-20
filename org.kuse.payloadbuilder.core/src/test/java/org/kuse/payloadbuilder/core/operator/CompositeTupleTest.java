package org.kuse.payloadbuilder.core.operator;

import java.util.Arrays;
import java.util.Objects;

import org.junit.Test;

/** Test of {@link CompositeTuple} */
public class CompositeTupleTest extends AOperatorTest
{
    @Test
    public void test_delgation()
    {
        TestTuple t3 = new TestTuple(3);
        CollectionTuple colt1 = new CollectionTuple(t3, 3);
        TestTuple t4 = new TestTuple(4);
        CompositeTuple ct = new CompositeTuple(colt1, t4, 10);
        assertSame(colt1, ct.getTuple(-1));
        assertNull(ct.getTuple(2));
        assertSame(colt1, ct.getTuple(3));
        assertSame(t4, ct.getTuple(4));
    }

    @Test
    public void test()
    {
        TestTuple t0 = new TestTuple(0);
        TestTuple t1 = new TestTuple(1);

        CompositeTuple ct = new CompositeTuple(t0, t1, 10);
        TestTuple t2 = new TestTuple(2);
        TestTuple t3 = new TestTuple(3);

        CompositeTuple ct1 = new CompositeTuple(t2, t3, 3);
        ct.add(ct1);

        TestTuple t4 = new TestTuple(4);
        CollectionTuple colt1 = new CollectionTuple(t4, 4);
        ct.add(colt1);

        TestTuple t5 = new TestTuple(5);
        TestTuple t6 = new TestTuple(6);
        CollectionTuple colt2 = new CollectionTuple(t5, 5);
        colt2.addTuple(t6);
        ct.add(colt2);

        assertEquals(-1, ct.getTupleOrdinal());

        assertSame(t0, ct.getCollectionTupleForMerge(0));
        assertSame(t1, ct.getCollectionTupleForMerge(1));
        assertSame(t2, ct.getCollectionTupleForMerge(2));
        assertSame(t3, ct.getCollectionTupleForMerge(3));
        assertNull(ct.getCollectionTupleForMerge(10));

        assertSame(t0, ct.getTuple(-1));
        assertSame(t0, ct.getTuple(0));
        assertSame(t1, ct.getTuple(1));
        assertSame(t2, ct.getTuple(2));
        assertSame(t3, ct.getTuple(3));
        assertNull(ct.getTuple(10));

        assertEquals(0, ct.getColumnCount());

        assertEquals(0, ct.getColumnOrdinal("col"));

        assertEquals("c0_0", ct.getColumn(0));

        assertEquals("v0_0", ct.getValue(0));

        assertFalse(t0.optimized);
        assertFalse(t1.optimized);
        assertFalse(t2.optimized);
        assertFalse(t3.optimized);
        assertFalse(t4.optimized);
        assertTrue(Arrays.stream(ct.tuples).anyMatch(Objects::isNull));
        assertSame(colt1, ct.getTuple(4));
        assertSame(colt2, ct.getTuple(5));
        CompositeTuple opCt = (CompositeTuple) ct.optimize(new ExecutionContext(session));
        assertFalse(Arrays.stream(opCt.tuples).anyMatch(Objects::isNull));
        assertTrue(t0.optimized);
        assertTrue(t1.optimized);
        assertTrue(t2.optimized);
        assertTrue(t3.optimized);
        assertTrue(t4.optimized);
        // Verify that collection tuple is discarded since it was a single collection
        assertSame(t4, ct.getTuple(4));
        // Verify that collection tuple is NOT discarded since it was NOT a single collection
        assertSame(colt2, ct.getTuple(5));
    }

    /** Test tuple */
    public static class TestTuple implements Tuple
    {
        private final int tupleOrdinal;
        boolean optimized;

        public TestTuple(int tupleOrdinal)
        {
            this.tupleOrdinal = tupleOrdinal;
        }

        @Override
        public int getTupleOrdinal()
        {
            return tupleOrdinal;
        }

        @Override
        public int getColumnCount()
        {
            return this.tupleOrdinal;
        }

        @Override
        public int getColumnOrdinal(String column)
        {
            return this.tupleOrdinal;
        }

        @Override
        public String getColumn(int columnOrdinal)
        {
            return "c" + columnOrdinal + "_" + this.tupleOrdinal;
        }

        @Override
        public Object getValue(int columnOrdinal)
        {
            return "v" + columnOrdinal + "_" + this.tupleOrdinal;
        }

        @Override
        public Tuple optimize(ExecutionContext context)
        {
            optimized = true;
            return this;
        }

        @Override
        public String toString()
        {
            return "Ord: " + tupleOrdinal;
        }
    }
}
