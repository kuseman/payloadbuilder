package se.kuseman.payloadbuilder.core.operator;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.core.operator.SubQueryExpressionOperator.HierarchyTuple;
import se.kuseman.payloadbuilder.core.parser.AParserTest;

/** Test of{@link SubQueryExpressionOperator.HierarchyTuple} */
public class HierarchyTupleTest extends AParserTest
{
    @Test
    public void test()
    {
        HierarchyTuple ht = new HierarchyTuple(-1, null);

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

        assertEquals(-1, ht.getTupleOrdinal());

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
            }
        };
        ht.setCurrent(current);

        assertEquals(15, ht.getTupleOrdinal());

        assertSame(current, ht.getTuple(-1));
        assertSame(current, ht.getTuple(15));
        assertEquals(17, ht.getTuple(17)
                .getTupleOrdinal());

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
            }
        };
        ht = new HierarchyTuple(-1, parent);
        ht.setCurrent(current);

        assertEquals(17, ht.getTuple(17)
                .getTupleOrdinal());
        // Test search in current and not found and then search in parent
        assertEquals(18, ht.getTuple(18)
                .getTupleOrdinal());
        assertSame(parent, ht.getTuple(12));

        assertEquals(15, ht.getColumnOrdinal("col"));
        assertEquals("v0_15", ht.getValue(0));
        assertSame(parent, ht.getTuple(12));

    }

    /**
     * Regression found when having a subquery expression accessing ordinal with a left joined table having no rows yielding results from wrong tuple since we traversed up to parent and back down.
     **/
    @Test
    public void test_limit_max_child_ordinal()
    {

        /*@formatter:off
         * product (0)
         * subquery (1)
         *   article (2)
         *   articlesustainability (3)
         * productsustainability (4)
         * @formatter:on
         */

        TestTuple parent = new TestTuple(0)
        {
            @Override
            public Tuple getTuple(final int tupleOrdinal)
            {
                return new TestTuple(tupleOrdinal)
                {
                    @Override
                    public Object getValue(int columnOrdinal)
                    {
                        return "parent_" + tupleOrdinal;
                    }
                };
            }
        };

        MutableBoolean returnChild = new MutableBoolean(true);
        TestTuple current = new TestTuple(1)
        {
            @Override
            public Object getValue(int columnOrdinal)
            {
                return "current_" + getTupleOrdinal();
            }

            @Override
            public Tuple getTuple(final int tupleOrdinal)
            {
                // Tuple returned from current
                // 2 is an inner joined table source
                // 3 is a left joined table source
                if (tupleOrdinal == 2
                        || (returnChild.getValue()
                                && tupleOrdinal <= 3))
                {
                    return new TestTuple(tupleOrdinal)
                    {
                        @Override
                        public Object getValue(int columnOrdinal)
                        {
                            return "child_" + tupleOrdinal;
                        }
                    };
                }
                return null;
            }
        };

        HierarchyTuple ht = new HierarchyTuple(-1, parent);
        ht.setCurrent(current);

        // Test no joined rows in left joined table
        returnChild.setFalse();
        assertEquals("parent_0", ht.getTuple(0)
                .getValue(0));
        assertEquals("current_1", ht.getTuple(1)
                .getValue(0));
        assertEquals("child_2", ht.getTuple(2)
                .getValue(0));
        // Delegated to parent
        assertEquals("parent_3", ht.getTuple(3)
                .getValue(0));
        // Delegated to parent
        assertEquals("parent_4", ht.getTuple(4)
                .getValue(0));

        // Test joined rows in left joined table
        returnChild.setTrue();
        assertEquals("parent_0", ht.getTuple(0)
                .getValue(0));
        assertEquals("current_1", ht.getTuple(1)
                .getValue(0));
        assertEquals("child_2", ht.getTuple(2)
                .getValue(0));
        assertEquals("child_3", ht.getTuple(3)
                .getValue(0));
        // Delegated to parent
        assertEquals("parent_4", ht.getTuple(4)
                .getValue(0));

        // Test limiting parent traversal with max ordinal
        ht = new HierarchyTuple(3, parent);
        ht.setCurrent(current);

        // Test no joined rows in left joined table
        returnChild.setFalse();
        assertEquals("parent_0", ht.getTuple(0)
                .getValue(0));
        assertEquals("current_1", ht.getTuple(1)
                .getValue(0));
        assertEquals("child_2", ht.getTuple(2)
                .getValue(0));
        // Null since we know that this is contained within currents children
        // and we don't need to delegate to parent
        assertNull(ht.getTuple(3));
        // Delegated to parent
        assertEquals("parent_4", ht.getTuple(4)
                .getValue(0));

        // Test joined rows in left joined table
        returnChild.setTrue();
        assertEquals("parent_0", ht.getTuple(0)
                .getValue(0));
        assertEquals("current_1", ht.getTuple(1)
                .getValue(0));
        assertEquals("child_2", ht.getTuple(2)
                .getValue(0));
        assertEquals("child_3", ht.getTuple(3)
                .getValue(0));
        // Delegated to parent
        assertEquals("parent_4", ht.getTuple(4)
                .getValue(0));
    }
}
