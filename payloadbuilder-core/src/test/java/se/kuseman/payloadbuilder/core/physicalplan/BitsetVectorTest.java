package se.kuseman.payloadbuilder.core.physicalplan;

import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEqualsNulls;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.BitSet;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Test of {@link BitSetVector} */
public class BitsetVectorTest extends APhysicalPlanTest
{
    @Test
    public void test()
    {
        BitSetVector v1 = bsv(false, true, false);
        assertEquals(3, v1.size());
        assertEquals(ResolvedType.of(Column.Type.Boolean), v1.type());
        assertVectorsEquals(vv(ResolvedType.of(Column.Type.Boolean), false, true, false), v1);
        assertTrue(v1.getBoolean(1));
        assertFalse(v1.getBoolean(0));
        assertFalse(v1.getPredicateBoolean(0));
        assertTrue(v1.getPredicateBoolean(1));
        // Test null

        v1 = bsv(false, null, true);
        assertVectorsEqualsNulls(vv(ResolvedType.of(Column.Type.Boolean), false, true, false), v1);
        assertFalse(v1.getPredicateBoolean(0));
        assertFalse(v1.getPredicateBoolean(1));
        assertTrue(v1.getPredicateBoolean(2));

        // Verify that not nullable when there are no null bits set
        v1 = new BitSetVector(1, new BitSet(1), new BitSet(1));
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_or_error_when_different_sizes()
    {
        bsv(true, false).or(bsv(true));
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_or_error_when_non_boolean()
    {
        bsv(true, false).or(vv(ResolvedType.of(Column.Type.Int), true, false));
    }

    @Test
    public void test_not()
    {
        BitSetVector v1 = bsv(true, false, null);
        assertVectorsEquals(vv(ResolvedType.of(Column.Type.Boolean), true, false, null), v1);
        v1 = v1.not();
        assertVectorsEquals(vv(ResolvedType.of(Column.Type.Boolean), false, true, null), v1);

        // Non null
        v1 = bsv(true, false);
        assertVectorsEquals(vv(ResolvedType.of(Column.Type.Boolean), true, false), v1);
        v1 = v1.not();
        assertVectorsEquals(vv(ResolvedType.of(Column.Type.Boolean), false, true), v1);
    }

    @Test
    public void test_or()
    {
        BitSetVector actual;
        BitSetVector v1;

        // This not null
        // That not null
        //@formatter:off
        v1 = bsv(                                                    true, false, true,  false);
        actual = v1.or(bsv(                                          true, true,  false, false));
        assertVectorsEquals(vv(ResolvedType.of(Column.Type.Boolean), true, true,  true,  false), actual);
        //@formatter:on

        // This not null
        // That null (but no nulls)
        // actual = v1.or(bsv(true, true, false, false));
        // assertVectorsEquals(vv(Type.Boolean, true, true, true, false), actual);
        // assertFalse(actual.isNullable());

        // This not null
        // That null
        //@formatter:off
        v1 = bsv(                                                         true,  false, true,  false, true,  false);
        actual = v1.or(bsv(                                               true,  true,  false, false, null,  null));
        assertVectorsEquals(vv(ResolvedType.of(Column.Type.Boolean),      true,  true,  true,  false, true,  null), actual);
        assertVectorsEqualsNulls(vv(ResolvedType.of(Column.Type.Boolean), false, false, false, false, false, true), actual);
        //@formatter:on

        // This null
        // That not null
        //@formatter:off
        v1 = bsv(                                                         true,  true,  false, false, null,  null);
        actual = v1.or(bsv(                                               true,  false, true,  false, true,  false));
        assertVectorsEquals(vv(ResolvedType.of(Column.Type.Boolean),      true,  true,  true,  false, true,  null), actual);
        assertVectorsEqualsNulls(vv(ResolvedType.of(Column.Type.Boolean), false, false, false, false, false, true), actual);
        //@formatter:on

        // This null
        // That not null (but no nulls)
        // actual = v1.or(bsv(true, true, true));
        // assertVectorsEquals(vv(Type.Boolean, false, false, null), actual);
        // assertTrue(actual.isNullable());
        // assertVectorsEqualsNulls(vv(Type.Boolean, false, false, true), actual);

        // This null
        // That null
        //@formatter:off
        v1 = bsv(                                                         true,  false, null,  true,  false, null);
        actual = v1.or(bsv(                                               true,  true,  false, false, null,  null));
        assertVectorsEquals(vv(ResolvedType.of(Column.Type.Boolean),      true,  true,  null,  true,  null,  null), actual);
        assertVectorsEqualsNulls(vv(ResolvedType.of(Column.Type.Boolean), false, false, true,  false, true,  true), actual);
        //@formatter:on
    }

    @Test
    public void test_convert_or()
    {
        BitSetVector actual;
        BitSetVector v1;

        // This not null
        // That not null
        //@formatter:off
        v1 = bsv(                                                    true, false, true,  false);
        actual = v1.or(vv(ResolvedType.of(Column.Type.Boolean),      true, true,  false, false));
        assertVectorsEquals(vv(ResolvedType.of(Column.Type.Boolean), true, true,  true,  false), actual);
        //@formatter:on

        // This not null
        // That null (but no nulls)
        // actual = v1.or(bsv(true, true, false, false));
        // assertVectorsEquals(vv(Type.Boolean, true, true, true, false), actual);
        // assertFalse(actual.isNullable());

        // This not null
        // That null
        //@formatter:off
        v1 = bsv(                                                         true,  false, true,  false, true,  false);
        actual = v1.or(vv(ResolvedType.of(Column.Type.Boolean),           true,  true,  false, false, null,  null));
        assertVectorsEquals(vv(ResolvedType.of(Column.Type.Boolean),      true,  true,  true,  false, true,  null), actual);
        assertVectorsEqualsNulls(vv(ResolvedType.of(Column.Type.Boolean), false, false, false, false, false, true), actual);
        //@formatter:on

        // This null
        // That not null
        //@formatter:off
        v1 = bsv(                                                         true,  true,  false, false, null,  null);
        actual = v1.or(vv(ResolvedType.of(Column.Type.Boolean),           true,  false, true,  false, true,  false));
        assertVectorsEquals(vv(ResolvedType.of(Column.Type.Boolean),      true,  true,  true,  false, true,  null), actual);
        assertVectorsEqualsNulls(vv(ResolvedType.of(Column.Type.Boolean), false, false, false, false, false, true), actual);
        //@formatter:on

        // This null
        // That not null (but no nulls)
        // actual = v1.or(bsv(true, true, true));
        // assertVectorsEquals(vv(Type.Boolean, false, false, null), actual);
        // assertTrue(actual.isNullable());
        // assertVectorsEqualsNulls(vv(Type.Boolean, false, false, true), actual);

        // This null
        // That null
        //@formatter:off
        v1 = bsv(                                                         true,  false, null,  true,  false, null);
        actual = v1.or(vv(ResolvedType.of(Column.Type.Boolean),           true,  true,  false, false, null,  null));
        assertVectorsEquals(vv(ResolvedType.of(Column.Type.Boolean),      true,  true,  null,  true,  null,  null), actual);
        assertVectorsEqualsNulls(vv(ResolvedType.of(Column.Type.Boolean), false, false, true,  false, true,  true), actual);
        //@formatter:on
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_and_error_when_different_sizes()
    {
        bsv(true, false).and(bsv(true));
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_and_error_when_non_boolean()
    {
        bsv(true, false).and(vv(ResolvedType.of(Column.Type.Int), true, false));
    }

    @Test
    public void test_and()
    {
        BitSetVector actual;
        BitSetVector v1;

        // This not null
        // That not null
        //@formatter:off
        v1 = bsv(                                                    true, false, true,  false);
        actual = v1.and(bsv(                                         true, true,  false, false));
        assertVectorsEquals(vv(ResolvedType.of(Column.Type.Boolean), true, false, false, false), actual);
        //@formatter:on
        // assertFalse(actual.isNullable());

        // // This not null
        // // That null (but no nulls)
        // v1 = bsv(true, false, true, false);
        // actual = v1.and(bsv(true, true, true));
        // assertVectorsEquals(vv(Type.Boolean, false, false, false), actual);
        // assertFalse(actual.isNullable());

        // This not null
        // That null
        //@formatter:off
        v1 = bsv(                                                         true,  false, true,  false, true, false);
        actual = v1.and(bsv(                                              true,  true,  false, false, null, null));
        assertVectorsEquals(vv(ResolvedType.of(Column.Type.Boolean),      true,  false, false, false, null, false), actual);
        assertVectorsEqualsNulls(vv(ResolvedType.of(Column.Type.Boolean), false, false, false, false, true, false), actual);
        //@formatter:on

        // This null
        // That not null
        //@formatter:off
        v1 = bsv(                                                         true,  false, null, true,  false, null);
        actual = v1.and(bsv(                                              true,  true,  true, false, false, false));
        assertVectorsEquals(vv(ResolvedType.of(Column.Type.Boolean),      true,  false, null, false, false, false), actual);
        assertVectorsEqualsNulls(vv(ResolvedType.of(Column.Type.Boolean), false, false, true, false, false, false), actual);
        //@formatter:on

        // This null
        // That null
        //@formatter:off
        v1 = bsv(                                                         true,  false, null,  true,  false, null);
        actual = v1.and(bsv(                                              true,  true,  false, false, null,  null));
        assertVectorsEquals(vv(ResolvedType.of(Column.Type.Boolean),      true,  false, false, false, false, null), actual);
        assertVectorsEqualsNulls(vv(ResolvedType.of(Column.Type.Boolean), false, false, false, false, false, true), actual);
        //@formatter:on
    }

    @Test
    public void test_convert_invert()
    {
        ValueVector vv = vv(ResolvedType.of(Column.Type.Boolean), true, false, true);
        ValueVector actual = BitSetVector.not(vv);
        assertVectorsEquals(vv(ResolvedType.of(Column.Type.Boolean), false, true, false), actual);
        actual = BitSetVector.not(actual);
        assertVectorsEquals(vv(ResolvedType.of(Column.Type.Boolean), true, false, true), actual);
    }

    @Test
    public void test_convert_and()
    {
        BitSetVector actual;
        BitSetVector v1;

        // This not null
        // That not null
        //@formatter:off
        v1 = bsv(                                                    true, false, true,  false);
        actual = v1.and(vv(ResolvedType.of(Column.Type.Boolean),     true, true,  false, false));
        assertVectorsEquals(vv(ResolvedType.of(Column.Type.Boolean), true, false, false, false), actual);
        //@formatter:on
        // assertFalse(actual.isNullable());

        // // This not null
        // // That null (but no nulls)
        // v1 = bsv(true, false, true, false);
        // actual = v1.and(bsv(true, true, true));
        // assertVectorsEquals(vv(Type.Boolean, false, false, false), actual);
        // assertFalse(actual.isNullable());

        // This not null
        // That null
        //@formatter:off
        v1 = bsv(                                                         true,  false, true,  false, true, false);
        actual = v1.and(vv(ResolvedType.of(Column.Type.Boolean),          true,  true,  false, false, null, null));
        assertVectorsEquals(vv(ResolvedType.of(Column.Type.Boolean),      true,  false, false, false, null, false), actual);
        assertVectorsEqualsNulls(vv(ResolvedType.of(Column.Type.Boolean), false, false, false, false, true, false), actual);
        //@formatter:on

        // This null
        // That not null
        //@formatter:off
        v1 = bsv(                                                         true,  false, null, true,  false, null);
        actual = v1.and(vv(ResolvedType.of(Column.Type.Boolean),          true,  true,  true, false, false, false));
        assertVectorsEquals(vv(ResolvedType.of(Column.Type.Boolean),      true,  false, null, false, false, false), actual);
        assertVectorsEqualsNulls(vv(ResolvedType.of(Column.Type.Boolean), false, false, true, false, false, false), actual);
        //@formatter:on

        // This null
        // That null
        //@formatter:off
        v1 = bsv(                                                         true,  false, null,  true,  false, null);
        actual = v1.and(vv(ResolvedType.of(Column.Type.Boolean),          true,  true,  false, false, null,  null));
        assertVectorsEquals(vv(ResolvedType.of(Column.Type.Boolean),      true,  false, false, false, false, null), actual);
        assertVectorsEqualsNulls(vv(ResolvedType.of(Column.Type.Boolean), false, false, false, false, false, true), actual);
        //@formatter:on
    }

    private BitSetVector bsv(Boolean... bits)
    {
        BitSet bs = new BitSet(bits.length);
        BitSet nulls = null;

        for (int i = 0; i < bits.length; i++)
        {
            if (bits[i] == null)
            {
                if (nulls == null)
                {
                    nulls = new BitSet(bits.length);
                }
                nulls.set(i, true);
            }
            else
            {
                bs.set(i, bits[i]);
            }
        }
        return new BitSetVector(bits.length, bs, nulls);
    }
}
