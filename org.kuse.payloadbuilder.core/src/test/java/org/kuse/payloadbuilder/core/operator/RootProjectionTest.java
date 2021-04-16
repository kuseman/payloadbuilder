package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;

import org.junit.Test;

/** Test of {@link RootProjection} */
public class RootProjectionTest extends AOperatorTest
{
    @Test
    public void test()
    {
        RootProjection p = new RootProjection(asList("col1", "col", ""),
                asList(
                        new ExpressionProjection(e("col1")),
                        new ExpressionProjection(e("col")),
                        new AsteriskProjection(new int[] {0})));

        assertArrayEquals(new String[] {"col1", "col"}, p.getColumns());

        // Asterisk in the middle then no columns is supported
        p = new RootProjection(asList("col1", "", "col3", "col"),
                asList(
                        new ExpressionProjection(e("col1")),
                        new AsteriskProjection(new int[] {0}),
                        new ExpressionProjection(e("col3")),
                        new ExpressionProjection(e("col"))));

        assertArrayEquals(new String[0], p.getColumns());

        p = new RootProjection(asList("col1", "col"),
                asList(
                        new ExpressionProjection(e("col1")),
                        new ExpressionProjection(e("col"))));

        assertArrayEquals(new String[] {"col1", "col"}, p.getColumns());
    }
}
