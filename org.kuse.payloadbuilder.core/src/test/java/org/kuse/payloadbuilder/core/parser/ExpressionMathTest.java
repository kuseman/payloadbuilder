package org.kuse.payloadbuilder.core.parser;

import static java.util.Arrays.asList;

import org.junit.Assert;
import org.junit.Test;

/** Unit test of {@link ExpressionMath} */
public class ExpressionMathTest extends Assert
{
    @Test
    public void test_inValue()
    {
        assertTrue(ExpressionMath.inValue(1, 1));
        assertFalse(ExpressionMath.inValue(1, 10));
        assertTrue(ExpressionMath.inValue(1, asList(1, 2)));
        assertTrue(ExpressionMath.inValue(asList(1, 2), 1));
        assertTrue(ExpressionMath.inValue(1, asList(1, 2).iterator()));
        assertFalse(ExpressionMath.inValue(1, asList(19, 2).iterator()));
        assertTrue(ExpressionMath.inValue(asList(1, 2).iterator(), 1));
        assertFalse(ExpressionMath.inValue(asList(19, 2).iterator(), 1));
    }
}
