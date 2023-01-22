package se.kuseman.payloadbuilder.api.catalog;

import org.junit.Assert;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.FunctionInfo.Arity;

/** Test of {@link FunctionInfo} */
public class FunctionInfoTest extends Assert
{
    @Test
    public void test_arity()
    {
        assertTrue(FunctionInfo.Arity.NO_LIMIT.satisfies(0));
        assertTrue(FunctionInfo.Arity.NO_LIMIT.satisfies(1));

        assertTrue(FunctionInfo.Arity.ZERO.satisfies(0));
        assertFalse(FunctionInfo.Arity.ZERO.satisfies(1));

        assertFalse(FunctionInfo.Arity.ONE.satisfies(0));
        assertTrue(FunctionInfo.Arity.ONE.satisfies(1));
        assertFalse(FunctionInfo.Arity.ONE.satisfies(2));

        assertFalse(FunctionInfo.Arity.AT_LEAST_ONE.satisfies(0));
        assertTrue(FunctionInfo.Arity.AT_LEAST_ONE.satisfies(1));
        assertTrue(FunctionInfo.Arity.AT_LEAST_ONE.satisfies(2));
        assertTrue(FunctionInfo.Arity.AT_LEAST_ONE.satisfies(3));
        assertTrue(FunctionInfo.Arity.AT_LEAST_ONE.satisfies(4));

        assertFalse(FunctionInfo.Arity.TWO.satisfies(0));
        assertFalse(FunctionInfo.Arity.TWO.satisfies(1));
        assertTrue(FunctionInfo.Arity.TWO.satisfies(2));
        assertFalse(FunctionInfo.Arity.TWO.satisfies(3));

        assertFalse(FunctionInfo.Arity.AT_LEAST_TWO.satisfies(0));
        assertFalse(FunctionInfo.Arity.AT_LEAST_TWO.satisfies(1));
        assertTrue(FunctionInfo.Arity.AT_LEAST_TWO.satisfies(2));
        assertTrue(FunctionInfo.Arity.AT_LEAST_TWO.satisfies(3));
        assertTrue(FunctionInfo.Arity.AT_LEAST_TWO.satisfies(4));

        Arity arity = new Arity(1, 2);

        assertFalse(arity.satisfies(0));
        assertTrue(arity.satisfies(1));
        assertTrue(arity.satisfies(2));
        assertFalse(arity.satisfies(3));
    }
}
