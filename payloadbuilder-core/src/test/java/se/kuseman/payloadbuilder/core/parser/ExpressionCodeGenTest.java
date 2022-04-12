package se.kuseman.payloadbuilder.core.parser;

import static java.util.Arrays.asList;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/** Test of various expression evaluations via code generation */
public class ExpressionCodeGenTest extends AParserTest
{
    @Test
    public void test() throws Exception
    {
        assertPredicate(false, null, "1=10");
        assertPredicate(true, null, "1=1");
        assertPredicate(true, null, "1=1.0");

        Map<String, Object> values = new HashMap<>();
        values.put("a", 10);
        values.put("b", null);
        values.put("v_false", false);
        values.put("v_true", true);
        values.put("v_null", null);

        assertPredicate(true, values, "a = 10");
        assertPredicate(false, values, "a != 10");
        assertPredicate(false, values, "a > 10");
        assertPredicate(true, values, "a >= 10");
        assertPredicate(false, values, "a < 10");
        assertPredicate(true, values, "a <= 10");

        assertPredicate(false, values, "a = null");
        assertPredicate(false, values, "a != null");
        assertPredicate(false, values, "a > null");
        assertPredicate(false, values, "a >= null");
        assertPredicate(false, values, "a < null");
        assertPredicate(false, values, "a <= null");

        assertPredicate(false, values, "a = b");
        assertPredicate(false, values, "a != b");
        assertPredicate(false, values, "a > b");
        assertPredicate(false, values, "a >= b");
        assertPredicate(false, values, "a < b");
        assertPredicate(false, values, "a <= b");

        assertPredicate(false, values, "v_false and true");
        assertPredicate(false, values, "v_false and false");
        assertPredicate(false, values, "v_false and null");
        assertPredicate(false, values, "v_false and v_true");
        assertPredicate(false, values, "v_false and v_false");

        assertPredicate(true, values, "v_true and true");
        assertPredicate(false, values, "v_true and false");
        assertPredicate(false, values, "v_true and null");
        assertPredicate(true, values, "v_true and v_true");
        assertPredicate(false, values, "v_true and v_false");

        // NOTE 3vl
        assertPredicate(true, values, "v_false or true");
        assertPredicate(false, values, "v_false or false");
        assertPredicate(false, values, "v_false or null");
        assertPredicate(true, values, "v_false or v_true");
        assertPredicate(false, values, "v_false or v_false");

        assertPredicate(true, values, "v_true or true");
        assertPredicate(true, values, "v_true or false");
        assertPredicate(true, values, "v_true or null");
        assertPredicate(true, values, "v_true or v_true");
        assertPredicate(true, values, "v_true or v_false");

        assertPredicate(false, values, "not v_true");
        assertPredicate(true, values, "not v_false");
        assertPredicate(false, values, "not v_null");
    }

    @Test
    public void test_functions() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("a", asList(-1, -2, -3, 0, 1, 2, 3));
        values.put("b", null);
        values.put("c", asList(-1, -2, -3, 0, 1, 2, 3).iterator());

        assertPredicate(false, values, "contains(null, 1)");
        assertPredicate(false, values, "contains(a, 10)");
        assertPredicate(true, values, "contains(a, -2)");
        assertPredicate(false, values, "contains(c, 10)");

        values.put("c", asList(-1, -2, -3, 0, 1, 2, 3).iterator());

        assertPredicate(true, values, "contains(c, -2)");
    }
}
