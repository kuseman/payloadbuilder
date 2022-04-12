package se.kuseman.payloadbuilder.core.parser;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToIntBiFunction;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.codegen.CodeGeneratorContext;
import se.kuseman.payloadbuilder.api.codegen.ExpressionCode;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.core.codegen.CodeGenerator;
import se.kuseman.payloadbuilder.core.operator.ExecutionContext;
import se.kuseman.payloadbuilder.core.operator.ExpressionHashFunction;

/** Test of various expression evaluations */
public class ExpressionTest extends AParserTest
{
    @Test
    public void test_codeGen_hashFunction()
    {
        List<Expression> list = asList(e("10"), e("10l"), e("10f"), e("10d"), e("null"), e("true"), e("'test'"));
        CodeGenerator g = new CodeGenerator();
        ToIntBiFunction<ExecutionContext, Tuple> function = g.generateHashFunction(list);
        assertEquals(1166284934, function.applyAsInt(context, null));

        function = new ExpressionHashFunction(list);
        assertEquals(1166284934, function.applyAsInt(context, null));
    }

    @Test
    public void test_auto_cast_strings() throws Exception
    {
        assertExpressionFail(IllegalArgumentException.class, "Cannot compare true (Boolean) and", null, "true = '1.0'");
        assertExpressionFail(IllegalArgumentException.class, "Cannot convert value 1.0 to", null, "1 = '1.0'");

        assertExpression(true, null, "1 = '1'");
        assertExpression(true, null, "'1' = 1");
        assertExpression(true, null, "1.12 = '1.12'");
        assertExpression(true, null, "'1.12' = 1.12");

        assertExpression(true, null, "'1' > 0");
        assertExpression(true, null, "1 > '0'");
        assertExpression(true, null, "'1' >= 0");
        assertExpression(true, null, "1 >= '0'");

        assertExpression(false, null, "'1' < 0");
        assertExpression(false, null, "1 < '0'");
        assertExpression(false, null, "'1' <= 0");
        assertExpression(false, null, "1 <= '0'");

        assertExpression(false, null, "1L <= '0'");
        assertExpression(false, null, "1D <= '0'");
    }

    @Test
    public void test_auto_cast_booleans() throws Exception
    {
        assertExpression(true, null, "1 = true");
        assertExpression(true, null, "2 > true");
        assertExpression(true, null, "0 = false");
    }

    @Test
    public void test_1() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("a", true);
        values.put("b", false);
        values.put("c", false);
        values.put("d", false);
        values.put("e", false);
        values.put("f", null);

        assertExpression(true, null, "1 + 1 > 10 - 9");

        assertExpression(null, null, "null and true");
        assertExpression(null, null, "true and null");
        assertExpression(true, null, "null or true");
        assertExpression(true, null, "true or null");
        assertExpression(null, null, "null or false");
        assertExpression(null, null, "false or null");
        assertExpression(false, null, "false and true");
        assertExpression(true, null, "true and true");
        assertExpression(false, null, "false and false");

        assertExpression(false, null, "true and false and true");

        assertExpression(null, null, "true and null and true");
        assertExpression(null, null, "not (true and null and true)");
        assertExpression(null, null, "not (null)");
        assertExpression(false, null, "true and null and true and 1 > 1 and 1 != 2 and 1 = null");
        assertExpression(true, values, "a and not (d or e)");
        assertExpression(false, null, "true and not true");
        assertExpression(false, null, "(1 = 0 or 3 in(1,2) or 2 in(3,2)) and not (4 in(4) or 5 in(5))");
        assertExpression(null, null, "null in (1,2,3)");
        assertExpression(false, null, "1 in (null,2,3)");

        assertExpression(true, null, "null is null");
        assertExpression(false, null, "null is not null");
        assertExpression(true, values, "a is not null");
        assertExpression(true, values, "f is null");
        assertExpression(true, values, "f is null and a");
        assertExpression(994, null, "hash(1,2)");
        assertExpression(null, null, "hash(1,null)");

        assertExpression(-2, null, "-2");
        long v = Integer.MIN_VALUE;
        v--;
        assertExpression(v, null, Long.toString(v));
    }

    @Test
    public void test_catalog() throws Exception
    {
        Catalog utils = new Catalog("UTILS")
        {
        };
        utils.registerFunction(new ScalarFunctionInfo(utils, "uuid")
        {
            @Override
            public ExpressionCode generateCode(CodeGeneratorContext context, List<? extends IExpression> arguments)
            {
                ExpressionCode code = context.getExpressionCode();
                code.setCode(String.format("boolean %s = false;\n" + "String %s = java.util.UUID.randomUUID().toString();\n", code.getNullVar(), code.getResVar()));
                return code;
            }

            @Override
            public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
            {
                return java.util.UUID.randomUUID()
                        .toString();
            }
        });
        session.getCatalogRegistry()
                .registerCatalog("UTILS", utils);

        assertFunction(true, emptyMap(), "UTILS#uuid() is not null");
    }

    @Test
    public void test_and() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("a", true);
        values.put("b", false);
        values.put("c", false);
        values.put("d", false);
        values.put("e", false);

        assertExpression(true, values, "true and true");
        assertExpression(false, values, "true and false");
        assertExpression(null, values, "true and null");
        assertExpression(false, values, "false and true");
        assertExpression(false, values, "false and false");
        assertExpression(false, values, "false and null");
    }

    @Test
    public void test_literal() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("a", true);
        values.put("b", false);
        values.put("c", false);
        values.put("d", false);
        values.put("e", false);
        values.put("f", 1);
        values.put("g", 1L);
        values.put("h", 1F);
        values.put("i", 1D);

        assertExpression(1, values, "1");
        assertExpression(Long.MAX_VALUE, values, Long.toString(Long.MAX_VALUE));
        assertExpression(1.1f, values, "1.1");
        assertExpression(10, values, "10");
        assertExpression("hello world", values, "'hello world'");
        assertExpression(null, values, "null");
        assertExpression(false, values, "false");
        assertExpression(true, values, "true");

        assertExpression(-2, values, "-(f+1)");
        assertExpression(-2L, values, "-(f+1l)");
        assertExpression(-2f, values, "-(f+1f)");
        assertExpression(-2d, values, "-(f+1d)");

        assertExpression(-1, values, "-f");
        assertExpression(-1L, values, "-g");
        assertExpression(-1f, values, "-h");
        assertExpression(-1d, values, "-i");

        assertExpressionFail(IllegalArgumentException.class, "Cannot negate false", values, "-b");
    }

    @Test
    public void test_in_expression() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("a", 1);
        values.put("b", null);
        values.put("c", asList(1, 2));

        assertExpression(false, values, "(1+2+3+4) > 123 and (1.0 <20)");
        assertExpression(null, values, "null in(1,2,3,'str')");
        assertExpression(true, values, "a in(1,null,3,'str')");
        assertExpression(true, values, "'str' in(1,2,3,'str')");
        assertExpression(true, values, "1 in(c, 1,2,3,'str', c)");
        assertExpression(false, values, "10.1 in(c, 1,2,3,'str', c)");
    }

    @Test
    public void test_functions() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("a", 1);
        values.put("b", null);

        assertFunction(10, values, "isnull(b, 10)");
        assertFunction(10, values, "isnull(10, var)");
        assertExpression(6, values, "coalesce(null, null, 1+2+3)");
        assertExpressionFail(ParseException.class, "Function isnull expects 2 arguments but got 3", null, "isnull(10, var, 1)");
    }

    @Test
    public void test_qualifiednames() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("a", 1);
        values.put("b", null);
        values.put("c", true);
        values.put("d", 1.1);
        values.put("e", 1.0);
        values.put("f", "string");
        values.put("g", "string2");

        assertExpression(false, values, "a > 10");
        assertExpression(null, values, "b > 10");
        assertExpression(false, values, "a > 10.0");
        assertExpression(false, values, "a > d");
        assertExpression(false, values, "a >= d");
        assertExpression(true, values, "a < d");
        assertExpression(true, values, "a <= d");
        assertExpression(false, values, "a = d");
        assertExpression(true, values, "a != d");
        assertExpression(true, values, "a = e");
        assertExpression(false, values, "a != e");
        assertExpression(true, values, "f != g");
        assertExpression(false, values, "f = g");
        assertExpression(false, values, "f > g");
        assertExpression(true, values, "f < g");
    }

    @Test
    public void test_booleanExpression_predicate() throws Exception
    {
        Map<String, Object> values = new HashMap<>();
        values.put("a", true);
        values.put("b", false);
        values.put("c", null);
        values.put("d", 10);

        String[] expression = new String[] { "true", "false", "null", "a", "b", "c" };
        String[] operators = new String[] { "and", "or" };

        Boolean[] results = new Boolean[] {
                // true true
                true, true,
                // true false
                false, true,
                // true null
                false, true,
                // true a
                true, true,
                // true b
                false, true,
                // true c
                false, true,

                // false true
                false, true,
                // false false
                false, false,
                // false null
                false, false,
                // false a
                false, true,
                // false b
                false, false,
                // false c
                false, false,

                // null true
                false, true,
                // null false
                false, false,
                // null null
                false, false,
                // null a
                false, true,
                // null b
                false, false,
                // null c
                false, false,

                // a true
                true, true,
                // a false
                false, true,
                // a null
                false, true,
                // a a
                true, true,
                // a b
                false, true,
                // a c
                false, true,

                // b true
                false, true,
                // b false
                false, false,
                // b null
                false, false,
                // b a
                false, true,
                // b b
                false, false,
                // b c
                false, false,

                // c true
                false, true,
                // c false
                false, false,
                // c null
                false, false,
                // c a
                false, true,
                // c b
                false, false,
                // c c
                false, false, };

        int index = 0;
        for (String l : expression)
        {
            for (String r : expression)
            {
                for (String o : operators)
                {
                    assertPredicate(results[index++], values, l + " " + o + " " + r);
                }
            }
        }

        assertExpression(false, values, "not (1 > 0)");
        // Test different types
        assertExpressionFail(ClassCastException.class, "java.lang.Integer cannot be cast to ", values, "a and d");
        assertExpressionFail(ClassCastException.class, "java.lang.Integer cannot be cast to ", values, "b or d");
    }

    @Test
    public void test_arithmetic() throws Exception
    {
        // CSOFF
        String[] expression = new String[] { "1", "1.0", "null", "a", "b", "c" };
        // CSON
        String[] operators = new String[] { "+", "-", "*", "/", "%" };

        Object[] results = new Object[] {
                // 1 1
                2, 0, 1, 1, 0,
                // 1 1.0
                2.0f, 0.0f, 1.0f, 1.0f, 0.0f,
                // 1 null
                null, null, null, null, null,
                // 1 a
                2, 0, 1, 1, 0,
                // 1 b
                2.0f, 0.0f, 1.0f, 1.0f, 0.0f,
                // 1 c
                null, null, null, null, null,

                // 1.0 1
                2.0f, 0.0f, 1.0f, 1.0f, 0.0f,
                // 1.0 1.0
                2.0f, 0.0f, 1.0f, 1.0f, 0.0f,
                // 1.0 null
                null, null, null, null, null,
                // 1.0 a
                2.0f, 0.0f, 1.0f, 1.0f, 0.0f,
                // 1.0 b
                2.0f, 0.0f, 1.0f, 1.0f, 0.0f,
                // 1.0 c
                null, null, null, null, null,

                // null 1
                null, null, null, null, null,
                // null 1.0
                null, null, null, null, null,
                // null null
                null, null, null, null, null,
                // null a
                null, null, null, null, null,
                // null b
                null, null, null, null, null,
                // null c
                null, null, null, null, null,

                // a 1
                2, 0, 1, 1, 0,
                // a 1.0
                2.0f, 0.0f, 1.0f, 1.0f, 0.0f,
                // a null
                null, null, null, null, null,
                // a a
                2, 0, 1, 1, 0,
                // a b
                2.0f, 0.0f, 1.0f, 1.0f, 0.0f,
                // a c
                null, null, null, null, null,

                // b 1
                2.0f, 0.0f, 1.0f, 1.0f, 0.0f,
                // b 1.0
                2.0f, 0.0f, 1.0f, 1.0f, 0.0f,
                // b null
                null, null, null, null, null,
                // b a
                2.0f, 0.0f, 1.0f, 1.0f, 0.0f,
                // b b
                2.0f, 0.0f, 1.0f, 1.0f, 0.0f,
                // b c
                null, null, null, null, null,

                // c 1
                null, null, null, null, null,
                // c 1.0
                null, null, null, null, null,
                // c null
                null, null, null, null, null,
                // c a
                null, null, null, null, null,
                // c b
                null, null, null, null, null,
                // c c
                null, null, null, null, null };

        Map<String, Object> values = new HashMap<>();
        values.put("a", 1);
        values.put("b", 1.0f);
        values.put("c", null);

        int index = 0;
        for (String l : expression)
        {
            for (String r : expression)
            {
                for (String o : operators)
                {
                    assertFunction(results[index++], values, l + " " + o + " " + r);
                }
            }
        }

        assertFunction("hello world", values, "'hello' + ' world'");

        // Test different types
        assertExpressionFail(ArithmeticException.class, "Cannot subtract true", null, "true - 10");
        assertExpressionFail(ArithmeticException.class, "Cannot multiply true", null, "true * 10");
        assertExpressionFail(ArithmeticException.class, "Cannot divide true", null, "true / 10");
        assertExpressionFail(ArithmeticException.class, "Cannot add true", null, "true + 10");
        assertExpressionFail(ArithmeticException.class, "Cannot modulo true", null, "true % 10");
        assertExpressionFail(ArithmeticException.class, "/ by zero", null, "1 / 0");
    }

    @Test
    public void test_comparison() throws Exception
    {
        // CSOFF
        String[] expression = new String[] { "1", "1.0", "null", "a", "b", "c" };
        // CSON
        String[] operators = new String[] { "=", "!=", ">", ">=", "<", "<=" };

        Object[] results = new Object[] {
                // 1 1
                true, false, false, true, false, true,
                // 1 1.0
                true, false, false, true, false, true,
                // 1 null
                null, null, null, null, null, null,
                // 1 a
                true, false, false, true, false, true,
                // 1 b
                true, false, false, true, false, true,
                // 1 c
                null, null, null, null, null, null,

                // 1.0 1
                true, false, false, true, false, true,
                // 1.0 1.0
                true, false, false, true, false, true,
                // 1.0 null
                null, null, null, null, null, null,
                // 1.0 a
                true, false, false, true, false, true,
                // 1.0 b
                true, false, false, true, false, true,
                // 1.0 c
                null, null, null, null, null, null,

                // null 1
                null, null, null, null, null, null,
                // null 1.0
                null, null, null, null, null, null,
                // null null
                null, null, null, null, null, null,
                // null a
                null, null, null, null, null, null,
                // null b
                null, null, null, null, null, null,
                // null c
                null, null, null, null, null, null,

                // a 1
                true, false, false, true, false, true,
                // a 1.0
                true, false, false, true, false, true,
                // a null
                null, null, null, null, null, null,
                // a a
                true, false, false, true, false, true,
                // a b
                true, false, false, true, false, true,
                // a c
                null, null, null, null, null, null,

                // b 1
                true, false, false, true, false, true,
                // b 1.0
                true, false, false, true, false, true,
                // b null
                null, null, null, null, null, null,
                // b a
                true, false, false, true, false, true,
                // b b
                true, false, false, true, false, true,
                // b c
                null, null, null, null, null, null,

                // c 1
                null, null, null, null, null, null,
                // c 1.0
                null, null, null, null, null, null,
                // c null
                null, null, null, null, null, null,
                // c a
                null, null, null, null, null, null,
                // c b
                null, null, null, null, null, null,
                // c c
                null, null, null, null, null, null, };

        Map<String, Object> values = new HashMap<>();
        values.put("a", 1);
        values.put("b", 1.0f);
        values.put("c", null);

        int index = 0;
        for (String l : expression)
        {
            for (String r : expression)
            {
                for (String o : operators)
                {
                    assertExpression(results[index++], values, l + " " + o + " " + r);
                }
            }
        }

        assertExpression(true, values, "'str' = 'str'");
        assertExpression(false, values, "'str' != 'str'");
        assertExpression(false, values, "'str' > 'str'");
        assertExpression(true, values, "'str' >= 'str'");
        assertExpression(false, values, "'str' < 'str'");
        assertExpression(true, values, "'str' <= 'str'");

        assertExpression(true, values, "false = false");
        assertExpression(false, values, "false != false");
        assertExpression(false, values, "false > false");
        assertExpression(true, values, "false >= false");
        assertExpression(false, values, "false < false");
        assertExpression(true, values, "false <= false");

        // Test different types
        assertExpressionFail(IllegalArgumentException.class, "Cannot compare ", null, "getdate() = 10");
        assertExpressionFail(IllegalArgumentException.class, "Cannot compare ", null, "getdate() != 10");
        assertExpressionFail(IllegalArgumentException.class, "Cannot compare ", null, "getdate() > 10");
        assertExpressionFail(IllegalArgumentException.class, "Cannot compare ", null, "getdate() >= 10");
        assertExpressionFail(IllegalArgumentException.class, "Cannot compare ", null, "getdate() < 10");
        assertExpressionFail(IllegalArgumentException.class, "Cannot compare ", null, "getdate() <= 10");
    }
}
