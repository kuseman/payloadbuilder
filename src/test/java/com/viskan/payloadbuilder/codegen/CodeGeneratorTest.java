package com.viskan.payloadbuilder.codegen;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.catalog.FunctionInfo.Type;
import com.viskan.payloadbuilder.catalog.ScalarFunctionInfo;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;
import com.viskan.payloadbuilder.parser.QueryParser;
import com.viskan.payloadbuilder.parser.tree.Expression;

import static java.util.Arrays.asList;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

/** Test of {@link CodeGenerator} */
public class CodeGeneratorTest extends Assert
{
    private final CatalogRegistry catalogRegistry = new CatalogRegistry();
    private final QueryParser parser = new QueryParser();
    private final CodeGenerator codeGenerator = new CodeGenerator();
    
    @Test
    public void test_dereference() throws Exception
    {
        TableAlias alias = TableAlias.of(null, "article", "a");
        alias.setColumns(new String[] {"a"});
        Row row = Row.of(alias, 0, new Object[] { asList(
                Row.of(alias, 0, new Object[] { 1 }),
                Row.of(alias, 1, new Object[] { 2 }),
                Row.of(alias, 2, new Object[] { 3 }),
                Row.of(alias, 3, new Object[] { 4 })
                )});
        
        assertExpression(true, row, "a.filter(a -> a.a = 2).a");
    }
    
    @Test
    public void test_auto_cast_strings() throws Exception
    {
        assertFail(IllegalArgumentException.class, "true = '1.0'");
        assertFail(IllegalArgumentException.class, "1 = '1.0'");
        
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
    }
    
    @Test
    public void test_1() throws Exception
    {
        TableAlias alias = TableAlias.of(null, "article", "a");
        alias.setColumns(new String[] {"a", "b", "c", "d" , "e", "f"});
        Row row = Row.of(alias, 0, new Object[] {true, false, false, false, false, null });

        assertExpression(true, row, "1 + 1 > 10 - 9");
        
        assertExpression(null, row, "null and true");
        assertExpression(null, row, "true and null");
        assertExpression(true, row, "null or true");
        assertExpression(true, row, "true or null");
        assertExpression(null, row, "null or false");
        assertExpression(null, row, "false or null");
        assertExpression(false, row, "false and true");
        assertExpression(true, row, "true and true");
        assertExpression(false, row, "false and false");

        assertExpression(false, row, "true and false and true");

        assertExpression(null, row, "true and null and true");
        assertExpression(null, row, "not (true and null and true)");
        assertExpression(null, row, "not (null)");
        assertExpression(false, row, "true and null and true and 1 > 1 and 1 != 2 and 1 = null");
        assertExpression(true, row, "a and not (d or e)");
        assertExpression(false, row, "true and not true");        
        assertExpression(false, row, "(1 = 0 or 3 in(1,2) or 2 in(3,2)) and not (4 in(4) or 5 in(5))");
        assertExpression(null, row, "null in (1,2,3)");
        assertExpression(false, row, "1 in (null,2,3)");
        
        assertExpression(true, row, "null is null");
        assertExpression(false, row, "null is not null");
        assertExpression(true, row, "a is not null");
        assertExpression(true, row, "f is null");
        assertExpression(true, row, "f is null and a");
        assertExpression(994, row, "hash(1,2)");
        assertExpression(null, row, "hash(1,null)");
        
        assertExpression(-2L, row, "-2");
    }
    
    @Test
    public void test_catalog() throws Exception
    {
        Catalog utils = new Catalog("UTILS") {};
        utils.registerFunction(new ScalarFunctionInfo(utils, "uuid", Type.SCALAR)
        {
            @Override
            public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode, List<Expression> arguments)
            {
                ExpressionCode code = ExpressionCode.code(context);
                code.setCode(String.format(
                        "boolean %s = false;\n"
                        + "String %s = java.util.UUID.randomUUID().toString();\n",
                        code.getIsNull(), code.getResVar()));
                return code;
            }
            
            @Override
            public Object eval(EvaluationContext context, List<Expression> arguments, Row row)
            {
                return java.util.UUID.randomUUID().toString();
            }
        });
        catalogRegistry.registerCatalog(utils);
        
        assertExpression(true, null, "UTILS.uuid() is not null");
    }
    
    @Test
    public void test_and() throws Exception
    {
        TableAlias alias = TableAlias.of(null, "article", "a");
        alias.setColumns(new String[] {"a", "b", "c", "d" , "e"});
        Row row = Row.of(alias, 0, new Object[] {true, false, false, false, false });

        assertExpression(true, row, "true and true");
        assertExpression(false, row, "true and false");
        assertExpression(null, row, "true and null");
        assertExpression(false, row, "false and true");
        assertExpression(false, row, "false and false");
        assertExpression(false, row, "false and null");
    }

    @Test
    public void test_literal() throws Exception
    {
        TableAlias alias = TableAlias.of(null, "article", "a");
        alias.setColumns(new String[] {"a", "b", "c", "d" , "e"});
        Row row = Row.of(alias, 0, new Object[] {true, false, false, false, false });

        assertExpression(1L, row, "1");
        assertExpression(1.1, row, "1.1");
        assertExpression(10L, row, "10");
        assertExpression("hello world", row, "'hello world'");
        assertExpression(null, row, "null");
        assertExpression(false, row, "false");
        assertExpression(true, row, "true");
    }

    @Test
    public void test_in_expression() throws Exception
    {
        TableAlias alias = TableAlias.of(null, "article", "a");
        alias.setColumns(new String[] {"a", "b", "c", "d", "e", "f", "g"});
        Row row = Row.of(alias, 0, new Object[] {1, null, asList(1, 2)});

        assertExpression(false, row, "(1+2+3+4) > 123 and (1.0 <20)");
        assertExpression(null, row, "null in(1,2,3,'str')");
        assertExpression(true, row, "a in(1,null,3,'str')");
        assertExpression(true, row, "'str' in(1,2,3,'str')");
        assertExpression(true, row, "1 in(c, 1,2,3,'str', c)");
        assertExpression(false, row, "10.1 in(c, 1,2,3,'str', c)");
    }

    @SuppressWarnings("unchecked")
    public <T> Set<T> asSet(T... items)
    {
        return new HashSet<>(asList(items));
    }

//    @Ignore
//    @Test
//    public void test_performance() throws Exception
//    {
//        TableAlias sourceMeta = TableAlias.of(null, "source", "s");
//        sourceMeta.setColumns(new String[] {"art_id", "sku_id"});
//        TableAlias campaignMeta = TableAlias.of(sourceMeta, "campaign", "c");
//        campaignMeta.setColumns(new String[] {"apply_to_articles", "includeArtIds", "includeSkuIds", "excludeArtIds", "excludeSkuIds"});
//
//        Random rnd = new Random();
//        // _source (art_id, sku_id)
//        Operator sourceScan = new ListScan(
//                sourceMeta,
//                new CachedSupplier<>(() -> IntStream.range(1, 300000).mapToObj(i -> new Object[] {i, i / 5 + i}).collect(Collectors.toList())));
//
//        // articleAttribute ("apply_to_articles", "includeArtIds", "includeSkuIds", "excludeArtIds", "excludeSkuIds")
//        Operator campaignScan = new ListScan(
//                campaignMeta,
//                new CachedSupplier<>(() -> IntStream.range(1, 200)
//                        .mapToObj(i -> new Object[] {0, asList(rnd.nextInt(300000), rnd.nextInt(300000), rnd.nextInt(300000), rnd.nextInt(300000)), asList(rnd.nextInt(1000)),
//                                asList(rnd.nextInt(1000)), asList(rnd.nextInt(1000))})
//                        .collect(Collectors.toList())));
//
//        //        "(c.apply_to_articles = 1 or ( s.art_id in (c.includeArtIds) or s.sku_id in (c.includeSkuIds) ))"
//
//        String expression = "(c.apply_to_articles = 1 or (s.art_id in (c.includeArtIds) or s.sku_id in (c.includeSkuIds)))  and not (s.art_id in (c.excludeArtIds) or s.sku_id in (c.excludeSkuIds) )";
//
//        /*
//         *  Input r_in_apply_to_articles = new Input("apply_to_articles");
//            Input r_out_art_id = new Input("art_id");
//            Input r_in_includeArtIds = new Input("includeArtIds");
//            Input r_out_sku_id = new Input("sku_id");
//            Input r_in_includeSkuIds = new Input("includeSkuIds");
//            Input r_in_excludeArtIds = new Input("excludeArtIds");
//            Input r_in_excludeSkuIds = new Input("excludeSkuIds");
//         *
//         */
//        
//        Expression e = parser.parseExpression(catalogRegistry, expression);
//
//
//        CodeGenerator gen = new CodeGenerator();
//        Predicate<Row> predicate = gen.generatePredicate(campaignMeta, e);
//        
////        BiPredicate<Row, Row> predicate = getBiPredicate(expression, campaignMeta);
//        //                ExpressionCodeGenVisitorTest::test;// getBiPredicate(expression, campaignMeta);
//
//        Operator op = new NestedLoop(
//                sourceScan,
//                campaignScan,
//                predicate,
//                DefaultRowMerger.DEFAULT);
//
//        for (int i = 0; i < 100; i++)
//        {
//            StopWatch sw = new StopWatch();
//            sw.start();
//            int rowCount = 0;
//            int innerCount = 0;
//            Iterator<Row> it = op.open(new OperatorContext());
//            while (it.hasNext())
//            {
//                Row row = it.next();
//                innerCount += row.getChildRows(0).size();
//                rowCount++;
//            }
//            sw.stop();
//            System.out.println("Row count: " + rowCount + " Inner count: " + innerCount + ", Memory: "
//                + FileUtils.byteCountToDisplaySize(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) + " Time: "
//                + sw.toString());
//        }
//
//    }

    @Test
    public void test_qualifiednames() throws Exception
    {
        TableAlias alias = TableAlias.of(null, "article", "a");
        alias.setColumns(new String[] {"a", "b", "c", "d", "e", "f", "g"});
        Row row = Row.of(alias, 0, new Object[] {1, null, true, 1.1, 1.0, "string", "string2"});
        assertExpression(false, row, "a > 10");
        assertExpression(null, row, "b > 10");
        assertExpression(false, row, "a > 10.0");
        assertExpression(false, row, "a > d");
        assertExpression(false, row, "a >= d");
        assertExpression(true, row, "a < d");
        assertExpression(true, row, "a <= d");
        assertExpression(false, row, "a = d");
        assertExpression(true, row, "a != d");
        assertExpression(true, row, "a = e");
        assertExpression(false, row, "a != e");
        assertExpression(true, row, "f != g");
        assertExpression(false, row, "f = g");
        assertExpression(false, row, "f > g");
        assertExpression(true, row, "f < g");
    }
    
    @Test
    public void test_booleanExpression() throws Exception
    {
        String[] expression = new String[] {"true", "false", "null", "a", "b", "c"};
        String[] operators = new String[] {"and", "or"};

        Object[] results = new Object[] {
                // true true
                true, true,
                // true false
                false, true,
                // true null
                null, true,
                // true a
                true, true,
                // true b
                false, true,
                // true c
                null, true,

                // false true
                false, true,
                // false false
                false, false,
                // false null
                false, null,
                // false a
                false, true,
                // false b
                false, false,
                // false c
                false, null,

                // null true
                null, true,
                // null false
                false, null,
                // null null
                null, null,
                // null a
                null, true,
                // null b
                false, null,
                // null c
                null, null,

                // a true
                true, true,
                // a false
                false, true,
                // a null
                null, true,
                // a a
                true, true,
                // a b
                false, true,
                // a c
                null, true,

                // b true
                false, true,
                // b false
                false, false,
                // b null
                false, null,
                // b a
                false, true,
                // b b
                false, false,
                // b c
                false, null,

                // c true
                null, true,
                // c false
                false, null,
                // c null
                null, null,
                // c a
                null, true,
                // c b
                false, null,
                // c c
                null, null,
        };

        TableAlias alias = TableAlias.of(null, "article", "a");
        alias.setColumns(new String[] {"a", "b", "c", "d"});
        Row row = Row.of(alias, 0, new Object[] {true, false, null, 10});

        int index = 0;
        for (String l : expression)
        {
            for (String r : expression)
            {
                for (String o : operators)
                {
                    assertExpression(results[index++], row, l + " " + o + " " + r);
                }
            }
        }

        assertExpression(false, row, "not (1 > 0)");

        // Test different types
        assertFail(ClassCastException.class, row, "a and d");
        assertFail(ClassCastException.class, row, "b or d");
    }

    @Test
    public void test_arithmetic() throws Exception
    {
        String[] expression = new String[] {"1", "1.0", "null", "a", "b", "c"};
        String[] operators = new String[] {"+", "-", "*", "/", "%"};

        Object[] results = new Object[] {
                // 1  1
                2L, 0L, 1L, 1L, 0L,
                // 1  1.0
                2.0, 0.0, 1.0, 1.0, 0.0,
                // 1  null
                null, null, null, null, null,
                // 1  a
                2L, 0L, 1L, 1L, 0L,
                // 1  b
                2.0, 0.0, 1.0, 1.0, 0.0,
                // 1  c
                null, null, null, null, null,

                // 1.0  1
                2.0, 0.0, 1.0, 1.0, 0.0,
                // 1.0  1.0
                2.0, 0.0, 1.0, 1.0, 0.0,
                // 1.0  null
                null, null, null, null, null,
                // 1.0  a
                2.0, 0.0, 1.0, 1.0, 0.0,
                // 1.0  b
                2.0, 0.0, 1.0, 1.0, 0.0,
                // 1.0  c
                null, null, null, null, null,

                // null  1
                null, null, null, null, null,
                // null  1.0
                null, null, null, null, null,
                // null  null
                null, null, null, null, null,
                // null  a
                null, null, null, null, null,
                // null  b
                null, null, null, null, null,
                // null  c
                null, null, null, null, null,

                // a  1
                2L, 0L, 1L, 1L, 0L,
                // a  1.0
                2.0, 0.0, 1.0, 1.0, 0.0,
                // a  null
                null, null, null, null, null,
                // a  a
                2L, 0L, 1L, 1L, 0L,
                // a  b
                2.0, 0.0, 1.0, 1.0, 0.0,
                // a  c
                null, null, null, null, null,

                // b  1
                2.0, 0.0, 1.0, 1.0, 0.0,
                // b  1.0
                2.0, 0.0, 1.0, 1.0, 0.0,
                // b  null
                null, null, null, null, null,
                // b  a
                2.0, 0.0, 1.0, 1.0, 0.0,
                // b  b
                2.0, 0.0, 1.0, 1.0, 0.0,
                // b  c
                null, null, null, null, null,

                // c  1
                null, null, null, null, null,
                // c  1.0
                null, null, null, null, null,
                // c  null
                null, null, null, null, null,
                // c  a
                null, null, null, null, null,
                // c  b
                null, null, null, null, null,
                // c  c
                null, null, null, null, null
        };

        TableAlias alias = TableAlias.of(null, "article", "a");
        alias.setColumns(new String[] {"a", "b", "c"});
        Row row = Row.of(alias, 0, new Object[] {1, 1.0, null});

        int index = 0;
        for (String l : expression)
        {
            for (String r : expression)
            {
                for (String o : operators)
                {
                    assertExpression(results[index++], row, l + " " + o + " " + r);
                }
            }
        }

        assertExpression("hello world", row, "'hello' + ' world'");

        // Test different types
        assertFail(ArithmeticException.class, "true + 10");
        assertFail(ArithmeticException.class, "true - 10");
        assertFail(ArithmeticException.class, "true * 10");
        assertFail(ArithmeticException.class, "true / 10");
        assertFail(ArithmeticException.class, "true % 10");
        assertFail(ArithmeticException.class, "1 / 0");
    }

    @Test
    public void test_comparison() throws Exception
    {
        String[] expression = new String[] {"1", "1.0", "null", "a", "b", "c"};
        String[] operators = new String[] {"=", "!=", ">", ">=", "<", "<="};

        Object[] results = new Object[] {
                // 1  1
                true, false, false, true, false, true,
                // 1  1.0
                true, false, false, true, false, true,
                // 1  null
                null, null, null, null, null, null,
                // 1  a
                true, false, false, true, false, true,
                // 1  b
                true, false, false, true, false, true,
                // 1  c
                null, null, null, null, null, null,

                // 1.0  1
                true, false, false, true, false, true,
                // 1.0  1.0
                true, false, false, true, false, true,
                // 1.0  null
                null, null, null, null, null, null,
                // 1.0  a
                true, false, false, true, false, true,
                // 1.0  b
                true, false, false, true, false, true,
                // 1.0  c
                null, null, null, null, null, null,

                // null  1
                null, null, null, null, null, null,
                // null  1.0
                null, null, null, null, null, null,
                // null  null
                null, null, null, null, null, null,
                // null  a
                null, null, null, null, null, null,
                // null  b
                null, null, null, null, null, null,
                // null  c
                null, null, null, null, null, null,

                // a  1
                true, false, false, true, false, true,
                // a  1.0
                true, false, false, true, false, true,
                // a  null
                null, null, null, null, null, null,
                // a  a
                true, false, false, true, false, true,
                // a  b
                true, false, false, true, false, true,
                // a  c
                null, null, null, null, null, null,

                // b  1
                true, false, false, true, false, true,
                // b  1.0
                true, false, false, true, false, true,
                // b  null
                null, null, null, null, null, null,
                // b  a
                true, false, false, true, false, true,
                // b  b
                true, false, false, true, false, true,
                // b  c
                null, null, null, null, null, null,

                // c  1
                null, null, null, null, null, null,
                // c  1.0
                null, null, null, null, null, null,
                // c  null
                null, null, null, null, null, null,
                // c  a
                null, null, null, null, null, null,
                // c  b
                null, null, null, null, null, null,
                // c  c
                null, null, null, null, null, null,
        };

        TableAlias alias = TableAlias.of(null, "article", "a");
        alias.setColumns(new String[] {"a", "b", "c"});
        Row row = Row.of(alias, 0, new Object[] {1, 1.0, null});

        int index = 0;
        for (String l : expression)
        {
            for (String r : expression)
            {
                for (String o : operators)
                {
                    assertExpression(results[index++], row, l + " " + o + " " + r);
                }
            }
        }

        assertExpression(true, row, "'str' = 'str'");
        assertExpression(false, row, "'str' != 'str'");
        assertExpression(false, row, "'str' > 'str'");
        assertExpression(true, row, "'str' >= 'str'");
        assertExpression(false, row, "'str' < 'str'");
        assertExpression(true, row, "'str' <= 'str'");

        assertExpression(true, row, "false = false");
        assertExpression(false, row, "false != false");
        assertExpression(false, row, "false > false");
        assertExpression(true, row, "false >= false");
        assertExpression(false, row, "false < false");
        assertExpression(true, row, "false <= false");

        // Test different types
        assertFail(IllegalArgumentException.class, "true = 10");
        assertFail(IllegalArgumentException.class, "true != 10");
        assertFail(IllegalArgumentException.class, "true > 10");
        assertFail(IllegalArgumentException.class, "true >= 10");
        assertFail(IllegalArgumentException.class, "true < 10");
        assertFail(IllegalArgumentException.class, "true <= 10");
    }

    private void assertFail(Class<? extends Exception> e, String expression)
    {
        assertFail(e, null, expression);
    }
    
    private void assertFail(Class<? extends Exception> e, Row row, String expression)
    {
        try
        {
            Expression expr = parser.parseExpression(catalogRegistry, expression);
            codeGenerator.generateFunction(null, expr).apply(row);
            fail(expression + " should fail.");
        }
        catch (Exception ee)
        {
            assertEquals(e, ee.getClass());
        }
    }

    private void assertExpression(Object value, Row row, String expression) throws Exception
    {
        TableAlias alias = row != null ? row.getTableAlias() : TableAlias.of(null, "article", "a");

        try
        {
            Expression expr = parser.parseExpression(catalogRegistry, expression);
            BaseFunction function = codeGenerator.generateFunction(alias, expr);
            assertEquals(expression, value, function.apply(row));
            assertEquals("Eval: " + expression, value, expr.eval(new EvaluationContext(), row));
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail(expression + " " + e.getMessage());
        }
    }
}
