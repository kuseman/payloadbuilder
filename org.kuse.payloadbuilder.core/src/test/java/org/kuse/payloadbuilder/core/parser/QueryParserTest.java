package org.kuse.payloadbuilder.core.parser;

import static java.util.Arrays.asList;

import org.junit.Test;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ArithmeticBinaryExpression.Type;

/** Test query parser */
public class QueryParserTest extends AParserTest
{
    @Test
    public void test_selectItems_failures()
    {
        assertQueryFail(ParseException.class, "Select items inside an ARRAY", "select array(10 col1, 20 col2 from s) col from source s");
        assertQueryFail(ParseException.class, "Cannot have a WHERE clause without a FROM clause", "select object(s.id where false) from source s");
        assertQueryFail(ParseException.class, "Cannot have an ORDER BY clause without a FROM clause", "select object(s.id order by 1) from source s");
    }

    @Test
    public void test_selectItems_assignment()
    {
        assertQuery("select @var = 10");
        assertQuery("select @var = a.col from table a");
        assertQueryFail(ParseException.class, "Cannot combine variable assignment items with data retrieval items", "select @var = a.col, a.col2 from table a");
    }

    @Test
    public void test_selectItems()
    {
        assertQuery("select 'str' myObj from articleName an where an.lang_id = 1 order by an.id asc nulls last, an.id2 desc nulls first, an.id3, an.id4 desc, an.id4 nulls last");

        assertQuery("select an.art_id, a.a_flg = a.b_flg as \"boolean column\" from articleName an");
        assertQuery("select an.art_id \"my shiny field\", a.art_id \"my \"\"new id\", aa.sku_id as \"my new ' id again\" from articleName an");
        assertQuery("select an.art_id, object(an.lang_id, an.id, an.id2 < 0.1 TEST, 'str' \"MY STRING\" from an.brand where brand_id > 0 ) myObj from articleName an");
        assertQuery("select array(array(object(1 col), 'str', null from a), 1.1, a.field from a) arr from article a");

        assertQuery("select array(array(object(1 col), 'str', null from a), 1.1, a.field from a) arr from article a where a.id is not null");
    }

    @Test
    public void test_functions()
    {
        assertExpression("isnull(null, 1+1.1)");
        assertExpression("coalesce(null, 1+1.1)");

        assertExpression("a.filter(x -> x.val > 0).map(x -> x.val).sum(x -> x.val2)");
        assertExpression("a.filter(x -> x.val > 0)");

        QualifiedFunctionCallExpression expected = new QualifiedFunctionCallExpression(
                (ScalarFunctionInfo) session.getCatalogRegistry().resolveFunctionInfo("", "map"),
                asList(
                        new QualifiedFunctionCallExpression(
                                (ScalarFunctionInfo) session.getCatalogRegistry().resolveFunctionInfo("", "flatMap"),
                                asList(
                                        new QualifiedReferenceExpression(QualifiedName.of("aa"), -1),
                                        new LambdaExpression(
                                                asList("x"),
                                                new QualifiedReferenceExpression(QualifiedName.of("x", "ap"), 0),
                                                new int[] {0}))),
                        new LambdaExpression(
                                asList("x"),
                                new QualifiedFunctionCallExpression(
                                        (ScalarFunctionInfo) session.getCatalogRegistry().resolveFunctionInfo("", "cast"),
                                        asList(
                                                new QualifiedReferenceExpression(QualifiedName.of("x", "price_sales"), 0),
                                                new LiteralStringExpression("FLOAT"))),
                                new int[] {0})));

        assertExpression("aa.flatMap(x -> x.ap).map(x -> cast(x.price_sales, float))", expected);
    }

    @Test
    public void test_dereference()
    {
        session.getCatalogRegistry().getBuiltin().registerFunction(new ScalarFunctionInfo(session.getCatalogRegistry().getBuiltin(), "func")
        {
        });
        session.getCatalogRegistry().registerCatalog("utils", new Catalog("utils")
        {
            {
                registerFunction(new ScalarFunctionInfo(this, "func")
                {
                });
                registerFunction(new ScalarFunctionInfo(this, "func2")
                {
                });
            }
        });

        ScalarFunctionInfo hashFunction = (ScalarFunctionInfo) session.getCatalogRegistry().resolveFunctionInfo("", "hash");

        assertExpression("a.b.c", new QualifiedReferenceExpression(QualifiedName.of("a", "b", "c"), -1));
        assertExpression("@list.filter(x -> x.value)",
                new QualifiedFunctionCallExpression(
                        (ScalarFunctionInfo) session.getCatalogRegistry().resolveFunctionInfo("", "filter"),
                        asList(
                                new VariableExpression(QualifiedName.of("list")),
                                new LambdaExpression(asList("x"),
                                        new QualifiedReferenceExpression(QualifiedName.of("x", "value"), 0),
                                        new int[] {0}))));
        assertExpression("a.hash()", new QualifiedFunctionCallExpression(
                hashFunction,
                asList(new QualifiedReferenceExpression(QualifiedName.of("a"), -1))));
        assertExpression("a.hash() + hash(a)",
                new ArithmeticBinaryExpression(
                        Type.ADD,
                        new QualifiedFunctionCallExpression(hashFunction, asList(new QualifiedReferenceExpression(QualifiedName.of("a"), -1))),
                        new QualifiedFunctionCallExpression(hashFunction, asList(new QualifiedReferenceExpression(QualifiedName.of("a"), -1)))));
        assertExpression("a.b.c.hash().value",
                new DereferenceExpression(
                        new QualifiedFunctionCallExpression(hashFunction, asList(new QualifiedReferenceExpression(QualifiedName.of("a", "b", "c"), -1))),
                        new QualifiedReferenceExpression(QualifiedName.of("value"), -1)));
        assertExpression("a.b.c.hash().hash()",
                new QualifiedFunctionCallExpression(hashFunction,
                        asList(new QualifiedFunctionCallExpression(hashFunction, asList(new QualifiedReferenceExpression(QualifiedName.of("a", "b", "c"), -1))))));
        assertExpression("a.b.c.hash().hash(123)",
                new QualifiedFunctionCallExpression(hashFunction,
                        asList(
                                new QualifiedFunctionCallExpression(hashFunction, asList(
                                        new QualifiedReferenceExpression(QualifiedName.of("a", "b", "c"), -1))),
                                new LiteralIntegerExpression(123))));
    }

    @Test
    public void test_control_flow()
    {
        assertQuery("if true then print 'hello' else print 'world' end if");
        assertQuery("print hash(a,b); print hash(10, null); print hash(123, 12.10);");
    }

    @Test
    public void test_joins()
    {
        assertQuery("select art_id from article a");

        // Regular joins
        assertQuery("select art_id from article a inner join articleAttribute aa on aa.art_id = a.art_id");
        assertQuery("select art_id from article a left join articleAttribute aa on aa.art_id = a.art_id");

        // Apply joins
        assertQuery("select art_id from article a cross apply articleAttribute aa");
        assertQuery("select art_id from article a outer apply articleAttribute aa");
        assertQuery("select art_id from article a outer apply range(10) r");
        assertQuery("select art_id from article a outer apply range(@from) r");

        // Populate joins
        assertQuery("select art_id from article a inner join articleAttribute aa with (populate=true) on aa.art_id = a.art_id ");
        assertQuery("select art_id from article a left join articleAttribute aa with (populate=true) on art_id = a.art_id ");

        // Nested
        assertQuery("select art_id from article a inner join (from articleAttribute aa  inner join articlePrice ap on ap.sku_id = aa.sku_id) aa with (populate=true) on aa.art_id = a.art_id ");
        assertQuery(
                "select art_id from article a inner join (from articleAttribute aa  left join articlePrice ap with (populate=true) on ap.sku_id = aa.sku_id) aa with (populate=true) on aa.art_id = a.art_id ");

        // TODO: more parser tests, where, orderby, group by
    }

    @Test
    public void test_ands()
    {
        assertExpression("a and (b or c)");
    }

    @Test
    public void test_expressions()
    {
        assertExpression("1");
        assertExpression("1+1");
        assertExpression("1-1");
        assertExpression("1/1");
        assertExpression("1*1");
        assertExpression("1%1");
        assertExpression("a and b");
        assertExpression("a or b");
        assertExpression("a > 1");
        assertExpression("a >= 1");
        assertExpression("a < 1");
        assertExpression("a <= 1");
        assertExpression("a = 1");
        assertExpression("a != 1");
        assertExpression("not a != 1");
        assertExpression("not a in (1,1,true,2,3.,3,null,false)");
        assertExpression("@value > 10 AND @value_two < 20");
    }

    @Test
    public void test_lambda_and_scopes()
    {
        assertExpression("articleAttribute.filter(a -> a.sku_id > 0)");
        assertExpression("articleAttribute.filter(a -> a.sku_id > 0).map(aa -> aa.sku_id).sum()");
        assertExpression("articleAttribute.filter(a -> a.sku_id > 0).price.map(p -> p.price_sales).sum()");

        assertExpression("articleAttribute.map(aa -> aa.price.map(ap -> ap.campaigns.map(c -> c.camp_name)))");

        assertExpression("articleAttribute.map(aa -> aa.price.map(ap -> ap.campaigns.map(c -> c.camp_name)))");

        // Reuse lambda parameter in sibling scope
        assertExpression("articleAttribute.map(aa -> aa.price.map(a -> a.price_sales) and aa.balance.map(a -> a.balance_disp))");

        // Lambda parameter already in use
        assertExpressionFail(ParseException.class, "Lambda identifier a is already defined in scope", "articleAttribute.map(a -> p.price.map(a -> a.price_sales))");
    }

    private void assertExpression(String expression)
    {
        assertExpression(expression, null);
    }

    private void assertExpression(String expression, Expression expected)
    {
        Expression e = e(expression);
        if (expected != null)
        {
            assertEquals(expected, e);
        }
    }

    private QueryStatement assertQuery(String query)
    {
        return q(query);
    }

    private void assertQueryFail(Class<? extends Exception> expected, String messageContains, String query)
    {
        try
        {
            q(query);
            fail("Query should fail with " + expected + " containing message: " + messageContains);
        }
        catch (Exception e)
        {
            assertTrue("Expected exception " + expected + " but got " + e.getClass(), expected.isAssignableFrom(e.getClass()));
            assertTrue(e.getMessage(), e.getMessage().contains(messageContains));
        }
    }

    private void assertExpressionFail(Class<? extends Exception> expected, String messageContains, String expression)
    {
        try
        {
            e(expression);
            fail();
        }
        catch (Exception e)
        {
            assertTrue(expected.isAssignableFrom(e.getClass()));
            assertTrue(e.getMessage(), e.getMessage().contains(messageContains));
        }
    }
}
