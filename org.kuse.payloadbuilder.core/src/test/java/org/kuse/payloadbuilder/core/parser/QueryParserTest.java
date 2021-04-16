package org.kuse.payloadbuilder.core.parser;

import static java.util.Arrays.asList;

import java.util.List;

import org.junit.Test;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ArithmeticBinaryExpression.Type;

/** Test query parser */
public class QueryParserTest extends AParserTest
{
    @Test
    public void test_set()
    {
        assertQuery("set @var = 10");
        assertQuery("set system.prop = false");
    }

    @Test
    public void test_misc()
    {
        assertQuery("show tables");
        assertQuery("show functions");
        assertQuery("use cat");
        assertQuery("use cat.prop = 10");
        assertQueryFail(ParseException.class, "Cannot assign value to a catalog alias", "use cat = 10");
        assertQueryFail(ParseException.class, "Must provide an assignment value to a catalog property", "use cat.prop");

        assertQuery("if true then print 'hello' end if");
        assertQuery("if true then print 'hello' else print 'world' end if");

        assertQuery("drop table table");
        assertQuery("drop table cat#table");
        assertQuery("drop table if exists table");

        assertQuery("select top 10 1");
        assertQuery("select top (@a) 1");
    }

    @Test
    public void test_selectItems_assignment()
    {
        assertQuery("select @var = 10");
        assertQuery("select @var = a.col from table a");
        assertQueryFail(ParseException.class, "Cannot combine variable assignment items with data retrieval items", "select @var = a.col, a.col2 from table a");

        assertQueryFail(ParseException.class, "Cannot assign to system variables", "select @@rowcount = 1");

        // Test assign a sub query
        assertQuery("select @var = (select 'value' key1, 1234 key2 for object)");
        assertQuery("select @var = (select Value from range(1,100) for array)");
    }

    @Test
    public void test_subquery_expression()
    {
        assertQueryFail(ParseException.class, "Subquery expressions are only allowed in select items", "select * from table where (select col1 > 10)");
        assertQueryFail(ParseException.class, "A FOR clause is mandatory when using a subquery expressions", "select (select 1 col, 2 col)");
        assertQueryFail(ParseException.class, "SELECT INTO are not allowed in sub query expressions", "select (select 1 col, 2 col into #temp for object)");
        assertQueryFail(ParseException.class, "Assignment selects are not allowed in sub query expressions", "select (select @var = 1, 2 col for object)");

        assertQueryFail(ParseException.class, "All select items in OBJECT output must have identifiers", "select (select 1, 2 col for object)");
        assertQueryFail(ParseException.class, "All select items in ARRAY output must have empty identifiers", "select (select 1, 2 col for array)");

        assertQuery("select (select 1, 2 for array)");

        assertQuery(""
            + "select col, x.col2.key "
            + "from "
            + "( "
            + "  select 'value' col, "
            + "        (select 123 key for object) col2"
            + ") x");
    }

    @Test
    public void test_selectItems()
    {
        assertQuery("select 'str' myObj from articleName an where an.lang_id = 1 order by an.id asc nulls last, an.id2 desc nulls first, an.id3, an.id4 desc, an.id4 nulls last");
        assertQuery("select an.art_id, an.a_flg = an.b_flg as \"boolean column\" from articleName an");
        assertQuery("select an.art_id \"my shiny field\", an.art_id \"my \"\"new id\", an.sku_id as \"my new ' id again\" from articleName an");
    }

    @Test
    public void test_describe()
    {
        assertQuery("describe table");
        assertQuery("describe cat#table");
        assertQuery("describe select * from table");
        assertQuery("analyze select * from table");
    }

    @Test
    public void test_functions()
    {
        // Test trigger of wrong parameter types for function
        assertQueryFail(ParseException.class, "Function map expects LambdaExpression as parameter at index 1 but got LiteralIntegerExpression", "select map(1, 2)");

        assertExpression("isnull(null, 1+1.1)");
        assertExpression("coalesce(null, 1+1.1)");

        assertExpression("a.filter(x -> x.val > 0).map(x -> x.val).sum(x -> x.val2)");
        assertExpression("a.filter(x -> x.val > 0)");

        QualifiedFunctionCallExpression expected = new QualifiedFunctionCallExpression(
                (ScalarFunctionInfo) session.getCatalogRegistry().resolveFunctionInfo("", "map").getValue(),
                asList(
                        new QualifiedFunctionCallExpression(
                                (ScalarFunctionInfo) session.getCatalogRegistry().resolveFunctionInfo("", "flatMap").getValue(),
                                asList(
                                        new QualifiedReferenceExpression(QualifiedName.of("aa"), -1, null),
                                        new LambdaExpression(
                                                asList("x"),
                                                new QualifiedReferenceExpression(QualifiedName.of("x", "ap"), 0, null),
                                                new int[] {0})),
                                null),
                        new LambdaExpression(
                                asList("x"),
                                new QualifiedFunctionCallExpression(
                                        (ScalarFunctionInfo) session.getCatalogRegistry().resolveFunctionInfo("", "cast").getValue(),
                                        asList(
                                                new QualifiedReferenceExpression(QualifiedName.of("x", "price_sales"), 0, null),
                                                new LiteralStringExpression("FLOAT")),
                                        null),
                                new int[] {0})),
                null);

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

        ScalarFunctionInfo hashFunction = (ScalarFunctionInfo) session.getCatalogRegistry().resolveFunctionInfo("", "hash").getValue();

        assertExpression("a.b.c", new QualifiedReferenceExpression(QualifiedName.of("a", "b", "c"), -1, null));
        assertExpression("@list.filter(x -> x.value)",
                new QualifiedFunctionCallExpression(
                        (ScalarFunctionInfo) session.getCatalogRegistry().resolveFunctionInfo("", "filter").getValue(),
                        asList(
                                new VariableExpression(QualifiedName.of("list")),
                                new LambdaExpression(asList("x"),
                                        new QualifiedReferenceExpression(QualifiedName.of("x", "value"), 0, null),
                                        new int[] {0})),
                        null));
        assertExpression("a.hash()", new QualifiedFunctionCallExpression(
                hashFunction,
                asList(new QualifiedReferenceExpression(QualifiedName.of("a"), -1, null)), null));
        assertExpression("a.hash() + hash(a)",
                new ArithmeticBinaryExpression(
                        Type.ADD,
                        new QualifiedFunctionCallExpression(hashFunction, asList(new QualifiedReferenceExpression(QualifiedName.of("a"), -1, null)), null),
                        new QualifiedFunctionCallExpression(hashFunction, asList(new QualifiedReferenceExpression(QualifiedName.of("a"), -1, null)), null)));
        assertExpression("a.b.c.hash().value",
                new DereferenceExpression(
                        new QualifiedFunctionCallExpression(hashFunction, asList(new QualifiedReferenceExpression(QualifiedName.of("a", "b", "c"), -1, null)), null),
                        new QualifiedReferenceExpression(QualifiedName.of("value"), -1, null)));
        assertExpression("a.b.c.hash().hash()",
                new QualifiedFunctionCallExpression(hashFunction,
                        asList(new QualifiedFunctionCallExpression(hashFunction, asList(new QualifiedReferenceExpression(QualifiedName.of("a", "b", "c"), -1, null)), null)), null));
        assertExpression("a.b.c.hash().hash(123)",
                new QualifiedFunctionCallExpression(hashFunction,
                        asList(
                                new QualifiedFunctionCallExpression(hashFunction, asList(
                                        new QualifiedReferenceExpression(QualifiedName.of("a", "b", "c"), -1, null)), null),
                                new LiteralIntegerExpression(123)),
                        null));
    }

    @Test
    public void test_subquery()
    {
        assertQueryFail(ParseException.class, "Assignment selects are not allowed in sub query context", "select * from (select @a=1 from tableA) x");
        assertQueryFail(ParseException.class, "SELECT INTO are not allowed in sub query context", "select * from (select 1,2 into #temp from tableA) x");
        assertQueryFail(ParseException.class, "Only a single asterisk select (*) are supported", "select * from (select *,* from tableA) x");
        assertQueryFail(ParseException.class, "Only non alias asterisk select (*) are supported", "select * from (select a.*, a.* from tableA a inner join tableB b on b.id = a.id) x");
        assertQueryFail(ParseException.class, "Missing identifier for select item", "select * from (select 1,2 from tableA) x");
        assertQueryFail(ParseException.class, "FOR clause are not allowed in sub query context", "select * from (select 1 col1 ,2 col2 from tableA for object) x");
        assertQueryFail(ParseException.class, "Sub query must have an alias", "select * from (select 1 col1 ,2 col2 from tableA for object)");

        SelectStatement selectStm = (SelectStatement) assertQuery("select * from tableA a inner join (select * from tableB b) x with (populate=true, batch_size=1000) on x.id = a.id").getStatements()
                .get(0);

        List<Option> joinOptions = selectStm.getSelect().getFrom().getJoins().get(0).getTableSource().getOptions();
        assertEquals(asList(
                new Option(QualifiedName.of("populate"), e("true")),
                new Option(QualifiedName.of("batch_size"), e("1000"))),
                joinOptions);
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
        assertQuery(
                "select art_id from article a inner join (select * from articleAttribute aa  inner join articlePrice ap on ap.sku_id = aa.sku_id) aa with (populate=true) on aa.art_id = a.art_id ");
        assertQuery(
                "select art_id from article a inner join (select * from articleAttribute aa  left join articlePrice ap with (populate=true) on ap.sku_id = aa.sku_id) aa with (populate=true) on aa.art_id = a.art_id ");

        // TODO: more parser tests, where, orderby, group by
    }

    @Test
    public void test_select()
    {
        // Selects without table source
        assertQuery("select 1");
        assertQuery("select 1 where false");
        assertQuery("select 1 order by 1");
        assertQuery("select top 10 1");

        assertQuery("select ( select 'value' key for object) select ( select 'value2' key for object)");

        assertQueryFail(ParseException.class, "Cannot have a GROUP BY clause without a FROM", "select 1 group by 1");
        assertQueryFail(ParseException.class, "FOR clause are not allowed in top select", "select 1 for array");

        assertQueryFail(ParseException.class, "Expected a TABLE function for concat", "select 1 from concat(10)");
    }

    @Test
    public void test_ands()
    {
        assertExpression("a and (b or c)");
    }

    @Test
    public void test_like()
    {
        assertExpression("col like 'hello' and col2 not like 'world'",
                new LogicalBinaryExpression(
                        LogicalBinaryExpression.Type.AND,
                        new LikeExpression(new QualifiedReferenceExpression(QualifiedName.of("col"), -1, null), new LiteralStringExpression("hello"), false, null),
                        new LikeExpression(new QualifiedReferenceExpression(QualifiedName.of("col2"), -1, null), new LiteralStringExpression("world"), true, null)));
    }

    @Test
    public void test_expressions()
    {
        assertExpression("1");
        assertExpression("+1");
        assertExpression("-1");
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
        assertExpression("a IN (1,2,3)");
        assertExpression("a NOT IN (1,2,3)");
        assertExpression("@a");
        assertExpression("@@rowcount");

        assertExpressionFail(ParseException.class, "Expected a SCALAR function for open_rows", "open_rows(10)");
        assertExpressionFail(ParseException.class, "Expected a SCALAR function for open_rows", "'string'.open_rows()");

        assertExpressionFail(ParseException.class, "No function found named: nonono in catalog: BuiltIn", "nonono()");
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
        assertExpressionFail(ParseException.class, "Lambda identifier a is already defined in scope", "articleAttribute.map(a -> a.price.map(a -> a.price_sales))");
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
