package se.kuseman.payloadbuilder.core.parser;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.util.Random;

import org.antlr.v4.runtime.CommonToken;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.api.expression.IArithmeticBinaryExpression;
import se.kuseman.payloadbuilder.api.expression.IDatePartExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.QuerySession;
import se.kuseman.payloadbuilder.core.catalog.CatalogRegistry;
import se.kuseman.payloadbuilder.core.expression.ArithmeticBinaryExpression;
import se.kuseman.payloadbuilder.core.expression.AtTimeZoneExpression;
import se.kuseman.payloadbuilder.core.expression.CastExpression;
import se.kuseman.payloadbuilder.core.expression.DateAddExpression;
import se.kuseman.payloadbuilder.core.expression.DatePartExpression;
import se.kuseman.payloadbuilder.core.expression.LambdaExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralIntegerExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedColumnExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedFunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.VariableExpression;
import se.kuseman.payloadbuilder.core.logicalplan.ConstantScan;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.OperatorFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.OverScan;
import se.kuseman.payloadbuilder.core.logicalplan.Projection;
import se.kuseman.payloadbuilder.core.logicalplan.TableScan;
import se.kuseman.payloadbuilder.core.statement.LogicalSelectStatement;

/** Test of {@link QueryParser} */
public class QueryParserTest extends Assert
{
    private static final QueryParser PARSER = new QueryParser();

    private IExpression add(IExpression left, IExpression right)
    {
        return new se.kuseman.payloadbuilder.core.expression.ArithmeticBinaryExpression(se.kuseman.payloadbuilder.api.expression.IArithmeticBinaryExpression.Type.ADD, left, right);
    }

    private IExpression litString(String str)
    {
        return new se.kuseman.payloadbuilder.core.expression.LiteralStringExpression(str);
    }

    private IExpression litInt(int value)
    {
        return new se.kuseman.payloadbuilder.core.expression.LiteralIntegerExpression(value);
    }

    private IExpression ce(String column)
    {
        return new se.kuseman.payloadbuilder.core.expression.UnresolvedColumnExpression(QualifiedName.of(column), -1, null);
    }

    @Test
    public void test_backtick()
    {
        //@formatter:off
        assertExpression("'hello'+`world ${a+b}`", 
            add(
                litString("hello"),
                new se.kuseman.payloadbuilder.core.expression.TemplateStringExpression(
                    asList(
                        litString("world "),
                        add(ce("a"), ce("b"))))));
        //@formatter:on

        // // Test nested back tick
        //@formatter:off
        assertExpression("`hello ${a+` world ${b}`}`",
                new se.kuseman.payloadbuilder.core.expression.TemplateStringExpression(
                    asList(
                            litString("hello "),
                            add(
                                ce("a"),
                                new se.kuseman.payloadbuilder.core.expression.TemplateStringExpression(
                                    asList(litString(" world "), ce("b")))))));
        //@formatter:on

        assertExpression("`hello`", litString("hello"));
        assertExpression("`${'hello'}`", litString("hello"));
        assertExpression("`hello${' world'}`", litString("hello world"));
    }

    @Test
    public void test_backwards_compability_functions()
    {
        IExpression ce = new UnresolvedColumnExpression(QualifiedName.of("col"), -1, null);

        // cast
        IExpression expected = new CastExpression(ce, ResolvedType.of(Type.DateTime));

        assertExpression("cast(col, 'datetime')", expected);
        assertExpression("cast(col, datetime)", expected);
        assertExpression("cast(col as datetime)", expected);

        // attimezone
        expected = new AtTimeZoneExpression(ce, new se.kuseman.payloadbuilder.core.expression.LiteralStringExpression("Europe/Berlin"));

        assertExpression("attimezone(col, 'Europe/Berlin')", expected);
        assertExpression("col at time zone 'Europe/Berlin'", expected);

        // dateadd
        expected = new DateAddExpression(IDatePartExpression.Part.DAY, LiteralExpression.createLiteralNumericExpression("10"), ce);
        assertExpression("dateadd(day, 10, col)", expected);
        assertExpression("dateadd('day', 10, col)", expected);

        // datepart
        expected = new DatePartExpression(IDatePartExpression.Part.DAY, ce);
        assertExpression("datepart(day, col)", expected);
        assertExpression("datepart('day', col)", expected);

    }

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

        assertQuery("cache flush batch/cache.name");
    }

    @Test
    public void test_cast()
    {
        assertExpression("cast(123 AS ValueVector)", new CastExpression(new se.kuseman.payloadbuilder.core.expression.LiteralIntegerExpression(123), ResolvedType.valueVector(Type.Any)));
        assertExpression("cast(123 AS int)", new CastExpression(new se.kuseman.payloadbuilder.core.expression.LiteralIntegerExpression(123), ResolvedType.of(Type.Int)));
        assertExpression("cast(123 AS long)", new CastExpression(new se.kuseman.payloadbuilder.core.expression.LiteralIntegerExpression(123), ResolvedType.of(Type.Long)));
        assertExpression("cast(123 AS float)", new CastExpression(new se.kuseman.payloadbuilder.core.expression.LiteralIntegerExpression(123), ResolvedType.of(Type.Float)));
        assertExpression("cast(123 AS double)", new CastExpression(new se.kuseman.payloadbuilder.core.expression.LiteralIntegerExpression(123), ResolvedType.of(Type.Double)));
        assertExpression("cast(123 AS boolean)", new CastExpression(new se.kuseman.payloadbuilder.core.expression.LiteralIntegerExpression(123), ResolvedType.of(Type.Boolean)));
    }

    @Test
    public void test_selectItems_assignment()
    {
        assertSelect("select @var = 10");
        assertSelect("select @var = a.col from table a");
        assertSelectFail(ParseException.class, "Cannot combine variable assignment items with data retrieval items", "select @var = a.col, a.col2 from table a");

        assertSelectFail(ParseException.class, "Cannot assign to system variables", "select @@rowcount = 1");

        // Test assign a sub query
        assertSelect("select @var = (select 'value' key1, 1234 key2 for object)");
        assertSelect("select @var = (select Value from range(1,100) for array)");
    }

    @Test
    public void test_subquery_expression()
    {
        //@formatter:off
        se.kuseman.payloadbuilder.core.statement.Statement expected =
                new LogicalSelectStatement(
                    new Projection(
                        new Filter(
                            new TableScan(
                                TableSchema.EMPTY,
                                new TableSourceReference("", QualifiedName.of("table"), "a"),
                                emptyList(),
                                false,
                                emptyList(),
                                null),
                            null,
                            e("a.col > 10")),
                        asList(
                            e("a.col"),
                            new se.kuseman.payloadbuilder.core.expression.SubQueryExpression(
                                new OperatorFunctionScan(
                                    Schema.of(Column.of("output", Type.Any)),
                                    new OverScan(Schema.EMPTY, "a", -1, null),
                                    "",
                                    "objectarray",
                                    null),
                                null)),
                        false),
                    false);
        //@formatter:on
        se.kuseman.payloadbuilder.core.statement.Statement actual = s("select a.col, (select * for objectarray over a) from table a where a.col > 10");

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
                .isEqualTo(expected);

        assertSelectFail(ParseException.class, "Sub query expressions are only supported in projections", "select * from table where (select col1 > 10)");
        assertSelectFail(ParseException.class, "Sub queries without a FOR clause must return a single select item", "select *, (select co1, col2) from table");
        assertSelectFail(ParseException.class, "Sub queries without a FOR clause must return a single select item", "select *, (select co1, col2 from table2) from table");
        assertSelectFail(ParseException.class, "Sub queries without a FOR clause must return a single select item", "select *, (select * from table2) from table");

        assertSelect("select (select 1, 2 for array)");

        //@formatter:off
        assertSelect("" 
                + "select col, x.col2.key " 
                + "from " 
                + "( " 
                + "  select 'value' col, " 
                + "        (select 123 key for object) col2" 
                + ") x");
        //@formatter:on
    }

    @Test
    public void test_selectItems()
    {
        assertSelect("select 'str' myObj from articleName an where an.lang_id = 1 order by an.id asc nulls last, an.id2 desc nulls first, an.id3, an.id4 desc, an.id4 nulls last");
        assertSelect("select an.art_id, an.a_flg = an.b_flg as \"boolean column\" from articleName an");
        assertSelect("select an.art_id \"my shiny field\", an.art_id \"my \"\"new id\", an.sku_id as \"my new ' id again\" from articleName an");
    }

    @Test
    public void test_describe()
    {
        assertQuery("describe select * from table");
        assertQuery("analyze select * from table");
    }

    @Test
    public void test_functions()
    {
        assertExpression("isnull(null, 1+1.1)");
        assertExpression("coalesce(null, 1+1.1)");

        assertExpression("a.filter(x -> x.val > 0).map(x -> x.val).sum(x -> x.val2)");
        assertExpression("a.filter(x -> x.val > 0)");

        //@formatter:off
        UnresolvedFunctionCallExpression expected = new UnresolvedFunctionCallExpression(
                "",
                "map",
                null,
                asList(
                    new UnresolvedFunctionCallExpression(
                        "",
                        "flatMap",
                        null,
                        asList(
                            new UnresolvedColumnExpression(QualifiedName.of("aa"), -1, null),
                            new LambdaExpression(asList("x"),
                                    new UnresolvedColumnExpression(QualifiedName.of("x", "ap"), 0, null), new int[] { 0 })),
                        null),
                    new LambdaExpression(
                        asList("x"),
                        new CastExpression(
                            new UnresolvedColumnExpression(QualifiedName.of("x", "price_sales"), 0, null),
                            ResolvedType.of(Type.Float)),
                        new int[] { 0 })),
                null);
        //@formatter:on

        IExpression actual = e("aa.flatMap(x -> x.ap).map(x -> cast(x.price_sales, float))");

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
                .isEqualTo(expected);
    }

    @Test
    public void test_dereference()
    {
        assertExpression("a.b.c", new UnresolvedColumnExpression(QualifiedName.of("a", "b", "c"), -1, null));
        assertExpression("@list.filter(x -> x.value)", new UnresolvedFunctionCallExpression("", "filter", null,
                asList(new VariableExpression(QualifiedName.of("list")), new LambdaExpression(asList("x"), new UnresolvedColumnExpression(QualifiedName.of("x", "value"), 0, null), new int[] { 0 })),
                null));
        assertExpression("a.hash()", new UnresolvedFunctionCallExpression("", "hash", null, asList(new UnresolvedColumnExpression(QualifiedName.of("a"), -1, null)), null));
        assertExpression("a.hash() + hash(a)",
                new ArithmeticBinaryExpression(IArithmeticBinaryExpression.Type.ADD,
                        new UnresolvedFunctionCallExpression("", "hash", null, asList(new UnresolvedColumnExpression(QualifiedName.of("a"), -1, null)), null),
                        new UnresolvedFunctionCallExpression("", "hash", null, asList(new UnresolvedColumnExpression(QualifiedName.of("a"), -1, null)), null)));
        assertExpression("a.b.c.hash().value", new se.kuseman.payloadbuilder.core.expression.DereferenceExpression(
                new UnresolvedFunctionCallExpression("", "hash", null, asList(new UnresolvedColumnExpression(QualifiedName.of("a", "b", "c"), -1, null)), null), "value"));
        assertExpression("a.b.c.hash().hash()", new UnresolvedFunctionCallExpression("", "hash", null,
                asList(new UnresolvedFunctionCallExpression("", "hash", null, asList(new UnresolvedColumnExpression(QualifiedName.of("a", "b", "c"), -1, null)), null)), null));
        assertExpression("a.b.c.hash().hash(123)",
                new UnresolvedFunctionCallExpression("", "hash", null,
                        asList(new UnresolvedFunctionCallExpression("", "hash", null, asList(new UnresolvedColumnExpression(QualifiedName.of("a", "b", "c"), -1, null)), null),
                                new LiteralIntegerExpression(123)),
                        null));
    }

    @Test
    public void test_subquery()
    {
        assertSelectFail(ParseException.class, "Assignment selects are not allowed in sub query context", "select * from (select @a=1 from tableA) x");
        assertSelectFail(ParseException.class, "SELECT INTO are not allowed in sub query context", "select * from (select 1,2 into #temp from tableA) x");
        assertSelectFail(ParseException.class, "Sub query must have an alias", "select * from (select 1 col1 ,2 col2 from tableA for object)");
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
        assertSelect("select art_id from article a");

        // Regular joins
        assertSelect("select a.art_id from article a inner join articleAttribute aa on aa.art_id = a.art_id");
        assertSelect("select a.art_id from article a left join articleAttribute aa on aa.art_id = a.art_id");
        assertSelect("select a.art_id from article a cross join articleAttribute aa ");
        assertSelectFail(ParseException.class, "A CROSS/OUTER join cannot have a join condition", "select * from tableA a cross join tableB b on a.col = b.col");
        assertSelectFail(ParseException.class, "A INNER/LEFT/RIGHT join must have a join condition", "select * from tableA a inner join tableB b");
        assertSelectFail(ParseException.class, "A INNER/LEFT/RIGHT join must have a join condition", "select * from tableA a left join tableB b");
        assertSelectFail(ParseException.class, "A INNER/LEFT/RIGHT join must have a join condition", "select * from tableA a right join tableB b");

        // Apply joins
        assertSelect("select a.art_id from article a cross apply articleAttribute aa");
        assertSelect("select a.art_id from article a outer apply articleAttribute aa");
        assertSelect("select a.art_id from article a outer apply range(10) r");
        assertSelect("select a.art_id from article a outer apply range(@from) r");

        // Populate joins
        assertSelect("select a.art_id from article a inner populate join articleAttribute aa on aa.art_id = a.art_id ");
        assertSelect("select a.art_id from article a left populate join articleAttribute aa on aa.art_id = a.art_id ");

        // Nested
        assertSelect(
                "select a.art_id from article a inner join (select * from articleAttribute aa  inner join articlePrice ap on ap.sku_id = aa.sku_id) aa with (populate=true) on aa.art_id = a.art_id ");
        assertSelect(
                // CSOFF
                "select a.art_id from article a inner join (select * from articleAttribute aa  left join articlePrice ap with (populate=true) on ap.sku_id = aa.sku_id) aa with (populate=true) on aa.art_id = a.art_id ");
        // CSON

        // TODO: more parser tests, where, orderby, group by
    }

    @Test
    public void test_select()
    {
        // Selects without table source
        assertEquals(new LogicalSelectStatement(new Projection(ConstantScan.INSTANCE, asList(litInt(1)), false), false), assertSelect("select 1"));
        assertSelect("select 1 where false");
        assertSelect("select 1 order by 1");
        assertSelect("select top 10 1");

        assertQuery("select ( select 'value' key for object) select ( select 'value2' key for object)");

        assertSelectFail(ParseException.class, "Cannot have a GROUP BY clause without a FROM", "select 1 group by 1");

        assertSelectFail(ParseException.class, "Must specify table source", "select *");
        assertSelectFail(ParseException.class, "Must specify table source", "select (select *)");
        assertSelectFail(ParseException.class, "Must specify table source", "select (select * for object)");
    }

    @Test
    public void test_ands()
    {
        assertExpression("a and (b or c)");
    }

    @Test
    public void test_like()
    {
        //@formatter:off
        assertExpression("col like 'hello' and col2 not like 'world'",
                new se.kuseman.payloadbuilder.core.expression.LogicalBinaryExpression(se.kuseman.payloadbuilder.api.expression.ILogicalBinaryExpression.Type.AND,
                        new se.kuseman.payloadbuilder.core.expression.LikeExpression(
                                new UnresolvedColumnExpression(QualifiedName.of("col"), -1, null),
                                new se.kuseman.payloadbuilder.core.expression.LiteralStringExpression("hello"), false, null),
                        new se.kuseman.payloadbuilder.core.expression.LikeExpression(
                                new UnresolvedColumnExpression(QualifiedName.of("col2"), -1, null),
                                new se.kuseman.payloadbuilder.core.expression.LiteralStringExpression("world"), true, null)));
        //@formatter:on
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

    protected void assertSelectFail(Class<? extends Exception> expected, String messageContains, String query)
    {
        try
        {
            s(query);
            fail("Query should fail with " + expected + " containing message: " + messageContains);
        }
        catch (Exception e)
        {
            if (!expected.isAssignableFrom(e.getClass()))
            {
                throw e;
            }
            assertTrue(e.getMessage(), isNotBlank(messageContains)
                    && e.getMessage()
                            .contains(messageContains));
        }
    }

    protected void assertQueryFail(Class<? extends Exception> expected, String messageContains, String query)
    {
        try
        {
            assertQuery(query);
            fail("Query should fail with " + expected + " containing message: " + messageContains);
        }
        catch (Exception e)
        {
            if (!expected.isAssignableFrom(e.getClass()))
            {
                throw e;
            }
            assertTrue(e.getMessage(), isNotBlank(messageContains)
                    && e.getMessage()
                            .contains(messageContains));
        }
    }

    private IExpression assertExpression(String expression)
    {
        return assertExpression(expression, null);
    }

    private IExpression assertExpression(String expression, IExpression expected)
    {
        IExpression e = e(expression);
        if (expected != null)
        {
            assertEquals(expected, e);
        }
        return e;
    }

    private se.kuseman.payloadbuilder.core.statement.Statement assertSelect(String select)
    {
        return s(select);
    }

    private se.kuseman.payloadbuilder.core.statement.QueryStatement assertQuery(String query)
    {
        return PARSER.parseQuery(new QuerySession(new CatalogRegistry()), query);
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
            assertTrue(e.getMessage(), e.getMessage()
                    .contains(messageContains));
        }
    }

    private IExpression e(String expression)
    {
        return PARSER.parseExpression(expression);
    }

    private se.kuseman.payloadbuilder.core.statement.Statement s(String expression)
    {
        return PARSER.parseSelect(expression);
    }
}
