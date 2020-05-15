package com.viskan.payloadbuilder.parser;

import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.parser.tree.Expression;

import org.junit.Assert;
import org.junit.Test;

/** Test query parser */
public class QueryParserTest extends Assert
{
    private final QueryParser parser = new QueryParser();
    private final CatalogRegistry catalogRegistry = new CatalogRegistry();

    @Test
    public void test_selectItems_failures()
    {
        assertQueryFail(IllegalArgumentException.class, "ARRAY select requires a from clause", "select array(10 col1, 20 col2) col from source s");
        assertQueryFail(IllegalArgumentException.class, "Select items inside an ARRAY", "select array(10 col1, 20 col2 from s) col from source s");
        assertQueryFail(IllegalArgumentException.class, "Select items inside an OBJECT", "select object(10) col from source s");
        assertQueryFail(IllegalArgumentException.class, "Select items on ROOT level", "select 'value' from source s");
        assertQueryFail(IllegalArgumentException.class, "Cannot have a WHERE clause without a FROM clause", "select object(s.id where false) from source s");
        assertQueryFail(IllegalArgumentException.class, "Cannot have an ORDER BY clause without a FROM clause", "select object(s.id order by 1) from source s");
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
        
        // Populate joins
        assertQuery("select art_id from article a inner join [articleAttribute] aa on aa.art_id = a.art_id ");
        assertQuery("select art_id from article a left join [articleAttribute] aa on art_id = a.art_id ");
        
        // Nested
        assertQuery("select art_id from article a inner join [articleAttribute aa  inner join articlePrice ap on ap.sku_id = aa.sku_id] aa on aa.art_id = a.art_id ");
        assertQuery("select art_id from article a inner join [articleAttribute aa  left join [articlePrice] ap on ap.sku_id = aa.sku_id] aa on aa.art_id = a.art_id ");
        
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

        // Should not dereference non qualified expressions
        assertExpressionFail(IllegalArgumentException.class, "Can only dereference qualified references or functions", "'rere'.filter(a -> a.sku_id > 0)");
        // Lambda parameter already in use
        assertExpressionFail(IllegalArgumentException.class, "Lambda identifier a is already defined in scope", "articleAttribute.map(a -> p.price.map(a -> a.price_sales))");
    }

    @Test
    public void test_queries()
    {
        //        assertQuery("inner join article a ()");
        //
        //        assertQuery("inner join article a (inner join article b ())");
        //
        //        assertQuery(""
        //            + "inner join article a ("
        //            + "),"
        //            + "left join article_attribute_media aam ("
        //            + ")");
        //
        //        assertQuery(""
        //                + "inner join article a ("
        //                + "  inner join articleBrand ab ("
        //                + "    on ab.artileBrandId == a.articleBrandId"
        //                + "  )"
        //                + "),"
        //                + "left join article_attribute_media aam ("
        //                + ")");
        //        
        //        assertQuery(
        //              "inner join article a ("
        //            + "  on a.art_id == _source.art_id"
        //            + ") "
        //            + "where a.art_id == _source.art_id "
        //            + "order by a.art_id"
        //            );
    }

    private void assertExpression(String expression)
    {
        Expression e = parser.parseExpression(catalogRegistry, expression);
        System.out.println(e);
    }

    private void assertQuery(String query)
    {
        parser.parseQuery(catalogRegistry, query);
    }

    private void assertQueryFail(Class<? extends Exception> expected, String messageContains, String query)
    {
        try
        {
            parser.parseQuery(catalogRegistry, query);
        }
        catch (Exception e)
        {
            assertTrue(expected.isAssignableFrom(e.getClass()));
            assertTrue(e.getMessage(), e.getMessage().contains(messageContains));
        }
    }

    private void assertExpressionFail(Class<? extends Exception> expected, String messageContains, String expression)
    {
        try
        {
            parser.parseExpression(catalogRegistry, expression);
            fail();
        }
        catch (Exception e)
        {
            assertTrue(expected.isAssignableFrom(e.getClass()));
            assertTrue(e.getMessage(), e.getMessage().contains(messageContains));
        }
    }
}
