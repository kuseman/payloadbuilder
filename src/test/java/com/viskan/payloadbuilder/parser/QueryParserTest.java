package com.viskan.payloadbuilder.parser;

import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.parser.tree.Expression;
import com.viskan.payloadbuilder.parser.tree.Query;

import org.junit.Assert;
import org.junit.Test;

/** Test query parser */
public class QueryParserTest extends Assert
{
    private final QueryParser parser = new QueryParser();
    private final CatalogRegistry catalogRegistry = new CatalogRegistry();
    
    @Test
    public void test_selectItems()
    {
        assertQuery("select 'str' myObj from articleName an where an.lang_id = 1 order by an.id asc nulls last, an.id2 desc nulls first, an.id3, an.id4 desc, an.id4 nulls last");
        
        assertQuery("select an.art_id, a.a_flg = a.b_flg as \"boolean column\" from articleName an");
        assertQuery("select an.art_id \"my shiny field\", a.art_id \"my \"\"new id\", aa.sku_id as \"my new ' id again\" from articleName an");
        assertQuery("select an.art_id, object(an.lang_id, an.id, an.id2 < 0.1 TEST, 'str' \"MY STRING\" from an.brand where brand_id > 0 ) myObj from articleName an");
        assertQuery("select array(array(object(1), 'str', null), 1.1, a.field) from article a");

        assertQuery("select array(array(object(1), 'str', null), 1.1, a.field) from article a where a.id is not null");
    }
    
    @Test
    public void test_joins()
    {
        assertQuery("select art_id from article a");
        assertQuery("select art_id from article a inner join articleAttribute aa on aa.art_id = a.art_id");
        assertQuery("select art_id from article a { inner join articleAttribute aa on aa.art_id = a.art_id }");
        assertQuery("select art_id from article a { inner join articleAttribute aa { inner join articlePrice ap {} on ap.sku_id = aa.sku_id } on aa.art_id = a.art_id }");
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
        assertExpressionFail(IllegalArgumentException.class, "'rere'.filter(a -> a.sku_id > 0)");
        // Lambda parameter already in use
        assertExpressionFail(IllegalArgumentException.class, "articleAttribute.map(a -> p.price.map(a -> a.price_sales))");
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
        Query q = parser.parseQuery(catalogRegistry, query);
        System.out.println(q);
        System.out.println();
    }
    
    private void assertExpressionFail(Class<? extends Exception> expected, String expression)
    {
        try
        {
            parser.parseExpression(catalogRegistry, expression);
            fail();
        }
        catch (Exception e)
        {
            assertTrue(expected.isAssignableFrom(e.getClass()));
        }
    }
}
