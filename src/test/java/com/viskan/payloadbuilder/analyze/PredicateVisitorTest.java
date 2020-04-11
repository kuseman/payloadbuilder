package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.parser.QueryParser;
import com.viskan.payloadbuilder.parser.tree.Expression;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/** Test of {@link PushDownPredicateVisitor} */
public class PredicateVisitorTest extends Assert
{
    private final QueryParser parser = new QueryParser();
    private final CatalogRegistry catalogRegistry = new CatalogRegistry();

    @Test
    public void test()
    {
        Expression e;
        Pair<Expression, Expression> actual;
        
        e = parser.parseExpression(catalogRegistry, "aa.active_flg AND (aa.sku_id = s.sku_id OR aa.internet_flg)");
        actual = PushDownPredicateVisitor.analyze(e, "aa");
        assertEquals(parser.parseExpression(catalogRegistry, "aa.active_flg"), actual.getKey());
        assertEquals(parser.parseExpression(catalogRegistry, "aa.sku_id = s.sku_id OR aa.internet_flg"), actual.getValue());
        
        e = parser.parseExpression(catalogRegistry, "note_id > 0");
        actual = PushDownPredicateVisitor.analyze(e, "ap");
        assertEquals(e, actual.getKey());
        assertNull(actual.getValue());
        
        e = parser.parseExpression(catalogRegistry, "aa.art_id = s.art_id AND aa.active_flg AND internet_flg");
        actual = PushDownPredicateVisitor.analyze(e, "aa");
        assertEquals(parser.parseExpression(catalogRegistry, "internet_flg and aa.active_flg"), actual.getKey());
        assertEquals(parser.parseExpression(catalogRegistry, "aa.art_id = s.art_id"), actual.getValue());
        
        e = parser.parseExpression(catalogRegistry, "aa.art_id = s.art_id");
        actual = PushDownPredicateVisitor.analyze(e, "aa");
        assertNull(actual.getKey());
        assertEquals(parser.parseExpression(catalogRegistry, "aa.art_id = s.art_id"), actual.getValue());
        
        e = parser.parseExpression(catalogRegistry, "aa.active_flg AND internet_flg");
        actual = PushDownPredicateVisitor.analyze(e, "aa");
        assertEquals(parser.parseExpression(catalogRegistry, "aa.active_flg AND internet_flg"), actual.getKey());
        assertNull(actual.getValue());
        
        e = parser.parseExpression(catalogRegistry, "aa.active_flg or internet_flg");
        actual = PushDownPredicateVisitor.analyze(e, "aa");
        assertNull(actual.getKey());
        assertEquals(parser.parseExpression(catalogRegistry, "aa.active_flg or internet_flg"), actual.getValue());
        
        e = parser.parseExpression(catalogRegistry, "aa.active_flg AND aa.art_id = s.art_id AND internet_flg");
        actual = PushDownPredicateVisitor.analyze(e, "aa");
        assertEquals(parser.parseExpression(catalogRegistry, "internet_flg and aa.active_flg"), actual.getKey());
        assertEquals(parser.parseExpression(catalogRegistry, "aa.art_id = s.art_id"), actual.getValue());
        
        e = parser.parseExpression(catalogRegistry, "aa.active_flg AND aa.art_id = s.art_id");
        actual = PushDownPredicateVisitor.analyze(e, "aa");
        assertEquals(parser.parseExpression(catalogRegistry, "aa.active_flg"), actual.getKey());
        assertEquals(parser.parseExpression(catalogRegistry, "aa.art_id = s.art_id"), actual.getValue());
        
        e = parser.parseExpression(catalogRegistry, "aa.sku_id = s.sku_id and aa.active_flg AND aa.art_id = s.art_id");
        actual = PushDownPredicateVisitor.analyze(e, "aa");
        assertEquals(parser.parseExpression(catalogRegistry, "aa.active_flg"), actual.getKey());
        assertEquals(parser.parseExpression(catalogRegistry, "aa.art_id = s.art_id and aa.sku_id = s.sku_id"), actual.getValue());
        
        e = parser.parseExpression(catalogRegistry, "aa.sku_id = s.sku_id and aa.active_flg AND aa.art_id = s.art_id");
        actual = PushDownPredicateVisitor.analyze(e, "ap");
        assertNull(actual.getKey());
        assertEquals(e, actual.getValue());
    }
}
