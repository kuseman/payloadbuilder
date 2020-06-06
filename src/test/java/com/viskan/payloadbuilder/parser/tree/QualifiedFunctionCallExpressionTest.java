package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.parser.QueryParser;

import org.junit.Assert;
import org.junit.Test;

/** Unit test of {@link QualifiedFunctionCallExpression} */
public class QualifiedFunctionCallExpressionTest extends Assert
{
    private final QueryParser p = new QueryParser();
    private final CatalogRegistry r = new CatalogRegistry();
    
    @Test
    public void test_fold()
    {
        Expression e;
        
        e = e("concat(1, 10)");
        assertEquals(e("concat(1,10)"), e);
        
        e = e("concat(1 + 1 + a, 10)");
        assertEquals(e("concat(2 + a, 10)"), e);
        
        e = e("a.filter(x -> 1+1+1+x > 10)");
        assertEquals(e("a.filter(x -> 3+x > 10)"), e);
        
        e = e("a.filter(x -> 1+1+1 > 10)");
        assertEquals(e("a.filter(x -> false)"), e);
    }
    
    private Expression e(String expression)
    {
        return p.parseExpression(r, expression);
    }
}
