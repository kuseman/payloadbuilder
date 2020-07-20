package com.viskan.payloadbuilder.parser;

import com.viskan.payloadbuilder.QuerySession;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.operator.Row;

import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;
import static java.util.Arrays.asList;

import java.util.Map;

import org.junit.Test;

public class QualifiedReferenceExpressionTest extends AParserTest
{
//    @Test
//    public void test_QualifiedReferenceContainer()
//    {
//        QualifiedReferenceExpression.QualifiedReferenceContainer c = new QualifiedReferenceContainer(new QualifiedName(asList("a", "b", "c")));
//        
//        assertNull(c.getValue((Row) null));
//        assertNull(c.getValue(IteratorUtils.emptyIterator()));
//        assertNull(c.getValue((Object) null));
//        
//        Map<Object, Object> map = ofEntries(entry("a", ofEntries(entry("b", ofEntries(entry("c", asList(1,2,3)))))));
//        assertEquals(asList(1,2,3), c.getValue(map));
//        assertEquals(asList(1,2,3), (c.getValue(asList(map).iterator())));
//        
//        TableAlias a = TableAlias.of(null, "art", "a");
//        a.setColumns(new String[] { "a" });
//        Row row = Row.of(a, 0, new Object[] { map.get("a") });
//        assertEquals(asList(1,2,3), c.getValue(row));
//        assertEquals(asList(1,2,3), c.getValue((Object) row));
//    }
    
    @Test
    public void test_evaluation()
    {
        QualifiedReferenceExpression e = new QualifiedReferenceExpression(new QualifiedName(asList("a", "b", "c")), -1);
        ExecutionContext ctx = new ExecutionContext(new QuerySession(new CatalogRegistry()));
        
        TableAlias a = TableAlias.of(null, "art", "_a");
        a.setColumns(new String[] { "a" });
        Map<Object, Object> map = ofEntries(entry("b", ofEntries(entry("c", asList(1,2,3)))));
        Row row = Row.of(a, 0, new Object[] { map });
        ctx.setRow(row);
        
        assertEquals(asList(1,2,3), e.eval(ctx));
    }
}
