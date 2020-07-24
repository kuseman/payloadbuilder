package org.kuse.payloadbuilder.core.parser;

import static java.util.Arrays.asList;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.Map;

import org.junit.Test;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.catalog.TableAlias;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;

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
