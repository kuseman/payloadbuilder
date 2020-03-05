package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;
import com.viskan.payloadbuilder.parser.tree.QualifiedReferenceExpression.QualifiedReferenceContainer;

import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;
import static java.util.Arrays.asList;

import java.util.Map;

import org.apache.commons.collections.IteratorUtils;
import org.junit.Assert;
import org.junit.Test;

public class QualifiedReferenceExpressionTest extends Assert
{
    @Test
    public void test_QualifiedReferenceContainer()
    {
        QualifiedReferenceExpression.QualifiedReferenceContainer c = new QualifiedReferenceContainer(new QualifiedName(asList("a", "b", "c")));
        
        assertNull(c.getValue((Row) null));
        assertNull(c.getValue(IteratorUtils.emptyIterator()));
        assertNull(c.getValue((Object) null));
        
        Map<Object, Object> map = ofEntries(entry("a", ofEntries(entry("b", ofEntries(entry("c", asList(1,2,3)))))));
        assertEquals(asList(1,2,3), c.getValue(map));
        assertEquals(asList(1,2,3), (c.getValue(asList(map).iterator())));
        
        TableAlias a = TableAlias.of(null, "art", "a");
        a.setColumns(new String[] { "a" });
        Row row = Row.of(a, 0, new Object[] { map.get("a") });
        assertEquals(asList(1,2,3), c.getValue(row));
        assertEquals(asList(1,2,3), c.getValue((Object) row));
    }
    
    @Test
    public void test_evaluation()
    {
        QualifiedReferenceExpression e = new QualifiedReferenceExpression(new QualifiedName(asList("a", "b", "c")), -1);
        EvaluationContext ctx = new EvaluationContext();
        
        TableAlias a = TableAlias.of(null, "art", "a");
        a.setColumns(new String[] { "a" });
        Map<Object, Object> map = ofEntries(entry("b", ofEntries(entry("c", asList(1,2,3)))));
        Row row = Row.of(a, 0, new Object[] { map });
        
        assertEquals(asList(1,2,3), e.eval(ctx, row));
    }
}
