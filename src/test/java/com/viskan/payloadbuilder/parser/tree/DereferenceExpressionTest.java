package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;
import com.viskan.payloadbuilder.parser.QueryParser;

import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;
import static java.util.Arrays.asList;

import org.junit.Assert;
import org.junit.Test;

/** Test {@link DereferenceExpression} */
public class DereferenceExpressionTest extends Assert
{
    private final QueryParser parser = new QueryParser();
    
    @Test
    public void test_evaluation()
    {
        Expression e = parser.parseExpression(new CatalogRegistry(), "a.filter(b -> b.id > 0).c");
        EvaluationContext ctx = new EvaluationContext();
        
        TableAlias t = TableAlias.of(null, "table", "t");
        t.setColumns(new String[] { "a" });
        Row row = Row.of(t, 0, new Object[] { asList(
                ofEntries(entry("id", -1)),
                ofEntries(entry("id", 0)),
                ofEntries(entry("id", 1)),
                ofEntries(entry("id", 2)))
                });
        
        e.eval(ctx, row);
    }
}
