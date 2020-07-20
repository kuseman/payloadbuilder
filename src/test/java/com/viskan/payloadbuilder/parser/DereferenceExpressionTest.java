package com.viskan.payloadbuilder.parser;

import com.viskan.payloadbuilder.QuerySession;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.operator.Row;

import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;
import static java.util.Arrays.asList;

import org.junit.Ignore;
import org.junit.Test;

/** Test {@link DereferenceExpression} */
@Ignore
public class DereferenceExpressionTest extends AParserTest
{
    @Test
    public void test_evaluation()
    {
        Expression e = e("a.filter(b -> b.id > 0).c");
        ExecutionContext ctx = new ExecutionContext(new QuerySession(new CatalogRegistry()));
        
        TableAlias t = TableAlias.of(null, "table", "t");
        t.setColumns(new String[] { "a" });
        Row row = Row.of(t, 0, new Object[] { asList(
                ofEntries(entry("id", -1)),
                ofEntries(entry("id", 0)),
                ofEntries(entry("id", 1)),
                ofEntries(entry("id", 2)))
                });
        ctx.setRow(row);
        
        System.out.println(e.eval(ctx));
    }
}
