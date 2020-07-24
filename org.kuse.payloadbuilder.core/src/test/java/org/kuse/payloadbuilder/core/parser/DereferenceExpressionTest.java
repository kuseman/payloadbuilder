package org.kuse.payloadbuilder.core.parser;

import static java.util.Arrays.asList;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import org.junit.Ignore;
import org.junit.Test;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.catalog.TableAlias;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.parser.DereferenceExpression;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

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
