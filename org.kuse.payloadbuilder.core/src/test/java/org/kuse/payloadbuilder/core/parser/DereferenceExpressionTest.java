package org.kuse.payloadbuilder.core.parser;

import static java.util.Arrays.asList;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import org.junit.Test;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.catalog.TableAlias;
import org.kuse.payloadbuilder.core.operator.Row;

/** Test {@link DereferenceExpression} */
public class DereferenceExpressionTest extends AParserTest
{
    @Test
    public void test_dereference_map()
    {
        ExecutionContext ctx = new ExecutionContext(new QuerySession(new CatalogRegistry()));

        TableAlias t = TableAlias.of(null, "table", "t");
        t.setColumns(new String[] {"a"});
        Row row = Row.of(t, 0, new Object[] {asList(
                ofEntries(entry("id", -1), entry("c", -10)),
                ofEntries(entry("id", 0), entry("c", 0)),
                ofEntries(entry("id", 1), entry("c", 10), entry("d", ofEntries(entry("key", "value")))),
                ofEntries(entry("id", 2), entry("c", 20)))
        });
        ctx.setRow(row);

        Expression e;

        e = e("a.filter(b -> b.id > 0)[10].d");
        assertNull(e.eval(ctx));

        e = e("a.filter(b -> b.id > 0)[0].c");
        assertEquals(10, e.eval(ctx));

        e = e("a.filter(b -> b.id > 0)[0].d.key");
        assertEquals("value", e.eval(ctx));

        try
        {
            e = e("a.filter(b -> b.id > 0)[0].d.key.missing");
            e.eval(ctx);
            fail("Cannot dereference a string");
        }
        catch (IllegalArgumentException ee)
        {
        }
    }

    @Test
    public void test_dereference_row()
    {
        ExecutionContext ctx = new ExecutionContext(new QuerySession(new CatalogRegistry()));

        TableAlias t = TableAlias.of(null, "table", "t");
        t.setColumns(new String[] {"a"});
        TableAlias child = TableAlias.of(t, "child", "c");
        child.setColumns(new String[] {"elite"});

        Row row = Row.of(t, 0, new Object[] {10});
        row.getChildRows(0).add(Row.of(child, 0, new Object[] {1337}));

        ctx.setRow(row);

        Expression e = e("c.filter(x -> true)[0].elite");
        assertEquals(1337, e.eval(ctx));
    }

}
