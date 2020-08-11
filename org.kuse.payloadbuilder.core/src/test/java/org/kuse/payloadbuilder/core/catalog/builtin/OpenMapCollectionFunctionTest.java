package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Arrays.asList;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.catalog.TableAlias;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;

/** Test {@link OpenMapCollectionFunction} */
public class OpenMapCollectionFunctionTest extends Assert
{
    private final OpenMapCollectionFunction f = new OpenMapCollectionFunction(new Catalog("test") {});

    @Test
    public void test()
    {
        TableAlias a = TableAlias.of(null, "article", "a");
        a.setColumns(new String[] {"article_attribute"});
        Row row = Row.of(a, 0, new Object[] {
                ofEntries(entry("attribute1", ofEntries(
                        entry("buckets", asList(
                                ofEntries(true, entry("key", 10), entry("count", 20)),
                                ofEntries(true, entry("key", 11), entry("count", 15)))))))
        });
        TableAlias func = TableAlias.of(a, "a", "a");
        func.setColumns(new String[] { "key", "count" });
        
        ExecutionContext context = new ExecutionContext(new QuerySession(new CatalogRegistry()));
        context.setRow(row);
        Expression arg = new QualifiedReferenceExpression(new QualifiedName(asList("a", "article_attribute", "attribute1", "buckets")), -1);
        Iterator<Row> it = f.open(context, "", func, asList(arg));
        while (it.hasNext())
        {
            Row r = it.next();
            if (r.getPos() == 0)
            {
                assertEquals(10, r.getObject(0));
                assertEquals(20, r.getObject(1));
            }
            else
            {
                assertEquals(11, r.getObject(0));
                assertEquals(15, r.getObject(1));
            }
        }
    }

}
