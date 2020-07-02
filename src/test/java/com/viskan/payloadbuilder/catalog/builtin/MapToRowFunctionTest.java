package com.viskan.payloadbuilder.catalog.builtin;

import com.viskan.payloadbuilder.QuerySession;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.operator.Row;
import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.QualifiedName;
import com.viskan.payloadbuilder.parser.QualifiedReferenceExpression;

import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;
import static java.util.Arrays.asList;

import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

/** Test {@link MapToRowFunction} */
public class MapToRowFunctionTest extends Assert
{
    private final MapToRowFunction f = new MapToRowFunction(new Catalog("test") {});

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
        Object value = new QualifiedReferenceExpression(new QualifiedName(null, asList("a", "article_attribute", "attribute1", "buckets")), -1).eval(context);
        Iterator<Row> it = f.open(context, func, asList(value));
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
