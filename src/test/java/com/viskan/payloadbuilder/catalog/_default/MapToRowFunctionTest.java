package com.viskan.payloadbuilder.catalog._default;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.operator.OperatorContext;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;
import com.viskan.payloadbuilder.parser.tree.QualifiedReferenceExpression;

import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;
import static java.util.Arrays.asList;

import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

/** Test {@link MapToRowFunction} */
public class MapToRowFunctionTest extends Assert
{
    private final MapToRowFunction f = new MapToRowFunction(new Catalog("test"));

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
        Object value = new QualifiedReferenceExpression(new QualifiedName(asList("a", "article_attribute", "attribute1", "buckets")), -1).eval(null, row);
        
        Iterator<Row> it = f.open(new OperatorContext(), func, asList(value));
        while (it.hasNext())
        {
            Row r = it.next();
            assertArrayEquals(new String[] { "key", "count" }, r.getTableAlias().getColumns());
            System.out.println(r);
        }
    }

}
