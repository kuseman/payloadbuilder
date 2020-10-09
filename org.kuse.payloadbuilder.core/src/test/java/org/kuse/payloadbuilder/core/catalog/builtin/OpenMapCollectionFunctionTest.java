package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Arrays.asList;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import org.junit.Assert;
import org.junit.Test;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;

/** Test {@link OpenMapCollectionFunction} */
public class OpenMapCollectionFunctionTest extends Assert
{
    private final OpenMapCollectionFunction f = new OpenMapCollectionFunction(new Catalog("test")
    {
    });

    @Test
    public void test()
    {
        TableAlias a = TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("article"), "a")
                .columns(new String[] {"article_attribute"})
                .children(asList(
                        TableAliasBuilder.of(TableAlias.Type.FUNCTION, QualifiedName.of("func"), "f").asteriskColumns()))
                .build();
        Row row = Row.of(a, 0, new Object[] {
                ofEntries(entry("attribute1", ofEntries(
                        entry("buckets", asList(
                                ofEntries(true, entry("key", 10), entry("count", 20)),
                                ofEntries(true, entry("key", 11), entry("count", 15), entry("id", "value")))))))
        });

        ExecutionContext context = new ExecutionContext(new QuerySession(new CatalogRegistry()));
        context.setTuple(row);
        Expression arg = new QualifiedReferenceExpression(new QualifiedName(asList("a", "article_attribute", "attribute1", "buckets")), -1);
        RowIterator it = f.open(context, "", a.getChildAliases().get(0), asList(arg));
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            int pos = (int) tuple.getValue(QualifiedName.of("f", "__pos"), 0);

            if (pos == 0)
            {
                assertEquals(10, tuple.getValue(QualifiedName.of("f", "key"), 0));
                assertEquals(20, tuple.getValue(QualifiedName.of("f", "count"), 0));
            }
            else
            {
                assertEquals(11, tuple.getValue(QualifiedName.of("f", "key"), 0));
                assertEquals(15, tuple.getValue(QualifiedName.of("f", "count"), 0));
                assertEquals("value", tuple.getValue(QualifiedName.of("f", "id"), 0));
            }
        }
    }
}
