/**
 *
 *  Copyright (c) Marcus Henriksson <kuseman80@gmail.com>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
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
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.TableAlias;
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
        TableAlias a = TableAlias.of(null, QualifiedName.of("article"), "a", new String[] {"article_attribute"});
        Row row = Row.of(a, 0, new Object[] {
                ofEntries(entry("attribute1", ofEntries(
                        entry("buckets", asList(
                                ofEntries(true, entry("key", 10), entry("count", 20)),
                                ofEntries(true, entry("key", 11), entry("count", 15), entry("id", "value")))))))
        });
        TableAlias func = TableAlias.of(a, QualifiedName.of("func"), "f", true);

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
                assertArrayEquals(r.getColumns(), new String[] {"key", "count", "id"});
                assertEquals(11, r.getObject(0));
                assertEquals(15, r.getObject(1));
                assertEquals("value", r.getObject(2));
            }
        }
    }

}
