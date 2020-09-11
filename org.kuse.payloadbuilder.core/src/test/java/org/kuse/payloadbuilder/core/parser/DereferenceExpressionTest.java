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
package org.kuse.payloadbuilder.core.parser;

import static java.util.Arrays.asList;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import org.junit.Test;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.TableAlias;

/** Test {@link DereferenceExpression} */
public class DereferenceExpressionTest extends AParserTest
{
    @Test
    public void test_dereference_map()
    {
        ExecutionContext ctx = new ExecutionContext(new QuerySession(new CatalogRegistry()));

        TableAlias t = TableAlias.of(null, QualifiedName.of("table"), "t", new String[] {"a"});
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

        TableAlias t = TableAlias.of(null, QualifiedName.of("table"), "t", new String[] {"a"});
        TableAlias child = TableAlias.of(t, QualifiedName.of("child"), "c", new String[] {"elite"});

        Row row = Row.of(t, 0, new Object[] {10});
        row.getChildRows(0).add(Row.of(child, 0, new Object[] {1337}));

        ctx.setRow(row);

        Expression e = e("c.filter(x -> true)[0].elite");
        assertEquals(1337, e.eval(ctx));
    }

}
