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

import java.util.stream.IntStream;

import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.utils.MapUtils;

/** Test {@link SubscriptExpression} */
public class SubscriptExpressionTest extends AParserTest
{
    TableAlias t = TableAlias.of(null, QualifiedName.of("table"), "t", new String[] {"a", "b", "c", "d"});
    @Test
    public void test()
    {
        Row row = Row.of(t, 0, new Object[] {asList(1, 2, 3), IntStream.range(1, 4).iterator(), new int[] {1, 2, 3}, MapUtils.ofEntries(MapUtils.entry("key", "value"))});
        context.setRow(row);

        assertNull(e("null[10]").eval(context));
        assertNull(e("a[null]").eval(context));

        assertEquals(2, e("a[1]").eval(context));
        assertNull(e("a[-1]").eval(context));
        assertNull(e("a[10]").eval(context));

        assertEquals(2, e("b[1]").eval(context));
        assertNull(e("b[-1]").eval(context));
        assertNull(e("b[10]").eval(context));

        assertEquals(2, e("c[1]").eval(context));
        assertNull(e("c[-1]").eval(context));
        assertNull(e("c[10]").eval(context));

        assertEquals("value", e("d['key']").eval(context));
        assertNull(e("d['key2']").eval(context));
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_invalid_value()
    {
        e("'test'[10]").eval(context);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_invalid_subscript_int()
    {
        Row row = Row.of(t, 0, new Object[] {asList(1, 2, 3), IntStream.range(1, 4).iterator(), new int[] {1, 2, 3}, MapUtils.ofEntries(MapUtils.entry("key", "value"))});
        context.setRow(row);
        e("a['string']").eval(context);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_invalid_subscript_string()
    {
        Row row = Row.of(t, 0, new Object[] {asList(1, 2, 3), IntStream.range(1, 4).iterator(), new int[] {1, 2, 3}, MapUtils.ofEntries(MapUtils.entry("key", "value"))});
        context.setRow(row);
        e("d[123]").eval(context);
    }
}
