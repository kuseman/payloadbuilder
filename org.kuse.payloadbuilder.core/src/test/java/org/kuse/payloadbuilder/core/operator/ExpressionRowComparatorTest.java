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
package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.SortItem;
import org.kuse.payloadbuilder.core.parser.SortItem.NullOrder;
import org.kuse.payloadbuilder.core.parser.SortItem.Order;

/** Test of {@link ExpressionRowComparator} */
public class ExpressionRowComparatorTest extends AOperatorTest
{
    @Test
    public void test()
    {
        TableAlias alias = TableAlias.of(null, QualifiedName.of("table"), "a", new String[] {"col1"});
        Row a, b;

        a = Row.of(alias, 0, new Object[] {1});
        b = Row.of(alias, 0, new Object[] {2});

        List<SortItem> items = new ArrayList<>(asList(
                new SortItem(e("col1"), Order.ASC, NullOrder.UNDEFINED)));

        ExecutionContext context = new ExecutionContext(session);
        ExpressionRowComparator comparator = new ExpressionRowComparator(items);
        assertEquals(-1, comparator.compare(context, a, b));

        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.UNDEFINED));
        assertEquals(1, comparator.compare(context, a, b));

        a = Row.of(alias, 0, new Object[] {2});
        assertEquals(0, comparator.compare(context, a, b));

        a = Row.of(alias, 0, new Object[] {null});
        b = Row.of(alias, 0, new Object[] {null});

        assertEquals(0, comparator.compare(context, a, b));

        // Nulls first
        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.FIRST));
        a = Row.of(alias, 0, new Object[] {1});
        b = Row.of(alias, 0, new Object[] {null});
        assertEquals(1, comparator.compare(context, a, b));

        // Nulls last
        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.LAST));
        a = Row.of(alias, 0, new Object[] {1});
        b = Row.of(alias, 0, new Object[] {null});
        assertEquals(-1, comparator.compare(context, a, b));

        // Nulls first
        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.FIRST));
        a = Row.of(alias, 0, new Object[] {null});
        b = Row.of(alias, 0, new Object[] {1});
        assertEquals(-1, comparator.compare(context, a, b));

        // Nulls last
        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.LAST));
        a = Row.of(alias, 0, new Object[] {null});
        b = Row.of(alias, 0, new Object[] {1});
        assertEquals(1, comparator.compare(context, a, b));

        // Nulls first if undefined
        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.UNDEFINED));
        a = Row.of(alias, 0, new Object[] {null});
        b = Row.of(alias, 0, new Object[] {1});
        assertEquals(-1, comparator.compare(context, a, b));
        a = Row.of(alias, 0, new Object[] {1});
        b = Row.of(alias, 0, new Object[] {null});
        assertEquals(1, comparator.compare(context, a, b));

    }
}
