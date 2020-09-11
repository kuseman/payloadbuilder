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
import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Unit test of {@link GroupByOperator} */
public class GroupByOperatorTest extends AOperatorTest
{
    @Test
    public void test()
    {
        TableAlias alias = TableAlias.of(null, QualifiedName.of("a"), "a");
        MutableBoolean close = new MutableBoolean();
        Operator op = op(ctx -> IntStream.range(0, 10).mapToObj(i -> Row.of(alias, i, new Object[] {i, i % 2})).iterator(), () -> close.setTrue());

        GroupByOperator gop = new GroupByOperator(
                0,
                op,
                emptyList(),
                (ctx, row, values) -> values[0] = row.getObject(1),
                1);

        RowIterator it = gop.open(new ExecutionContext(session));

        List<Object> expected = asList(
                asList(0, 2, 4, 6, 8),
                asList(0, 0, 0, 0, 0),
                asList(1, 3, 5, 7, 9),
                asList(1, 1, 1, 1, 1));

        List<Object> actual = new ArrayList<>();

        while (it.hasNext())
        {
            Row row = it.next();

            actual.add(IteratorUtils.toList(IteratorUtils.getIterator(row.getObject(0))));
            actual.add(IteratorUtils.toList(IteratorUtils.getIterator(row.getObject(1))));
        }
        it.close();

        assertEquals(expected, actual);
        assertTrue(close.booleanValue());
    }
}
