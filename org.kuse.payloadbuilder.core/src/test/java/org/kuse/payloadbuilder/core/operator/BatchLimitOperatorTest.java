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

import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.LiteralExpression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Unit test of {@link BatchLimitOperator} */
public class BatchLimitOperatorTest extends AOperatorTest
{
    @Test
    public void test()
    {
        TableAlias alias = TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("a"), "a").build();
        MutableBoolean close = new MutableBoolean();
        Operator op = op(ctx -> IntStream.range(0, 10).mapToObj(i -> (Tuple) Row.of(alias, i, new Object[] {i})).iterator(), () -> close.setTrue());
        Operator limitOp = new BatchLimitOperator(0, op, LiteralExpression.create(5));

        ExecutionContext ctx = new ExecutionContext(session);
        RowIterator it = limitOp.open(ctx);

        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            int pos = (int) tuple.getValue(QualifiedName.of("a", "__pos"), 0);
            assertEquals(count, pos);
            count++;
        }

        assertEquals(5, count);

        // Open operator again
        it = limitOp.open(ctx);
        assertTrue(it.hasNext());

        while (it.hasNext())
        {
            Tuple tuple = it.next();
            int pos = (int) tuple.getValue(QualifiedName.of("a", "__pos"), 0);
            assertEquals(count, pos);
            count++;
        }

        assertEquals(10, count);
        // Open once more
        it = limitOp.open(ctx);
        assertFalse(it.hasNext());
        it.close();
        assertTrue(close.booleanValue());
    }
}
