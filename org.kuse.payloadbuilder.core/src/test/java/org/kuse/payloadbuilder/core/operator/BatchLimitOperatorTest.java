package org.kuse.payloadbuilder.core.operator;

import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.parser.LiteralExpression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Unit test of {@link BatchLimitOperator} */
public class BatchLimitOperatorTest extends AOperatorTest
{
    @Test
    public void test()
    {
        TableAlias alias = TableAliasBuilder.of(0, TableAlias.Type.TABLE, QualifiedName.of("a"), "a").build();
        MutableBoolean close = new MutableBoolean();
        Operator op = op(ctx -> IntStream.range(0, 10).mapToObj(i -> (Tuple) Row.of(alias, i, new Object[] {i})).iterator(), () -> close.setTrue());
        Operator limitOp = new BatchLimitOperator(0, op, LiteralExpression.create(5));

        ExecutionContext ctx = new ExecutionContext(session);
        RowIterator it = limitOp.open(ctx);

        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            int pos = (int) tuple.getTuple(0).getValue("__pos");
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
            int pos = (int) tuple.getTuple(0).getValue("__pos");
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
