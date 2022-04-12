package se.kuseman.payloadbuilder.core.operator;

import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.TableAlias.TableAliasBuilder;
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Operator.TupleIterator;
import se.kuseman.payloadbuilder.api.operator.Row;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.core.parser.LiteralExpression;

/** Unit test of {@link BatchLimitOperator} */
public class BatchLimitOperatorTest extends AOperatorTest
{
    @Test
    public void test()
    {
        TableAlias alias = TableAliasBuilder.of(0, TableAlias.Type.TABLE, QualifiedName.of("a"), "a")
                .build();
        MutableBoolean close = new MutableBoolean();
        Operator op = op(ctx -> IntStream.range(0, 10)
                .mapToObj(i -> (Tuple) Row.of(alias, new String[] { "pos", "Value" }, new Object[] { i, i }))
                .iterator(), () -> close.setTrue());
        Operator limitOp = new BatchLimitOperator(0, op, LiteralExpression.create(5));

        ExecutionContext ctx = new ExecutionContext(session);
        TupleIterator it = limitOp.open(ctx);

        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            int pos = (int) tuple.getValue(0);
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
            int pos = (int) tuple.getValue(0);
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
